import glob
import json
import logging
import os
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import sys
import threading
import time
import traceback

from datetime import datetime, timedelta, timezone
from loguru import logger
from queue import Queue

from common import send_message, generate_random_string
from config import settings
from data import FileDescriptor, ReplicationMessage, TransactionEvent
from decoder import Decoder, json_serializer

# Configure logging
if settings.DEBUG == 1:
    logging.basicConfig(level=logging.DEBUG)
logger.add(os.path.join(settings.LOG_DIR, f'{os.path.basename(__file__)}.log'), rotation='00:00', retention='7 days', level='INFO')


class LogicalReplicationStreamer:
    """
    This process will spawn 3 additional threads:
    - Streamer: constantly read the database message, parse it into structured object and put it into queue.
    - Consumer: pull the message from the queue, decode and write the content to files. This process took time for each message processed, so a large amount of database transaction will introduce delays.
    - Monitor: constantly logs the time difference between the current processed message timestamp and current timestamp.

    Note that we are referring "database message" as the actual transaction happened inside the database.
    """

    def __init__(self, host: str, port: str, user: str, password: str, database: str, application_name: str = f'cdc-streamer-{settings.STREAMER_DB_NAME}-{generate_random_string(alphanum=True)}', **kwargs) -> None:
        self.dsn = psycopg2.extensions.make_dsn(host=host, port=port, user=user, password=password, database=database, application_name=application_name, **kwargs)

        self.decoder = Decoder(self.dsn)

        self.q = Queue(maxsize=settings.STREAM_CONSUMER_QUEUE_MAX_SIZE)

        self.transaction = None
        self.send_feedback = False
        self.latest_lsn = None
        self.latest_commit_ts = None
        self.now = datetime.now(timezone.utc)
        self.msg_count = 0
        self.latest_msg_ts = datetime.now(timezone.utc)
        self.latest_all_file_closed_ts = datetime.now(timezone.utc)
        self.opened_files: dict[str, FileDescriptor] = {}  # { table_name: FileDescriptor }

        self.exception_event = threading.Event()
        self.is_exception = False

        self.conn = psycopg2.connect(self.dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection)
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.ReplicationCursor)
        logger.debug(f'Connected to db: {settings.STREAMER_DB_NAME} (Replication)')

        # # Send starting message
        # send_message(f'_CDC Streamer [{settings.STREAM_DB_NAME}]_ started')

    def run(self) -> None:
        thread_streamer = threading.Thread(target=self.streamer, daemon=True)  # Spawn thread to start streaming from replication slot
        thread_consumer = threading.Thread(target=self.consumer, daemon=True)  # Spawn thread to consume the streamed logical replication message
        thread_monitor = threading.Thread(target=self.monitor, daemon=True)  # Spawn thread to monitor the streamer

        thread_streamer.start()
        thread_consumer.start()
        thread_monitor.start()

        thread_consumer.join()
        if thread_consumer.is_alive():
            logger.warning('Consumer thread is still alive, stopping the streamer...')
            self.exception_event.set()
            thread_consumer.join()

        thread_monitor.join()
        if thread_monitor.is_alive():
            logger.warning('Monitor thread is still alive, stopping the streamer...')
            self.exception_event.set()
            thread_monitor.join()

        self.stop()
        logger.info('Exiting')

    def streamer(self) -> None:
        logger.debug('Streamer thread started...')
        try:
            # Make sure replication slot exists
            self.cursor.execute(f'SELECT COUNT(1) FROM pg_replication_slots WHERE slot_name = \'{settings.STREAMER_REPLICATION_SLOT_NAME}\'')
            if self.cursor.fetchone()[0] == 0:
                self.cursor.create_replication_slot(settings.STREAMER_REPLICATION_SLOT_NAME, output_plugin='pgoutput')
                logger.info(f'Replication slot created: {settings.STREAMER_REPLICATION_SLOT_NAME}')
            self.cursor.start_replication(slot_name=settings.STREAMER_REPLICATION_SLOT_NAME, options={
                'proto_version': '1',
                'publication_names': settings.STREAMER_PUBLICATION_NAME,
            })
            logger.debug(f'Replication started, publication name: \'{settings.STREAMER_PUBLICATION_NAME}\', replication slot name: \'{settings.STREAMER_REPLICATION_SLOT_NAME}\'')
            self.cursor.consume_stream(self.put_message_to_queue)
        except Exception as e:
            t = traceback.format_exc()
            send_message(f'_CDC Streamer [{settings.STREAMER_DB_NAME}]_ streamer error: **{type(e)}: {e}**\n```{t}```')
            logger.error(f'Error in streamer thread: {e}\n{t}')
            self.exception_event.set()
            raise e
        finally:
            logger.warning('Gracefully stopped streamer thread')

    def put_message_to_queue(self, msg: psycopg2.extras.ReplicationMessage) -> None:
        self.q.put(ReplicationMessage(
            data_start=msg.data_start,
            payload=msg.payload,
            send_time=msg.send_time,
            data_size=msg.data_size,
            wal_end=msg.wal_end,
        ))

    def consumer(self) -> None:
        logger.debug('Consumer thread started...')
        try:
            while not self.exception_event.is_set():
                if not self.q.empty():
                    msg: ReplicationMessage = self.q.get()
                    self.msg_count += 1
                    self.latest_lsn = msg.data_start
                    decoded_msgs = self.decoder.decode(msg)

                    if decoded_msgs is None:  # Begin, commit
                        continue

                    for decoded_msg in decoded_msgs:
                        self.write_to_file(decoded_msg)
                        self.latest_commit_ts = decoded_msg.transaction.commit_ts

                    # Close all files if it's opened for too long
                    if (self.now - self.latest_all_file_closed_ts).total_seconds() > settings.STREAM_FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S:
                        self.close_all_files(f'All files opened for {settings.STREAM_FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S} seconds')

                    self.latest_msg_ts = self.now

                else:  # No message
                    # Close all files if no message received for too long
                    if (self.now - self.latest_all_file_closed_ts).total_seconds() > settings.STREAM_FILEWRITER_NO_MESSAGE_WAIT_TIME_S:
                        self.close_all_files(f'No message for {settings.STREAM_FILEWRITER_NO_MESSAGE_WAIT_TIME_S} seconds')

                    time.sleep(settings.STREAM_CONSUMER_POLL_INTERVAL_S)

                if self.send_feedback:
                    if not self.latest_lsn:
                        raise ValueError('No LSN to send feedback')

                    self.cursor.send_feedback(flush_lsn=self.latest_lsn)
                    logger.info(f'Flush LSN: {self.latest_lsn} ')
                    self.latest_lsn = None
                    self.send_feedback = False

            # If the code reaches here, it means there is exception, make this thread stop gracefully
            return

        except Exception as e:
            t = traceback.format_exc()
            send_message(f'_CDC Streamer [{settings.STREAMER_DB_NAME}]_ consumer error: **{type(e)}: {e}**\n```{t}```')
            logger.error(f'Error in consumer thread: {e}\n{t}')
            self.exception_event.set()
        finally:
            logger.warning('Gracefully stopped consumer thread')

    def write_to_file(self, decoded_msg: TransactionEvent) -> None:
        filename = os.path.join(settings.STREAM_OUTPUT_DIR, f'{decoded_msg.transaction.commit_ts.strftime("%Y%m%d%H%M%S%f")}-{datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")}-{decoded_msg.table.fqn}.json')
        dirname = os.path.dirname(filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        # Construct data
        data = {
            # Metadata: message
            '__m_op': decoded_msg.op,
            '__m_ord': decoded_msg.ord,
            '__m_lsn': decoded_msg.replication_msg.data_start,
            '__m_send_ts': decoded_msg.replication_msg.send_time,
            '__m_size': decoded_msg.replication_msg.data_size,
            '__m_wal_end': decoded_msg.replication_msg.wal_end,
            # Metadata: transaction
            '__tx_lsn': decoded_msg.transaction.lsn,
            '__tx_commit_ts': decoded_msg.transaction.commit_ts,
            '__tx_id': decoded_msg.transaction.xid,
            # Metadata: table
            '__tb': {
                'fqn': decoded_msg.table.fqn,
                'proto_classname': decoded_msg.table.proto_classname,
                'proto_filename': decoded_msg.table.proto_filename,
                'columns': [
                    {
                        'pk': column.pk,
                        'name': column.name,
                        'dtype': column.dtype,
                        'bq_dtype': column.bq_dtype,
                        'proto_dtype': column.proto_dtype,
                    } for column in decoded_msg.table.columns
                ],
            },
        }
        data.update(decoded_msg.data) if decoded_msg.data else None  # None is for delete, truncate event
        data = json.dumps(data, default=json_serializer) + '\n'
        data_size = sys.getsizeof(data)

        # Register file if not exists
        if decoded_msg.table.fqn not in self.opened_files:
            self.opened_files[decoded_msg.table.fqn] = FileDescriptor(
                filename=filename,
                file=open(filename, 'w'),
                size=data_size,
            )
        self.opened_files[decoded_msg.table.fqn].file.write(data)
        self.opened_files[decoded_msg.table.fqn].size += data_size

        # Close all files if this file is too big
        if self.opened_files[decoded_msg.table.fqn].size > settings.STREAM_FILEWRITER_MAX_FILE_SIZE_B:
            self.close_all_files(f'table \'{decoded_msg.table.fqn}\' size exceeds {settings.STREAM_FILEWRITER_MAX_FILE_SIZE_B} bytes')

    def close_all_files(self, reason: str) -> None:
        is_any_closed = False
        for table_name in list(self.opened_files.keys()):
            if table_name in self.opened_files:
                is_any_closed = True
                self.opened_files[table_name].file.close()
                logger.info(f'Closed file \'{self.opened_files[table_name].filename}\' due to: {reason}')

                # Move streamed file to upload dir
                os.rename(self.opened_files[table_name].filename, os.path.join(settings.UPLOADER_OUTPUT_DIR, os.path.basename(self.opened_files[table_name].filename)))

            del self.opened_files[table_name]

        if is_any_closed:
            self.send_feedback = True

        self.latest_all_file_closed_ts = datetime.now(timezone.utc)

    def monitor(self) -> None:
        logger.debug('Monitor thread started...')
        try:
            empty_msg_count = 0
            latest_no_msg_print_ts = datetime.now(timezone.utc)
            while not self.exception_event.is_set():
                self.now = datetime.now(timezone.utc)
                if self.msg_count or empty_msg_count < 3:
                    if self.msg_count == 0:
                        empty_msg_count += 1
                    else:
                        empty_msg_count = 0

                    commit_ts = (self.latest_commit_ts or datetime.fromtimestamp(0)).astimezone(timezone(timedelta(hours=7))).strftime('%Y-%m-%d %H:%M:%S.%f%z')
                    delay = (self.now - self.latest_commit_ts).total_seconds() if self.latest_commit_ts and self.msg_count > 0 else 0
                    latest_lsn = self.latest_lsn or 0
                    logger.info(f'MSGs: {self.msg_count:6d} | COMMIT_TS: {commit_ts} | DELAY: {delay:10.2f}s | LSN: {latest_lsn:10d} | Files: {len(self.opened_files):4d}')
                    self.msg_count = 0
                else:
                    empty_msg_count += 1

                if (self.now - self.latest_msg_ts).total_seconds() > settings.STREAM_NO_MESSAGE_REPORT_INTERVAL_S and (self.now - latest_no_msg_print_ts).total_seconds() > settings.STREAM_NO_MESSAGE_REPORT_INTERVAL_S:
                    logger.warning(f'No message for {(self.now - self.latest_msg_ts).total_seconds()} seconds')
                    latest_no_msg_print_ts = self.now
                time.sleep(1)
        except Exception as e:
            t = traceback.format_exc()
            send_message(f'_CDC Streamer [{settings.STREAMER_DB_NAME}]_ monitor error: **{type(e)}: {e}**\n```{t}```')
            logger.error(f'Error in monitor thread: {e}\n{t}')
            self.exception_event.set()
            raise e
        finally:
            logger.warning('Gracefully stopped monitor thread')

    def stop(self) -> None:
        self.cursor.close()
        self.conn.close()
        logger.debug(f'Disconnected from db: {settings.STREAMER_DB_NAME} (Replication)')


if __name__ == '__main__':
    # Create output directory if not exists
    if not os.path.exists(settings.STREAM_OUTPUT_DIR):
        os.makedirs(settings.STREAM_OUTPUT_DIR)
        logger.info(f'Create stream output dir: {settings.STREAM_OUTPUT_DIR}')
    if not os.path.exists(settings.UPLOADER_OUTPUT_DIR):
        os.makedirs(settings.UPLOADER_OUTPUT_DIR)
        logger.info(f'Create upload output dir: {settings.UPLOADER_OUTPUT_DIR}')

    # Clear stream output directory
    [os.remove(file) for file in glob.glob(f'{settings.STREAM_OUTPUT_DIR}/*.json')]

    # Run the streamer
    logger.info('Starting cdc streamer...')
    streamer = LogicalReplicationStreamer(host=settings.STREAMER_DB_HOST, port=settings.STREAMER_DB_PORT, user=settings.STREAMER_DB_USER, password=settings.STREAMER_DB_PASS, database=settings.STREAMER_DB_NAME)
    streamer.run()
