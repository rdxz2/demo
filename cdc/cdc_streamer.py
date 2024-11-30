import dotenv
import glob
import json
import os
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import sys
import threading
# import traceback
import time
import uuid

from datetime import datetime, timezone
from loguru import logger
from queue import Queue
from threading import Thread

from data import (
    FileDescriptor,
    ReplicationMessage,
    TransactionEvent,
)
from decoder import (
    Decoder,
    json_serializer,
)

dotenv.load_dotenv()

REPL_DB_HOST = os.environ['REPL_DB_HOST']
REPL_DB_PORT = int(os.environ['REPL_DB_PORT'])
REPL_DB_USER = os.environ['REPL_DB_USER']
REPL_DB_PASSWORD = os.environ['REPL_DB_PASSWORD']
REPL_DB_NAME = os.environ['REPL_DB_NAME']
REPL_PUBL_NAME = os.environ['REPL_PUBL_NAME']
REPL_SLOT_NAME = os.environ['REPL_SLOT_NAME']

STREAM_NO_MESSAGE_REPORT_INTERVAL_S = int(os.environ['STREAM_NO_MESSAGE_REPORT_INTERVAL_S'])  # 1 minute, if no message received for this time, report it
STREAM_DELAY_PRINT_INTERVAL_S = int(os.environ['STREAM_DELAY_PRINT_INTERVAL_S'])

FILEWRITER_OUTPUT_DIR = os.path.join('output', REPL_DB_NAME, 'stream')
FILEWRITER_MAX_FILE_SIZE_B = int(os.environ['FILEWRITER_MAX_FILE_SIZE_B'])  # 1 GB, if a single file exceeds this size, close all files
FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S = int(os.environ['FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S'])  # 1 minute, if a file is opened for this time, close all files
FILEWRITER_NO_MESSAGE_WAIT_TIME_S = int(os.environ['FILEWRITER_NO_MESSAGE_WAIT_TIME_S'])  # 1 minute, if no message received for this time, close all files

CONSUMER_QUEUE_MAX_SIZE = int(os.environ['CONSUMER_QUEUE_MAX_SIZE'])
CONSUMER_POLL_INTERVAL_S = int(os.environ['CONSUMER_POLL_INTERVAL_S'])

UPLOAD_OUTPUT_DIR = os.path.join('output', REPL_DB_NAME, 'upload')


class LogicalReplicationStreamer:
    """
    Main process, responsible for consuming replication message from the pipe and converting it into JSON format

    This process as another subprocess `file_writer()` to consume the decoded messages and write it into files.
    """

    def __init__(self, host: str, port: str, user: str, password: str, database: str, application_name: str = f'streamer-{REPL_DB_NAME}-{uuid.uuid4()}', **kwargs) -> None:
        self.dsn = psycopg2.extensions.make_dsn(host=host, port=port, user=user, password=password, database=database, application_name=application_name, **kwargs)

        self.decoder = Decoder(self.dsn)

        self.q = Queue(maxsize=CONSUMER_QUEUE_MAX_SIZE)

        self.transaction = None
        self.send_feedback = False
        self.latest_lsn = None
        self.latest_delay_print_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
        self.latest_msg_ts = datetime.now(tz=timezone.utc)
        self.latest_all_file_closed_ts = datetime.now(tz=timezone.utc)
        self.opened_files: dict[str, FileDescriptor] = {}  # { table_name: FileDescriptor }

        self.exception_event = threading.Event()
        self.is_exception = False

        self.conn = psycopg2.connect(self.dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection)
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.ReplicationCursor)
        logger.debug(f'Connected to db: {REPL_DB_NAME} (Replication)')

    def run(self) -> None:
        thread_stream = Thread(target=self.streamer, daemon=True)  # Spawn thread to start streaming from replication slot
        thread_consumer = Thread(target=self.consumer, daemon=True)  # Spawn thread to consume the streamed logical replication message

        thread_stream.start()
        thread_consumer.start()

        thread_consumer.join()
        if thread_consumer.is_alive():
            logger.warning('Consumer thread is still alive, stopping the streamer...')
            self.exception_event.set()
            thread_consumer.join()

        self.stop()
        logger.info('Exiting')

    def streamer(self) -> None:
        logger.debug('Streamer thread started...')
        try:
            # Make sure replication slot exists
            self.cursor.execute(f'SELECT COUNT(1) FROM pg_replication_slots WHERE slot_name = \'{REPL_SLOT_NAME}\'')
            if self.cursor.fetchone()[0] == 0:
                self.cursor.create_replication_slot(REPL_SLOT_NAME, output_plugin='pgoutput')
                logger.info(f'Replication slot created: {REPL_SLOT_NAME}')
            self.cursor.start_replication(slot_name=REPL_SLOT_NAME, options={
                'proto_version': '1',
                'publication_names': REPL_PUBL_NAME,
            })
            logger.debug(f'Replication started, publication name: \'{REPL_PUBL_NAME}\', replication slot name: \'{REPL_SLOT_NAME}\'')
            self.cursor.consume_stream(self.put_message_to_queue)
        except Exception as e:
            logger.error(f'Error in streamer thread: {e}')
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
        latest_no_msg_print_ts = datetime.now(tz=timezone.utc)
        try:
            while not self.exception_event.is_set():
                now = datetime.now(tz=timezone.utc)
                if not self.q.empty():
                    msg: ReplicationMessage = self.q.get()
                    self.latest_lsn = msg.data_start
                    decoded_msgs = self.decoder.decode(msg)

                    if decoded_msgs is None:  # Begin, commit
                        continue

                    for decoded_msg in decoded_msgs:
                        self.write_to_file(decoded_msg)
                        # Print the delay
                        if (now - self.latest_delay_print_ts).total_seconds() > STREAM_DELAY_PRINT_INTERVAL_S:
                            logger.info(f'TXID: {decoded_msg.transaction.xid}, LSN: {decoded_msg.transaction.lsn}, DELAY: {(now - decoded_msg.transaction.commit_ts).total_seconds()} s')
                            self.latest_delay_print_ts = now

                    # Close all files if it's opened for too long
                    if (now - self.latest_all_file_closed_ts).total_seconds() > FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S:
                        self.close_all_files(f'all files opened for {FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S} seconds')

                else:  # No message
                    if (now - self.latest_msg_ts).total_seconds() > STREAM_NO_MESSAGE_REPORT_INTERVAL_S and (now - latest_no_msg_print_ts).total_seconds() > STREAM_NO_MESSAGE_REPORT_INTERVAL_S:
                        logger.warning(f'No message for {(now - self.latest_msg_ts).total_seconds()} seconds')
                        latest_no_msg_print_ts = now

                    # Close all files if no message received for too long
                    if (now - self.latest_all_file_closed_ts).total_seconds() > FILEWRITER_NO_MESSAGE_WAIT_TIME_S:
                        self.close_all_files(f'no message for {FILEWRITER_NO_MESSAGE_WAIT_TIME_S} seconds')

                    time.sleep(CONSUMER_POLL_INTERVAL_S)

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
            self.exception_event.set()
            logger.error(f'Error in consumer thread: {e}')
        finally:
            logger.warning('Gracefully stopped consumer thread')

    def write_to_file(self, decoded_msg: TransactionEvent) -> None:
        table_name = f'{decoded_msg.table.tschema}.{decoded_msg.table.name}'
        filename = os.path.join(FILEWRITER_OUTPUT_DIR, f'{decoded_msg.transaction.commit_ts.strftime("%Y%m%d%H%M%S")}-{table_name}.json')
        dirname = os.path.dirname(filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        # Construct data
        data = {
            '__op': decoded_msg.op,
            '__lsn': decoded_msg.replication_msg.data_start,
            '__send_ts': decoded_msg.replication_msg.send_time,
            '__size': decoded_msg.replication_msg.data_size,
            '__wal_end': decoded_msg.replication_msg.wal_end,
            '__tlsn': decoded_msg.transaction.lsn,
            '__tcommit_ts': decoded_msg.transaction.commit_ts,
            '__tid': decoded_msg.transaction.xid,
            # Additional metadata
            '__metadata': {
                'table': {
                    'db': decoded_msg.table.db,
                    'schema': decoded_msg.table.tschema,
                    'name': decoded_msg.table.name,
                    'oid': decoded_msg.table.oid,
                    'columns': [
                        {
                            'pk': column.pk,
                            'name': column.name,
                            'dtype_oid': column.dtype_oid,
                            'dtype': column.dtype,
                            'bq_dtype': column.bq_dtype,
                            'is_nullable': column.is_nullable,
                            'ordinal_position': column.ordinal_position,
                        } for column in decoded_msg.table.columns
                    ],
                },
            },
        }
        data.update(decoded_msg.data) if decoded_msg.data else None  # None is for delete, truncate event
        data = json.dumps(data, default=json_serializer) + '\n'
        data_size = sys.getsizeof(data)

        # Register file if not exists
        if table_name not in self.opened_files:
            self.opened_files[table_name] = FileDescriptor(
                filename=filename,
                file=open(filename, 'w'),
                size=data_size,
            )
        self.opened_files[table_name].file.write(data)
        self.opened_files[table_name].size += data_size

        # Close all files if this file is too big
        if self.opened_files[table_name].size > FILEWRITER_MAX_FILE_SIZE_B:
            self.close_all_files(f'table \'{table_name}\' size exceeds {FILEWRITER_MAX_FILE_SIZE_B} bytes')

    def close_all_files(self, reason: str) -> None:
        is_any_closed = False
        for table_name in list(self.opened_files.keys()):
            if table_name in self.opened_files:
                is_any_closed = True
                self.opened_files[table_name].file.close()
                logger.info(f'Closed file \'{self.opened_files[table_name].filename}\' due to: {reason}')

                # Move streamed file to upload dir
                os.rename(self.opened_files[table_name].filename, os.path.join(UPLOAD_OUTPUT_DIR, os.path.basename(self.opened_files[table_name].filename)))

            del self.opened_files[table_name]

        if is_any_closed:
            self.send_feedback = True

        self.latest_all_file_closed_ts = datetime.now(tz=timezone.utc)

    def stop(self) -> None:
        self.cursor.close()
        self.conn.close()
        logger.debug(f'Disconnected from db: {REPL_DB_NAME} (Replication)')


if __name__ == '__main__':
    # Create output directory if not exists
    if not os.path.exists(FILEWRITER_OUTPUT_DIR):
        os.makedirs(FILEWRITER_OUTPUT_DIR)
        logger.info(f'Create stream output dir: {FILEWRITER_OUTPUT_DIR}')

    if not os.path.exists(UPLOAD_OUTPUT_DIR):
        os.makedirs(UPLOAD_OUTPUT_DIR)
        logger.info(f'Create upload output dir: {UPLOAD_OUTPUT_DIR}')

    # Clear stream output directory
    [os.remove(file) for file in glob.glob(f'{FILEWRITER_OUTPUT_DIR}/*.json')]

    # Run the streamer
    logger.info('Starting cdc streamer...')
    streamer = LogicalReplicationStreamer(host=REPL_DB_HOST, port=REPL_DB_PORT, user=REPL_DB_USER, password=REPL_DB_PASSWORD, database=REPL_DB_NAME)
    streamer.run()
