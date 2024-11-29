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
REPL_DB_PORT = os.environ['REPL_DB_PORT']
REPL_DB_USER = os.environ['REPL_DB_USER']
REPL_DB_PASSWORD = os.environ['REPL_DB_PASSWORD']
REPL_DB_NAME = os.environ['REPL_DB_NAME']
REPL_PUBL_NAME = os.environ['REPL_PUBL_NAME']
REPL_SLOT_NAME = os.environ['REPL_SLOT_NAME']

STREAM_NO_MESSAGE_REPORT_INTERVAL_S = os.environ['STREAM_NO_MESSAGE_REPORT_INTERVAL_S']  # 1 minute, if no message received for this time, report it
STREAM_DELAY_PRINT_INTERVAL_S = os.environ['STREAM_DELAY_PRINT_INTERVAL_S']

FILEWRITER_OUTPUT_DIR = os.path.join('output', REPL_DB_NAME, 'stream')
FILEWRITER_MAX_FILE_SIZE_B = os.environ['FILEWRITER_MAX_FILE_SIZE_B']  # 1 GB, if a single file exceeds this size, close all files
FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S = os.environ['FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S']  # 1 minute, if a file is opened for this time, close all files
FILEWRITER_NO_MESSAGE_WAIT_TIME_S = os.environ['FILEWRITER_NO_MESSAGE_WAIT_TIME_S']  # 1 minute, if no message received for this time, close all files

CONSUMER_QUEUE_MAX_SIZE = os.environ['CONSUMER_QUEUE_MAX_SIZE']
CONSUMER_POLL_INTERVAL_S = os.environ['CONSUMER_POLL_INTERVAL_S']

UPLOAD_OUTPUT_DIR = os.path.join('output', REPL_DB_NAME, 'upload')


class LogicalReplicationStreamer:
    """
    Main process, responsible for consuming replication message from the pipe and converting it into JSON format

    This process as another subprocess `file_writer()` to consume the decoded messages and write it into files.
    """

    def __init__(self, host: str, port: str, user: str, password: str, database: str, aplication_name: str = f'streamer-{REPL_DB_NAME}-{uuid.uuid4()}', **kwargs) -> None:
        self.dsn = psycopg2.extensions.make_dsn(host=host, port=port, user=user, password=password, database=database, application_name=aplication_name, **kwargs)

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
        logger.info('Starting cdc streamer...')
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
        empty_counter = 0
        latest_no_msg_print_ts = datetime.now(tz=timezone.utc)
        try:
            while not self.exception_event.is_set():
                if not self.q.empty():
                    empty_counter = 0

                    msg: ReplicationMessage = self.q.get()
                    self.latest_lsn = msg.data_start
                    msg = self.decoder.decode(msg)

                    if msg is None:  # From begin, commit
                        continue
                    elif type(msg) == TransactionEvent:  # From insert, update, delete
                        msg_to_write = [msg]
                    elif type(msg) == list:  # From update, truncate
                        msg_to_write = msg

                    for msg in msg_to_write:
                        self.write_to_file(msg)
                        now = datetime.now(tz=timezone.utc)
                        if (now - self.latest_delay_print_ts).total_seconds() > STREAM_DELAY_PRINT_INTERVAL_S:
                            logger.info(f'TXID: {msg.transaction.xid}, LSN: {msg.transaction.lsn}, DELAY: {(now - msg.transaction.commit_ts).total_seconds()} s')
                            self.latest_delay_print_ts = now

                else:
                    empty_counter += 1

                    now = datetime.now(tz=timezone.utc)
                    if (now - self.latest_msg_ts).total_seconds() > STREAM_NO_MESSAGE_REPORT_INTERVAL_S and (now - latest_no_msg_print_ts).total_seconds() > STREAM_NO_MESSAGE_REPORT_INTERVAL_S:
                        logger.warning(f'No message for {(now - self.latest_msg_ts).total_seconds()} seconds')
                        latest_no_msg_print_ts = now

                    time.sleep(CONSUMER_POLL_INTERVAL_S)

                    # Close all files if it's opened for too long
                    if (datetime.now(tz=timezone.utc) - self.latest_all_file_closed_ts).total_seconds() > FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S:
                        self.close_all_files(f'Reached all files opened for {FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S} seconds')
                        self.latest_all_file_closed_ts = datetime.now(tz=timezone.utc)

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

    def write_to_file(self, msg: TransactionEvent) -> None:
        table_name = f'{msg.table.tschema}.{msg.table.name}'
        filename = os.path.join(FILEWRITER_OUTPUT_DIR, f'{table_name}-{datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M%S")}.json')
        dirname = os.path.dirname(filename)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        # Construct data
        data = {
            'op': msg.op,
            'msg_lsn': msg.replication_msg.data_start,
            'msg_send_time': msg.replication_msg.send_time,
            'msg_data_size': msg.replication_msg.data_size,
            'msg_wal_end': msg.replication_msg.wal_end,
            'transaction_lsn': msg.transaction.lsn,
            'transaction_commit_ts': msg.transaction.commit_ts,
            'transaction_xid': msg.transaction.xid,
        }
        data.update(msg.data) if msg.data else None
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
    streamer = LogicalReplicationStreamer(host=REPL_DB_HOST, port=REPL_DB_PORT, user=REPL_DB_USER, password=REPL_DB_PASSWORD, database=REPL_DB_NAME)
    streamer.run()
