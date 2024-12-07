import dotenv
import glob
import grpc_tools.protoc
import importlib
import json
import os
import time
import traceback

from concurrent.futures import ThreadPoolExecutor
from data import PgcColumn, PgTable, BqColumn, BqTable
from datetime import datetime
from google.cloud import bigquery, bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types, writer
from google.protobuf import descriptor_pb2
from loguru import logger
from typing import Any

from alert import send_message

# import logging
# logging.basicConfig(level=logging.DEBUG)
# logging.basicConfig(level=logging.INFO)

dotenv.load_dotenv()

SA_FILENAME = os.environ['SA_FILENAME']

REPL_DB_NAME = os.environ['REPL_DB_NAME']

PROTO_OUTPUT_DIR = os.path.join('output', REPL_DB_NAME, 'proto')

UPLOAD_OUTPUT_DIR = os.path.join('output', REPL_DB_NAME, 'upload')
UPLOADER_THREADS = int(os.environ['UPLOADER_THREADS'])
UPLOADER_FILE_POLL_INTERVAL_S = int(os.environ['UPLOADER_FILE_POLL_INTERVAL_S'])
UPLOADER_STREAM_CHUNK_SIZE_B = int(os.environ['UPLOADER_STREAM_CHUNK_SIZE_B'])

DISCORD_WEBHOOK_URL = os.environ['DISCORD_WEBHOOK_URL']

META_PG_COLUMNS = [
    PgcColumn(pk=False, name='__m_op', dtype_oid=0, dtype='varchar', bq_dtype='', proto_dtype='string', is_nullable=True, ordinal_position=0),
    PgcColumn(pk=False, name='__m_lsn', dtype_oid=0, dtype='bigint', bq_dtype='', proto_dtype='int64', is_nullable=True, ordinal_position=0),
    PgcColumn(pk=False, name='__m_send_ts', dtype_oid=0, dtype='timestamp with time zone', bq_dtype='', proto_dtype='int64', is_nullable=True, ordinal_position=0),
    PgcColumn(pk=False, name='__m_size', dtype_oid=0, dtype='int', bq_dtype='', proto_dtype='int32', is_nullable=True, ordinal_position=0),
    PgcColumn(pk=False, name='__m_wal_end', dtype_oid=0, dtype='bigint', bq_dtype='', proto_dtype='int64', is_nullable=True, ordinal_position=0),
    PgcColumn(pk=False, name='__tx_lsn', dtype_oid=0, dtype='bigint', bq_dtype='', proto_dtype='int64', is_nullable=True, ordinal_position=0),
    PgcColumn(pk=False, name='__tx_commit_ts', dtype_oid=0, dtype='timestamp with time zone', bq_dtype='', proto_dtype='int64', is_nullable=True, ordinal_position=0),
    PgcColumn(pk=False, name='__tx_id', dtype_oid=0, dtype='int', bq_dtype='', proto_dtype='int32', is_nullable=True, ordinal_position=0),
    PgcColumn(pk=False, name='__tb', dtype_oid=0, dtype='json', bq_dtype='', proto_dtype='string', is_nullable=True, ordinal_position=0),
]
META_MAP_PG_COLUMNS = {column.name: column.dtype for column in META_PG_COLUMNS}


def read_file_last_line(file: str) -> str:
    with open(file, 'rb') as f:
        try:  # catch OSError in case of a one line file
            f.seek(-2, os.SEEK_END)
            while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
        except OSError:
            f.seek(0)
        return f.readline().decode()


class Uploader:
    def __init__(self) -> None:
        self.bq_client = bigquery.Client.from_service_account_json(SA_FILENAME)
        self.dataset_id_log = f'log__{REPL_DB_NAME}'
        self.dataset_id_main = REPL_DB_NAME
        self.append_rows_streams: dict[str, writer.AppendRowsStream] = {}
        logger.debug(f'Connected to BQ: {self.bq_client.project}')

        # Get existing table columns
        self.map__bq_table_fqn__bq_table: dict[str, BqTable] = {}  # { fqn: table}
        results = self.bq_client.query_and_wait(
            f'''
            SELECT CONCAT(table_catalog, '.', table_schema, '.', table_name) AS fqn
                , column_name
                , data_type AS dtype
            FROM `{self.bq_client.project}.{self.dataset_id_log}.INFORMATION_SCHEMA.COLUMNS`
            WHERE column_name NOT LIKE r'\_\_%'  -- Exclude metadata columns
            '''
        )
        for result in results:
            if result['fqn'] not in self.map__bq_table_fqn__bq_table:
                self.map__bq_table_fqn__bq_table[result['fqn']] = BqTable(name=result['fqn'])
            self.map__bq_table_fqn__bq_table[result['fqn']].columns.append(BqColumn(name=result['column_name'], dtype=result['dtype']))
        logger.debug('Fetched existing tables')

        # Send starting message
        send_message(f'_cdc_uploader [{REPL_DB_NAME}]_ started')

    def generate_and_compile_proto(self, table: PgTable):
        proto_filename = os.path.join(PROTO_OUTPUT_DIR, f'{table.proto_filename}.proto')

        # Generate proto file
        with open(proto_filename, 'w') as f:
            f.write(f'syntax = "proto3";\n\n')
            f.write(f'package {REPL_DB_NAME};\n\n')
            f.write(f'message {table.proto_classname} {{\n')
            for i, column in enumerate(sorted(META_PG_COLUMNS + table.columns, key=lambda x: x.ordinal_position)):
                # All columns are optional
                # This fixes the proto serializer issue where default values are not serialized
                # Example: the default value for 'bool' is 'false'
                #   If we put 'false' into a 'bool' field, it won't be serialized and will output 'NULL' instead of 'false'
                f.write(f'    optional {column.proto_dtype} {column.name} = {i + 1};\n')
            f.write('}\n')

        # Compile proto file
        proto_filename = os.path.join(PROTO_OUTPUT_DIR, f'{table.proto_filename}.proto')
        grpc_tools.protoc.main(['protoc', '--python_out=.', proto_filename])

    def import_proto(self, table: PgTable) -> Any:
        pb2 = importlib.import_module(f'{PROTO_OUTPUT_DIR.replace(os.sep, ".")}.{table.proto_filename}_pb2')
        pb2_class = getattr(pb2, table.proto_classname)
        return pb2_class

    def generate_raw_data(self, filenames: set[str], pg_table: PgTable):
        map__column__dtype = {column.name: column.dtype for column in pg_table.columns}
        map__column__dtype.update(META_MAP_PG_COLUMNS)
        for filename in sorted(filenames):  # Ensure transactions order
            with open(filename, 'r') as f:
                while line := f.readline():
                    data: dict = json.loads(line)
                    # del data['__tb']  # Strip-off table schema
                    for key in data.keys():
                        if map__column__dtype[key] in {'jsonb', 'json'}:
                            data[key] = json.dumps(data[key])
                        if map__column__dtype[key] in {'timestamp with time zone'}:
                            data[key] = int(datetime.fromisoformat(data[key]).timestamp() * 1000000)  # Convert to microseconds
                        elif map__column__dtype[key] in {'timestamp without time zone'}:
                            data[key] = datetime.fromisoformat(data[key]).strftime('%Y-%m-%d %H:%M:%S')  # Convert to string
                    yield data

    def write(self, filenames, bq_table_log_fqn: str, pg_table: PgTable, pb2_class):
        """
        Source: https://cloud.google.com/bigquery/docs/write-api-batch#batch_load_data_using_pending_type
        """

        # Create a batch of row data by appending proto2 serialized bytes to the
        # serialized_rows repeated field.
        proto_rows = types.ProtoRows()
        for data in self.generate_raw_data(filenames, pg_table):
            proto_rows.serialized_rows.append(pb2_class(**data).SerializeToString())

        write_client = bigquery_storage_v1.BigQueryWriteClient.from_service_account_json(SA_FILENAME)
        parent = write_client.table_path(*bq_table_log_fqn.split('.'))
        write_stream = types.WriteStream()

        # When creating the stream, choose the type. Use the PENDING type to wait
        # until the stream is committed before it is visible. See:
        # https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.WriteStream.Type
        write_stream.type_ = types.WriteStream.Type.PENDING
        write_stream = write_client.create_write_stream(
            parent=parent, write_stream=write_stream
        )
        stream_name = write_stream.name

        # Create a template with fields needed for the first request.
        request_template = types.AppendRowsRequest()

        # The initial request must contain the stream name.
        request_template.write_stream = stream_name

        # So that BigQuery knows how to parse the serialized_rows, generate a
        # protocol buffer representation of your message descriptor.
        proto_schema = types.ProtoSchema()
        proto_descriptor = descriptor_pb2.DescriptorProto()
        pb2_class.DESCRIPTOR.CopyToProto(proto_descriptor)
        proto_schema.proto_descriptor = proto_descriptor
        proto_data = types.AppendRowsRequest.ProtoData()
        proto_data.writer_schema = proto_schema
        request_template.proto_rows = proto_data

        # Some stream types support an unbounded number of requests. Construct an
        # AppendRowsStream to send an arbitrary number of requests to a stream.
        if bq_table_log_fqn not in self.append_rows_streams:
            self.append_rows_streams[bq_table_log_fqn] = writer.AppendRowsStream(write_client, request_template)
        append_rows_stream = self.append_rows_streams[bq_table_log_fqn]

        # Set an offset to allow resuming this stream if the connection breaks.
        # Keep track of which requests the server has acknowledged and resume the
        # stream at the first non-acknowledged message. If the server has already
        # processed a message with that offset, it will return an ALREADY_EXISTS
        # error, which can be safely ignored.
        #
        # The first request must always have an offset of 0.
        request = types.AppendRowsRequest()
        request.offset = 0
        proto_data = types.AppendRowsRequest.ProtoData()
        proto_data.rows = proto_rows
        request.proto_rows = proto_data

        response_future_1 = append_rows_stream.send(request)
        response_future_1_result = response_future_1.result()

        # Shutdown background threads and close the streaming connection.
        append_rows_stream.close()
        self.append_rows_streams.pop(bq_table_log_fqn)

        # A PENDING type stream must be "finalized" before being committed. No new
        # records can be written to the stream after this method has been called.
        write_client.finalize_write_stream(name=write_stream.name)

        # Commit the stream you created earlier.
        batch_commit_write_streams_request = types.BatchCommitWriteStreamsRequest()
        batch_commit_write_streams_request.parent = parent
        batch_commit_write_streams_request.write_streams = [write_stream.name]
        write_client.batch_commit_write_streams(batch_commit_write_streams_request)

    def upload(self, pg_table_fqn, filenames: set[str]):
        """
        Each tables must be run serially to avoid race conditions on updating BQ external table
        """

        if not filenames:
            raise ValueError(f'{pg_table_fqn}: no files to upload')

        pg_table_db, pg_table_schema, pg_table_name = pg_table_fqn.split('.')

        bq_table_log_fqn = f'{self.bq_client.project}.{self.dataset_id_log}.{pg_table_schema}__{pg_table_name}'

        # Detect schema changes
        for filename in sorted(filenames):  # Ensure transactions order
            pg_table = json.loads(read_file_last_line(filename))['__tb']  # Get the latest transaction as it represents the latest schema
            pg_table['columns'] = [PgcColumn(**column) for column in pg_table['columns']]
            pg_table = PgTable(**pg_table)

            # Detect new table
            if bq_table_log_fqn not in self.map__bq_table_fqn__bq_table:
                self.map__bq_table_fqn__bq_table[bq_table_log_fqn] = BqTable(
                    name=bq_table_log_fqn,
                    columns=[BqColumn(name=column.name, dtype=column.dtype) for column in sorted(pg_table.columns, key=lambda x: x.ordinal_position)],
                )

                # Create log table
                logger.info(f'{pg_table_fqn}: new log table: {bq_table_log_fqn}')
                columns_str = ',\n'.join([f'    `{column.name}` {column.bq_dtype}' for column in sorted(pg_table.columns, key=lambda x: x.ordinal_position)])
                self.bq_client.query_and_wait(
                    f'''
                    CREATE TABLE `{bq_table_log_fqn}` (
                        `__m_op` STRING,
                        `__m_lsn` INT64,
                        `__m_send_ts` TIMESTAMP,
                        `__m_size` INT64,
                        `__m_wal_end` INT64,
                        `__tx_lsn` INT64,
                        `__tx_commit_ts` TIMESTAMP,
                        `__tx_id` INT64,
                        `__tb` JSON,
                    {columns_str}
                    )
                    PARTITION BY DATE(`__tx_commit_ts`)
                    CLUSTER BY (`__tx_commit_ts`)
                    OPTIONS (
                        require_partition_filter=true
                    );
                    '''
                )
                continue

            # Detect new columns
            new_columns = []
            existing_columns = {column.name: column.dtype for column in self.map__bq_table_fqn__bq_table[bq_table_log_fqn].columns}
            for column in pg_table.columns:
                if column.name not in existing_columns:
                    new_columns.append((column.name, column.dtype))
                    self.map__bq_table_fqn__bq_table[bq_table_log_fqn].columns.append((column.name, column.bq_dtype))
            if new_columns:
                # Alter log table
                logger.info(f'{pg_table_fqn}: new log table column(s): {bq_table_log_fqn} -> {new_columns}')
                add_columns_str = ',\n'.join([f'ADD COLUMN `{column_name}` {column_dtype}' for column_name, column_dtype in new_columns])
                self.bq_client.query_and_wait(
                    f'''
                    ALTER TABLE `{bq_table_log_fqn}`
                    {add_columns_str};
                    '''
                )
                continue

        # Generate proto file
        self.generate_and_compile_proto(pg_table)  # Here we will get the latest pg_columns read from the last file
        pb2_class = self.import_proto(pg_table)
        logger.debug(f'{pg_table_fqn}: compiled proto file')

        self.write_pending(filenames, bq_table_log_fqn, pg_table, pb2_class)

        logger.info(f'{pg_table_fqn}: streamed to log table')

        # Remove all processed files
        [os.remove(filename) for filename in filenames]
        logger.debug(f'{pg_table_fqn}: removed processed files')

        return pg_table.fqn


if __name__ == '__main__':
    # Create output directory if not exists
    if not os.path.exists(PROTO_OUTPUT_DIR):
        os.makedirs(PROTO_OUTPUT_DIR)
        logger.info(f'Create proto output dir: {PROTO_OUTPUT_DIR}')

    thread_pool_executor = ThreadPoolExecutor(max_workers=UPLOADER_THREADS)
    logger.info('Starting cdc uploader...')
    uploader = Uploader()
    while True:
        if filenames := glob.glob(f'{UPLOAD_OUTPUT_DIR}/*.json'):

            # Group by table
            grouped_filenames: dict[str, set[str]] = {}
            for filename in filenames:
                pg_table_fqn = os.path.basename(filename).split('-')[1].removesuffix('.json')  # yyyyMMddhhmmss-pg_table_fqn.json
                if pg_table_fqn not in grouped_filenames:
                    grouped_filenames[pg_table_fqn] = set()
                grouped_filenames[pg_table_fqn].add(filename)

            # Process each group in a separate thread
            futures = [(pg_table_fqn, filenames, thread_pool_executor.submit(uploader.upload, pg_table_fqn.removesuffix('.json'), filenames)) for pg_table_fqn, filenames in grouped_filenames.items()]
            # Wait for all threads to finish before processing the next batch
            for pg_table_fqn, filenames, future in futures:
                try:
                    future.result()
                except Exception as e:
                    t = traceback.format_exc()
                    send_message(f'_cdc_uploader [{REPL_DB_NAME}]_ error: **{e}**\nTable: **{pg_table_fqn}**\nFiles:\n```{filenames}```Traceback:\n```{t}```')
                    raise e

        else:
            time.sleep(UPLOADER_FILE_POLL_INTERVAL_S)
