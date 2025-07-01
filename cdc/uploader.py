import glob
import grpc_tools.protoc
import importlib
import json
import logging
import os
import sys
import time
import traceback

from concurrent.futures import ThreadPoolExecutor
from data import PgColumn, PgTable, BqColumn, BqTable
from datetime import datetime, timezone
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types, writer
from google.protobuf import descriptor_pb2
from loguru import logger
from textwrap import dedent
from typing import Any

from common import send_message
from config import settings

# Configure logging
if settings.DEBUG == 1:
    logging.basicConfig(level=logging.DEBUG)
logger.add(os.path.join(settings.LOG_DIR, f'{os.path.basename(__file__)}.log'), rotation='00:00', retention='7 days', level='INFO')

PROTO_OUTPUT_DIR = os.path.join('output', settings.STREAMER_DB_NAME, 'proto')  # This must use the current directory because proto generation will throw error without --proto-path parameter

META_PARTITION_COLUMN = '__tx_commit_ts'
META_PG_COLUMNS = [
    PgColumn(pk=False, name='__m_op', dtype='varchar', bq_dtype='STRING', proto_dtype='string'),
    PgColumn(pk=False, name='__m_ord', dtype='bigint', bq_dtype='INT64', proto_dtype='int64'),
    PgColumn(pk=False, name='__m_lsn', dtype='bigint', bq_dtype='INT64', proto_dtype='int64'),
    PgColumn(pk=False, name='__m_send_ts', dtype='timestamp with time zone', bq_dtype='TIMESTAMP', proto_dtype='int64'),
    PgColumn(pk=False, name='__m_size', dtype='int', bq_dtype='INT64', proto_dtype='int32'),
    PgColumn(pk=False, name='__m_wal_end', dtype='bigint', bq_dtype='INT64', proto_dtype='int64'),
    PgColumn(pk=False, name='__tx_lsn', dtype='bigint', bq_dtype='INT64', proto_dtype='int64'),
    PgColumn(pk=False, name='__tx_commit_ts', dtype='timestamp with time zone', bq_dtype='TIMESTAMP', proto_dtype='int64'),
    PgColumn(pk=False, name='__tx_id', dtype='int', bq_dtype='INT64', proto_dtype='int32'),
    # PgColumn(name='__tb', dtype='json', bq_dtype='JSON', proto_dtype='string'),
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
    """
    This process will constantly monitor the upload directory for new files produced by the streamer.

    There will be a maximum of X threads being spawned, each thread will:
    - Take a single group of table (a single table can produce multiple files if it has big enough transactions).
    - Generate a protobuf class for that table.
    - Write file contents directly into BigQuery using BigQuery Write API, utilizing the generated protobuf class.
    """

    def __init__(self) -> None:
        self.bq_client = bigquery.Client.from_service_account_json(settings.UPLOADER_SA_FILENAME)
        self.write_client = bigquery_storage_v1.BigQueryWriteClient.from_service_account_json(settings.UPLOADER_SA_FILENAME)
        self.dataset_id_log = f'{settings.UPLOADER_BQ_LOG_DATASET_PREFIX}{settings.STREAMER_DB_NAME}'
        self.dataset_id_main = settings.STREAMER_DB_NAME
        # self.append_rows_streams: dict[str, writer.AppendRowsStream] = {}
        logger.debug(f'Connected to BQ: {self.bq_client.project}, Target dataset: {self.dataset_id_log}')

        # Create dataset if not exists
        dataset_fqn_log = f'{settings.UPLOADER_BQ_PROJECT_ID}.{self.dataset_id_log}'
        try:
            self.bq_client.get_dataset(dataset_fqn_log)
        except NotFound:
            dataset = bigquery.Dataset(dataset_fqn_log)
            dataset.location = settings.UPLOADER_BQ_DATASET_LOCATION
            self.bq_client.create_dataset(dataset)
            logger.info(f'Created dataset: {dataset_fqn_log}')

        # Get existing table columns
        self.map__bq_table_fqn__bq_table: dict[str, BqTable] = {}  # { fqn: table}
        # TODO: do not use column_name NOT LIKE '__%' because there's possibility of postgresql table contains column name starting with '__'
        results = self.bq_client.query_and_wait(dedent(
            f'''
            SELECT CONCAT(table_catalog, '.', table_schema, '.', table_name) AS fqn
                , column_name
                , data_type AS dtype
            FROM `{settings.UPLOADER_BQ_PROJECT_ID}.{self.dataset_id_log}.INFORMATION_SCHEMA.COLUMNS`
            WHERE column_name NOT LIKE r'\_\_%'  -- Exclude metadata columns
            '''
        ).strip())
        for result in results:
            if result['fqn'] not in self.map__bq_table_fqn__bq_table:
                self.map__bq_table_fqn__bq_table[result['fqn']] = BqTable(name=result['fqn'], columns=[])
            self.map__bq_table_fqn__bq_table[result['fqn']].columns.append(BqColumn(name=result['column_name'], dtype=result['dtype']))
        logger.debug('Fetched existing tables')

        # # Send starting message
        # send_message(f'_CDC Uploader [{settings.STREAM_DB_NAME}]_ started')

    def generate_and_compile_proto(self, table: PgTable):
        proto_filename = os.path.join(PROTO_OUTPUT_DIR, f'{table.proto_filename}.proto')

        # Generate proto file
        with open(proto_filename, 'w') as f:
            f.write(f'syntax = "proto3";\n\n')
            f.write(f'package {settings.STREAMER_DB_NAME};\n\n')
            f.write(f'message {table.proto_classname} {{\n')
            for i, column in enumerate(META_PG_COLUMNS + table.columns):
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
        for filename in filenames:
            with open(filename, 'r') as f:
                while line := f.readline():
                    data: dict = json.loads(line)
                    del data['__tb']  # Strip-off table schema
                    for key in data.keys():
                        if map__column__dtype[key] in {'jsonb', 'json'}:
                            data[key] = json.dumps(data[key])
                        if map__column__dtype[key] in {'timestamp with time zone'}:
                            data[key] = None if data[key] is None else int(datetime.fromisoformat(data[key]).timestamp() * 1000000)  # Convert to microseconds
                        elif map__column__dtype[key] in {'timestamp without time zone'}:
                            data[key] = None if data[key] is None else datetime.fromisoformat(data[key]).strftime('%Y-%m-%d %H:%M:%S')  # Convert to string
                    yield data

    def write(self, filenames, bq_table_log_fqn: str, pg_table: PgTable, pb2_class):
        """
        Source: https://cloud.google.com/bigquery/docs/write-api-batch#batch_load_data_using_pending_type
        """

        # Create a batch of row data by appending proto2 serialized bytes to the
        # serialized_rows repeated field.
        list__proto_rows = [types.ProtoRows()]
        current_chunk_no = 0
        current_chunk_size = 0
        for data in self.generate_raw_data(sorted(filenames), pg_table):  # Ensure transactions order
            # Serialize data
            data = pb2_class(**data).SerializeToString()
            data_size = sys.getsizeof(data)
            if data_size > settings.UPLOADER_STREAM_CHUNK_SIZE_B:
                raise ValueError(f'{bq_table_log_fqn}: data size {data_size} exceeds the limit {settings.UPLOADER_STREAM_CHUNK_SIZE_B} bytes')

            # Manage chunks
            list__proto_rows[current_chunk_no].serialized_rows.append(data)
            current_chunk_size += data_size
            if current_chunk_size > settings.UPLOADER_STREAM_CHUNK_SIZE_B:
                logger.debug(f'{bq_table_log_fqn}: chunk size {current_chunk_size} exceeds the limit {settings.UPLOADER_STREAM_CHUNK_SIZE_B} bytes')
                current_chunk_no += 1
                current_chunk_size = 0
                list__proto_rows.append(types.ProtoRows())

        total = len(list__proto_rows)
        for i, proto_rows in enumerate(list__proto_rows):
            parent = self.write_client.table_path(*bq_table_log_fqn.split('.'))
            # write_stream = types.WriteStream()

            # When creating the stream, choose the type. Use the PENDING type to wait
            # until the stream is committed before it is visible. See:
            # https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.WriteStream.Type
            # write_stream.type_ = types.WriteStream.Type.PENDING
            # write_stream = self.write_client.create_write_stream(
            #     parent=parent, write_stream=write_stream
            # )
            # stream_name = write_stream.name
            stream_name = f'{parent}/_default'

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
            append_rows_stream = writer.AppendRowsStream(self.write_client, request_template)

            # Set an offset to allow resuming this stream if the connection breaks.
            # Keep track of which requests the server has acknowledged and resume the
            # stream at the first non-acknowledged message. If the server has already
            # processed a message with that offset, it will return an ALREADY_EXISTS
            # error, which can be safely ignored.
            #
            # The first request must always have an offset of 0.
            logger.debug(f'{bq_table_log_fqn}: sending chunk ({i + 1} / {total}) ({proto_rows.serialized_rows.__len__()} rows)')
            request = types.AppendRowsRequest()
            # request.offset = 0
            proto_data = types.AppendRowsRequest.ProtoData()
            proto_data.rows = proto_rows
            request.proto_rows = proto_data

            response_future_1 = append_rows_stream.send(request)
            # response_future_1_result = response_future_1.result()

            # Shutdown background threads and close the streaming connection.
            append_rows_stream.close()
            # self.append_rows_streams.pop(bq_table_log_fqn)

            # # A PENDING type stream must be "finalized" before being committed. No new
            # # records can be written to the stream after this method has been called.
            # write_client.finalize_write_stream(name=stream_name)

            # # Commit the stream you created earlier.
            # batch_commit_write_streams_request = types.BatchCommitWriteStreamsRequest()
            # batch_commit_write_streams_request.parent = parent
            # batch_commit_write_streams_request.write_streams = [stream_name]
            # write_client.batch_commit_write_streams(batch_commit_write_streams_request)

    def upload(self, pg_table_fqn, filenames: set[str]):
        """
        Each table set must run in a single thread to avoid race conditions on updating BQ schema
        """

        if not filenames:
            raise ValueError(f'{pg_table_fqn}: no files to upload')

        pg_table_db, pg_table_schema, pg_table_name = pg_table_fqn.split('.')

        bq_table_log_fqn = f'{settings.UPLOADER_BQ_PROJECT_ID}.{self.dataset_id_log}.{pg_table_schema}__{pg_table_name}'

        # Detect schema changes
        for filename in sorted(filenames):  # Ensure transactions order
            pg_table = json.loads(read_file_last_line(filename))['__tb']  # Get the latest transaction as it represents the latest schema
            pg_table['columns'] = [PgColumn(**column) for column in pg_table['columns']]  # Convert into python object
            pg_table = PgTable(**pg_table)

            # Detect new table
            if bq_table_log_fqn not in self.map__bq_table_fqn__bq_table:
                self.map__bq_table_fqn__bq_table[bq_table_log_fqn] = BqTable(
                    name=bq_table_log_fqn,
                    columns=[BqColumn(name=column.name, dtype=column.dtype) for column in pg_table.columns],
                )

                # Create log table
                # For cost and performance reason, partition and cluster columns are used to merge to main table
                logger.info(f'{pg_table_fqn}: new log table: {bq_table_log_fqn}')
                bq_table_schema = (
                    [bigquery.SchemaField(pg_column.name, pg_column.bq_dtype, mode='REQUIRED') for pg_column in META_PG_COLUMNS] +  # Metadata columns should not be null
                    [bigquery.SchemaField(pg_column.name, pg_column.bq_dtype) for pg_column in pg_table.columns]
                )
                bq_table = bigquery.Table(bq_table_log_fqn, schema=bq_table_schema)
                bq_table.require_partition_filter = True
                bq_table.time_partitioning = bigquery.TimePartitioning(field=META_PARTITION_COLUMN)
                bq_table.partitioning_type = 'DAY'
                self.bq_client.create_table(bq_table)
                continue

            # Detect new columns
            new_columns: list[PgColumn] = []
            existing_columns = {column.name: column.dtype for column in self.map__bq_table_fqn__bq_table[bq_table_log_fqn].columns}
            for column in pg_table.columns:
                if column.name not in existing_columns:
                    new_columns.append(column)
                    self.map__bq_table_fqn__bq_table[bq_table_log_fqn].columns.append((column.name, column.bq_dtype))
            if new_columns:
                # Alter log table
                logger.info(f'{pg_table_fqn}: new log table column(s): {bq_table_log_fqn} -> {new_columns}')
                bq_table = self.bq_client.get_table(bq_table_log_fqn)
                bq_table.schema = bq_table.schema[:] + [bigquery.SchemaField(pg_column.name, pg_column.bq_dtype) for pg_column in new_columns]
                self.bq_client.update_table(bq_table)
                continue

        # Generate proto file
        self.generate_and_compile_proto(pg_table)  # Here we will get the latest pg_columns read from the last file
        pb2_class = self.import_proto(pg_table)
        logger.debug(f'{pg_table_fqn}: proto file compiled')

        self.write(filenames, bq_table_log_fqn, pg_table, pb2_class)

        logger.info(f'{pg_table_fqn}: streamed to log table')

        # Remove all processed files
        [os.remove(filename) for filename in filenames]

        # Write to file to notify merger
        open(os.path.join(settings.MERGER_OUTPUT_DIR, f'{pg_table_fqn}.txt'), 'w').write(pg_table_fqn)

        return pg_table.fqn


if __name__ == '__main__':
    # Create output directory if not exists
    if not os.path.exists(PROTO_OUTPUT_DIR):
        os.makedirs(PROTO_OUTPUT_DIR)
        logger.info(f'Create proto output dir: {PROTO_OUTPUT_DIR}')
    if not os.path.exists(settings.MERGER_OUTPUT_DIR):
        os.makedirs(settings.MERGER_OUTPUT_DIR)
        logger.info(f'Create merge output dir: {settings.MERGER_OUTPUT_DIR}')

    thread_pool_executor = ThreadPoolExecutor(max_workers=settings.UPLOADER_THREADS)
    logger.info('Starting cdc uploader...')
    uploader = Uploader()
    latest_file_ts = datetime.now(timezone.utc)
    latest_no_file_print_ts = datetime.now(timezone.utc)
    logger.info(f'Listening to folder: {settings.UPLOADER_OUTPUT_DIR}')
    while True:
        if filenames := glob.glob(f'{settings.UPLOADER_OUTPUT_DIR}/*.json'):

            # Group by table
            grouped_filenames: dict[str, set[str]] = {}
            for filename in filenames:
                pg_table_fqn = os.path.basename(filename).split('-')[-1].removesuffix('.json')  # yyyyMMddhhmmssffffff-yyyyMMddhhmmssffffff-pg_table_fqn.json
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
                    logger.error(f'Error processing table: {pg_table_fqn}, files: {filenames}\nTraceback:\n{t}')
                    send_message(f'_CDC Uploader [{settings.STREAMER_DB_NAME}]_ error: **{e}**\nTable: **{pg_table_fqn}**\nFiles:\n```{filenames}```Traceback:\n```{t}```')
                    raise e

            latest_file_ts = datetime.now(timezone.utc)
        else:
            time.sleep(settings.UPLOADER_FILE_POLL_INTERVAL_S)

            now = datetime.now(timezone.utc)
            if (now - latest_file_ts).total_seconds() > settings.UPLOADER_NO_FILE_REPORT_INTERVAL_S and (now - latest_no_file_print_ts).total_seconds() > settings.UPLOADER_NO_FILE_REPORT_INTERVAL_S:
                logger.warning(f'No file for {(now - latest_file_ts).total_seconds()} seconds')
                latest_no_file_print_ts = now
