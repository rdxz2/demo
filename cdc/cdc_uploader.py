import dotenv
import glob
import gzip
import os
import shutil
import time
import json

from data import Column
from concurrent.futures import ThreadPoolExecutor
from google.cloud import bigquery, storage
from io import StringIO
from loguru import logger

dotenv.load_dotenv()

SA_FILENAME = os.environ['SA_FILENAME']

GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']

GCS_BUCKET = os.environ['GCS_BUCKET']

REPL_DB_NAME = os.environ['REPL_DB_NAME']

UPLOAD_OUTPUT_DIR = os.path.join('output', REPL_DB_NAME, 'upload')
UPLOADER_THREADS = int(os.environ['UPLOADER_THREADS'])

HIVE_PARTITION_COLUMN = '__tcommit_dt'


def compress(src_file: str):
    dst_file = src_file + '.gz'

    os.remove(dst_file) if os.path.exists(dst_file) else None
    with open(src_file, 'rb') as f_in:
        with gzip.open(dst_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    return dst_file


class Uploader:
    def __init__(self) -> None:
        self.bq = bigquery.Client.from_service_account_json(SA_FILENAME)
        self.dataset = self.bq.dataset(f'log__{REPL_DB_NAME}')
        logger.debug(f'Connected to BQ: {self.dataset.dataset_id}')

        # Get all tables
        self.map__table__columns: dict[str, list[tuple]] = {}  # { table_name: { column_name : column_type } }
        tables = self.bq.list_tables(self.dataset.dataset_id)
        for table in tables:
            table = self.bq.get_table(f'{table.project}.{table.dataset_id}.{table.table_id}')
            self.map__table__columns[table.table_id] = [(field.name, field.field_type) for field in table.schema]
        logger.debug('Loaded all tables')

        self.gcs = storage.Client.from_service_account_json(SA_FILENAME)
        self.bucket = self.gcs.bucket(GCS_BUCKET)
        logger.debug(f'Connected to GCS: {self.bucket.name}')

    def upload(self, group: str, filenames: set[str]):
        """
        Each tables must be run serially to avoid race conditions on updating BQ external table
        """

        for filename in filenames:
            # Read file metadata
            is_new_table = False
            is_columns_changed = False
            with open(filename, 'r') as f:
                # all_msgs = []
                while line := f.readline():
                    metadata: dict = json.loads(line)['__metadata']
                    table = metadata['table']
                    table_name = table['name']
                    new_columns: list[Column] = [Column(**column) for column in table['columns']]

                    # Determine changed metadata
                    if table_name not in self.map__table__columns:  # New table
                        self.map__table__columns[table_name] = [(column.name, column.bq_dtype) for column in sorted(new_columns, key=lambda x: x.ordinal_position)]
                        is_new_table = True

                    existing_columns = {column[0]: column[1] for column in self.map__table__columns[table_name]}
                    for column in new_columns:
                        if column.name not in existing_columns:  # New column
                            is_columns_changed = True
                            break
                        elif column.bq_dtype != existing_columns[column.name]:  # Changed column datatype
                            is_columns_changed = True
                            break

                    # all_msgs.append(msg)

            # Compress data
            compressed_filename = compress(filename)
            logger.debug(f'Compress: {filename} --> {compressed_filename}')
            compressed_filename_base = os.path.basename(compressed_filename)

            # Determine final GCS path
            with StringIO(compressed_filename_base.removesuffix('.json.gz')) as buffer:
                year = buffer.read(4)
                month = buffer.read(2)
                day = buffer.read(2)
                # hour = buffer.read(2)
                # minute = buffer.read(2)
                # second = buffer.read(2)
                # dash = buffer.read(1)
                # table_name = buffer.read()
            gcs_path_hive = f'cdc/{REPL_DB_NAME}/{table_name}'
            gcs_path = f'{gcs_path_hive}/{HIVE_PARTITION_COLUMN}={year}-{month}-{day}/{compressed_filename_base}'

            # Upload
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_filename(compressed_filename)

            logger.info(f'Upload: {compressed_filename} --> {blob.name}')

            if is_new_table or is_columns_changed:
                logger.info(f'New table: {table_name}')
                columns_str = ',\n'.join([f'    {column.name} {column.bq_dtype}' for column in sorted(new_columns, key=lambda x: x.ordinal_position)])
                job = self.bq.query(
                    f'''
DROP TABLE IF EXISTS `{self.dataset.dataset_id}.{table_name}`;

CREATE EXTERNAL TABLE `{self.dataset.dataset_id}.{table_name}` (
    __op STRING,
    __lsn INTEGER,
    __send_ts TIMESTAMP,
    __size INTEGER,
    __wal_end INTEGER,
    __tlsn INTEGER,
    __tcommit_ts TIMESTAMP,
    __tid INTEGER,
    __metadata JSON,
{columns_str}
)
WITH PARTITION COLUMNS ({HIVE_PARTITION_COLUMN} DATE)
OPTIONS (
  format='NEWLINE_DELIMITED_JSON',
  compression='GZIP',
  uris=['gs://{self.bucket.name}/{gcs_path_hive}/*'],
  hive_partition_uri_prefix='gs://{self.bucket.name}/{gcs_path_hive}',
  require_hive_partition_filter=TRUE
);
'''.strip()
                )
                job.result()

            os.remove(compressed_filename)
            os.remove(filename)


if __name__ == '__main__':
    thread_pool_executor = ThreadPoolExecutor(max_workers=UPLOADER_THREADS)
    logger.info('Starting cdc uploader...')
    uploader = Uploader()
    while True:
        if filenames := glob.glob(f'{UPLOAD_OUTPUT_DIR}/*.json'):
            # Group by table
            grouped_filenames: dict[str, set[str]] = {}
            for filename in filenames:
                table_name = os.path.basename(filename).split('-')[1].removeprefix('.json')
                if table_name not in grouped_filenames:
                    grouped_filenames[table_name] = set()
                grouped_filenames[table_name].add(filename)

            # Process each group in a separate thread
            futures = [thread_pool_executor.submit(uploader.upload, group, filenames) for group, filenames in grouped_filenames.items()]
            [future.result() for future in futures]  # Wait for all task to complete before moving on another batch
        else:
            time.sleep(1)
