import click
import csv
import dotenv
import glob
import gzip
import io
import logging
import os
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import sys
import threading

from datetime import datetime, timezone
from datetime import datetime, timezone
from google.api_core.exceptions import NotFound
from google.cloud import storage, bigquery
from loguru import logger
from queue import Queue, Empty
from utill.my_string import generate_random_string

from decoder import MAP__PG_DTYPE__BQ_DTYPE

dotenv.load_dotenv()

if os.environ['DEBUG'] == '1':
    logging.basicConfig(level=logging.DEBUG)

SA_FILENAME = os.environ['SA_FILENAME']

REPL_DB_HOST = os.environ['REPL_DB_HOST']
REPL_DB_PORT = int(os.environ['REPL_DB_PORT'])
REPL_DB_USER = os.environ['REPL_DB_USER']
REPL_DB_PASS = os.environ['REPL_DB_PASS']
REPL_DB_NAME = os.environ['REPL_DB_NAME']

PGTOBQ_OUTPUT_DIR = os.environ['PGTOBQ_OUTPUT_DIR']

BQ_PROJECT_ID = os.environ['BQ_PROJECT_ID']
BQ_DATASET_LOCATION = os.environ['BQ_DATASET_LOCATION']
BQ_LOG_DATASET_PREFIX = os.environ['BQ_LOG_DATASET_PREFIX']

GCS_BUCKET = os.environ['GCS_BUCKET']
GCS_BASE_PATH_PGTOBQ = os.environ['GCS_BASE_PATH_PGTOBQ']

STREAM_FILEWRITER_MAX_FILE_SIZE_B = int(os.environ['STREAM_FILEWRITER_MAX_FILE_SIZE_B'])

PGTOBQ_UPLOAD_QUEUE_MAX_SIZE = int(os.environ['PGTOBQ_UPLOAD_QUEUE_MAX_SIZE'])

APPLICATION_NAME = f'cdc-pgtobq-{REPL_DB_NAME}-{generate_random_string()}'


def download_pg_table(cursor: psycopg2.extensions.cursor, pg_table_fqn: str, pg_cols: list[str], output_filename: str):
    cols_str = ', '.join([f'"{col}"' for col in pg_cols])

    q = f'''
        COPY (
            SELECT {cols_str}
            FROM {pg_table_fqn}
        )
        TO STDOUT
        WITH DELIMITER ','
        CSV HEADER;
        '''
    cursor.copy_expert(q, open(output_filename, 'w'))


def write_data_to_gzip(data: list[list], header=str):
    file_obj = io.BytesIO()
    with gzip.GzipFile(fileobj=file_obj, mode='wb') as gzipped_file:
        for row in data:
            csv_row = ','.join(map(str, row)) + '\n'
            gzipped_file.write(csv_row.encode('utf-8'))
    file_obj.seek(0)
    return file_obj


def compress_csv(q: Queue, output_filename: str, reader, header: list[str], part: int = 1):
    final_filename = f'{output_filename}.{part}.gz'
    total_size = 0

    gz = gzip.open(final_filename, 'wt')

    writer = csv.writer(gz, quoting=csv.QUOTE_ALL)

    # Write header
    writer.writerow(header)

    logger.debug(f'Compressing {final_filename}...')

    is_any_data = False
    for row in reader:
        is_any_data = True
        writer.writerow(row)

        size = sys.getsizeof(row)
        total_size += size

        if total_size > STREAM_FILEWRITER_MAX_FILE_SIZE_B:
            # Finalize current batch
            gz.close()

            # Send signal to upload thread
            logger.info(f'Compressed {final_filename}')
            q.put(final_filename)

            # Process another batch
            compress_csv(q, output_filename, reader, header, part + 1)

            is_any_data = False

    if is_any_data:
        # Finalize the final batch
        gz.close()

        # Send signal to upload thread
        logger.info(f'Compressed {final_filename}')
        q.put(final_filename)


def thread_upload(q: Queue, stream_done_event: threading.Event, bucket: storage.Bucket, gcs_dirname: str):
    while not stream_done_event.is_set() or not q.empty():
        try:
            final_filename = q.get(timeout=1)
            blob = bucket.blob(f'{GCS_BASE_PATH_PGTOBQ}/{gcs_dirname}/{os.path.basename(final_filename)}')
            logger.debug(f'Uploading {final_filename} to {blob.name}...')
            blob.upload_from_filename(final_filename)

            # Remove uploaded file
            os.remove(final_filename)

            logger.info(f'Uploaded {final_filename} to {blob.name}')

        except Empty:
            pass


@click.command()
@click.argument('pg_table_fqn', type=str)
@click.option('--force', is_flag=True, type=bool, help='Force reprocess even the table if exists (will rename the existing table)')
def main(pg_table_fqn: str, force: bool):
    now = datetime.now(timezone.utc)
    # now_ts = now.strftime('%Y-%m-%d %H:%M:%S.%f%Z')
    now_ts = int(now.timestamp() * 1000000)
    pg_table_schema, pg_table_name = pg_table_fqn.split('.')
    bq_dataset_id = REPL_DB_NAME
    bq_dataset_fqn = f'{BQ_PROJECT_ID}.{bq_dataset_id}'
    bq_table_id = f'{pg_table_schema}__{pg_table_name}'
    bq_table_fqn = f'{BQ_PROJECT_ID}.{bq_dataset_id}.{bq_table_id}'
    logger.info(f'Processing table {pg_table_fqn}')

    if not os.path.exists(PGTOBQ_OUTPUT_DIR):
        os.makedirs(PGTOBQ_OUTPUT_DIR)
        logger.info(f'Create output dir: {PGTOBQ_OUTPUT_DIR}')

    dsn = psycopg2.extensions.make_dsn(host=REPL_DB_HOST, port=REPL_DB_PORT, user=REPL_DB_USER, password=REPL_DB_PASS, database=REPL_DB_NAME, application_name=APPLICATION_NAME)
    conn = psycopg2.connect(dsn)
    cursor = conn.cursor()

    # Get source table schema
    cursor.execute(
        '''
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s
            AND table_name = %s
        ORDER BY ordinal_position
        ''',
        (pg_table_schema, pg_table_name)
    )
    pg_cols = cursor.fetchall()
    if not pg_cols:
        logger.error(f'PG table {pg_table_fqn} not found')
        return
    pg_cols = {column_name: data_type for column_name, data_type in pg_cols}

    # Translate into BQ schema
    bq_cols = [bigquery.SchemaField(name=column_name, field_type=MAP__PG_DTYPE__BQ_DTYPE.get(data_type, 'STRING')) for column_name, data_type in pg_cols.items()]

    bq_client = bigquery.Client.from_service_account_json(SA_FILENAME)

    # Create or rename table
    try:
        bq_table = bq_client.get_table(bq_table_fqn)

        # Rename table
        bq_table_new_id = f'__{now_ts}__{pg_table_schema}__{pg_table_name}'
        bq_table_new_fqn = f'{BQ_PROJECT_ID}.{bq_dataset_id}.{bq_table_new_id}'
        bq_table_new = bigquery.Table(bq_table_new_fqn, schema=bq_cols)
        bq_table_new = bq_client.create_table(bq_table_new)
        bq_client.delete_table(bq_table)
        logger.info(f'Renameed table {bq_table_fqn} to {bq_table_new_fqn}')

    except NotFound:
        # Create table
        bq_table = bigquery.Table(bq_table_fqn, schema=bq_cols)
        bq_table = bq_client.create_table(bq_table)
        logger.info(f'Created BQ table {bq_table_fqn}')

    # Download PG table
    output_filename = os.path.join(PGTOBQ_OUTPUT_DIR, f'{pg_table_fqn}.csv')
    [os.remove(filename) for filename in glob.glob(f'{output_filename}*')]  # Clean up existing files
    logger.debug(f'Downloading PG table {pg_table_fqn}...')
    download_pg_table(cursor, pg_table_fqn, pg_cols.keys(), output_filename)
    logger.info(f'Downloaded PG table {pg_table_fqn} to {output_filename}')

    # Stream compress & upload to GCS
    storage_client = storage.Client.from_service_account_json(SA_FILENAME)
    bucket = storage_client.bucket(GCS_BUCKET)
    gcs_dirname = f'{pg_table_fqn}/{now_ts}'
    q = Queue(maxsize=PGTOBQ_UPLOAD_QUEUE_MAX_SIZE)  # Prevent high disk usage
    stream_done_event = threading.Event()
    thread = threading.Thread(target=thread_upload, args=(q, stream_done_event, bucket, gcs_dirname), daemon=True)
    thread.start()
    with open(output_filename, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)

        # Compress CSV while limiting file size
        compress_csv(q, output_filename, reader, header)
        stream_done_event.set()

    os.remove(output_filename)

    # Wait all files to be uploaded
    thread.join()

    # Load to BQ
    logger.debug(f'Loading BQ {bq_table.full_table_id}...')
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.allow_quoted_newlines = True
    job_config.schema = bq_table.schema
    load_job = bq_client.load_table_from_uri(f'gs://{GCS_BUCKET}/{GCS_BASE_PATH_PGTOBQ}/{gcs_dirname}/*.gz', bq_table, job_config=job_config)
    load_job.result()
    logger.info(f'Loaded to BQ {bq_table.full_table_id}')

    bq_client.close()
    storage_client.close()


if __name__ == '__main__':
    main()
