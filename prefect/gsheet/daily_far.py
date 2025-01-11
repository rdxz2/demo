import csv
import dotenv
import os

from apiclient import discovery
from datetime import datetime, timezone, timedelta
from google.api_core.exceptions import NotFound
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from prefect import flow, task
from prefect.logging import get_run_logger
from uuid import uuid4

dotenv.load_dotenv()

SA_FILENAME = os.environ['SA_FILENAME']

OUTPUT_DIR = os.environ['OUTPUT_DIR']

GCS_BUCKET = os.environ['GCS_BUCKET']
GCS_BASE_PATH_DATALAKE = os.environ['GCS_BASE_PATH_DATALAKE']
GCS_BASE_PATH_GSHEET = os.environ['GCS_BASE_PATH_GSHEET']

BQ_PROJECT_ID = os.environ['BQ_PROJECT_ID']
BQ_DATASET_ID = os.environ['BQ_DATASET_ID']

GSHEET_SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/spreadsheets.readonly']
GSHEET_ID = '1pTwHma-f_OSiq2jTZN4-o04EBbMLZlM0WbXGTJLuurQ'
GSHEET_SHEET = 'daily far 2025'
GSHEET_RANGE = 'A2:J1000'
GSHEET_EPOCH = datetime(1899, 12, 30, tzinfo=timezone.utc)
BQ_TABLE_ID_PREFIX = 'gsheet__'
BQ_TABLE_ID = 'daily_far'
BQ_TABLE_COLS = {
    'year': 'INTEGER',
    'month': 'INTEGER',
    'day': 'INTEGER',
    'date': 'DATE',
    'item': 'STRING',
    'category': 'STRING',
    'price': 'FLOAT64',
    'payment_method': 'STRING',
    'notes': 'STRING',
    'billed_on': 'INTEGER',
}
BQ_TABLE_PARTITION_COL = 'date'

TODAY = datetime.now(timezone.utc).strftime('%Y-%m-%d')


@task
def fetch(sheet_id: str) -> str:
    logger = get_run_logger()

    filename = os.path.join(OUTPUT_DIR, f'{sheet_id}_{TODAY}_{uuid4()}.csv')
    filedir = os.path.dirname(filename)
    if not os.path.exists(filedir):
        os.makedirs(filedir)
        logger.debug(f'Created directory {filedir}')

    creds = service_account.Credentials.from_service_account_file(SA_FILENAME, scopes=GSHEET_SCOPES)
    service = discovery.build('sheets', 'v4', credentials=creds)
    rows = service.spreadsheets().values().get(spreadsheetId=GSHEET_ID, range=f'{GSHEET_SHEET}!{GSHEET_RANGE}', valueRenderOption='UNFORMATTED_VALUE').execute()['values']

    cols_length = len(BQ_TABLE_COLS)
    for i in range(len(rows)):
        for j, bq_col in enumerate(BQ_TABLE_COLS.items()):
            # Prevent index not found
            if not cols_length > j:
                continue
            if not rows[i][j]:
                continue

            col, dtype = bq_col

            match dtype:
                case 'INTEGER':
                    rows[i][j] = int(rows[i][j])
                case 'DATE':
                    rows[i][j] = (GSHEET_EPOCH + timedelta(days=int(rows[i][j]))).strftime('%Y-%m-%d')
                case _:
                    pass

        # Add missing columns
        for _ in range(cols_length - len(rows[i])):
            rows[i].append(None)

    # Write into CSV
    with open(filename, 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(BQ_TABLE_COLS.keys())
        csv_writer.writerows(rows)
    logger.info(f'Fetched {len(rows)} rows from {GSHEET_ID}:{GSHEET_SHEET}!{GSHEET_RANGE} to {filename}')

    return filename


@task
def upload_to_gcs(filename: str) -> str:
    logger = get_run_logger()
    gcs_filename = f'{GCS_BASE_PATH_DATALAKE}/{GCS_BASE_PATH_GSHEET}/{TODAY}/{os.path.basename(filename)}'

    storage_client = storage.Client.from_service_account_json(SA_FILENAME)
    storage_bucket = storage_client.bucket(GCS_BUCKET)
    storage_object = storage_bucket.blob(gcs_filename)

    storage_object.upload_from_filename(filename)
    storage_client.close()
    logger.info(f'Uploaded {filename} to {gcs_filename}')
    return gcs_filename


@task
def load_to_bq(gcs_filename: str):
    logger = get_run_logger()

    bq_client = bigquery.Client.from_service_account_json(SA_FILENAME)

    bq_table_fqn = f'{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID_PREFIX}{BQ_TABLE_ID}'

    # Drop & create table
    try:
        bq_table = bq_client.get_table(bq_table_fqn)
        bq_client.delete_table(bq_table)
        logger.info(f'Table dropped: {bq_table_fqn}')
    except NotFound:
        pass
    schema = [
        bigquery.SchemaField(k, v) for k, v in BQ_TABLE_COLS.items()
    ]
    table = bigquery.Table(bq_table_fqn, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field=BQ_TABLE_PARTITION_COL)
    table.partitioning_type = 'DAY'
    bq_table = bq_client.create_table(table)
    logger.info(f'Table created: {bq_table_fqn}')

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.schema = schema

    load_job = bq_client.load_table_from_uri(f'gs://{GCS_BUCKET}/{gcs_filename}', bq_table, job_config=job_config)
    load_job.result()
    bq_client.close()
    logger.info(f'Loaded {gcs_filename} to {bq_table_fqn}')


@task
def cleanup(filename: str):
    logger = get_run_logger()

    os.remove(filename)
    logger.info(f'Removed {filename}')


@flow
def gsheet__daily_far():
    filename = fetch(GSHEET_ID)

    gcs_filename = upload_to_gcs(filename)

    load_to_bq(gcs_filename)

    cleanup(filename)


if __name__ == '__main__':
    gsheet__daily_far()
