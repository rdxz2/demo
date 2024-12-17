import dotenv
import os
import csv

from apiclient import discovery
from datetime import datetime, timezone
from google.api_core.exceptions import NotFound
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from prefect import flow, task

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
GSHEET_SHEET = '2024H2'
GSHEET_RANGE = 'A4:J1000'
BQ_TABLE_ID_PREFIX = 'gsheet__'
BQ_TABLE_ID = 'split_bill'
BQ_TABLE_COLS = {
    'day': 'STRING',
    'date': 'DATE',
    'what': 'STRING',
    'price': 'FLOAT',
    'paid_by': 'STRING',
    'os_f': 'FLOAT',
    'os_r': 'FLOAT',
    'half_half': 'BOOLEAN',
    'description1': 'STRING',
    'description2': 'STRING',
}

TODAY = datetime.now(timezone.utc).strftime('%Y-%m-%d')


@task(log_prints=True)
def fetch(sheet_id: str) -> str:
    filename = os.path.join(OUTPUT_DIR, f'{sheet_id}_{TODAY}.csv')
    filedir = os.path.dirname(filename)
    if not os.path.exists(filedir):
        os.makedirs(filedir)

    creds = service_account.Credentials.from_service_account_file(os.path.join(os.path.pardir, SA_FILENAME), scopes=GSHEET_SCOPES)
    service = discovery.build('sheets', 'v4', credentials=creds)
    rows = service.spreadsheets().values().get(spreadsheetId=GSHEET_ID, range=f'{GSHEET_SHEET}!{GSHEET_RANGE}').execute()['values']
    rows = [row for row in rows if row[0]]

    cols_length = len(BQ_TABLE_COLS)
    for i in range(len(rows)):
        # Validation
        if not rows[i][0]:
            continue

        # Day
        rows[i][0] = rows[i][0].strip()

        # Price
        rows[i][3] = float(rows[i][3].replace(',', '').removeprefix('IDR '))

        # OS F
        rows[i][5] = float(rows[i][5].replace(',', '').removeprefix('IDR ') or 0)

        # OS R
        rows[i][6] = float(rows[i][6].replace(',', '').removeprefix('IDR ') or 0)

        # Add missing columns
        for _ in range(cols_length - len(rows[i])):
            rows[i].append(None)

    # Write into CSV
    with open(filename, 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(BQ_TABLE_COLS.keys())
        csv_writer.writerows(rows)
    print(f'Fetched {len(rows)} rows from {GSHEET_ID}:{GSHEET_SHEET}!{GSHEET_RANGE} to {filename}')

    return filename


@task(log_prints=True)
def upload_to_gcs(filename: str) -> str:
    gcs_filename = f'{GCS_BASE_PATH_DATALAKE}/{GCS_BASE_PATH_GSHEET}/{TODAY}/{os.path.basename(filename)}'

    storage_client = storage.Client.from_service_account_json(os.path.join(os.path.pardir, SA_FILENAME))
    storage_bucket = storage_client.bucket(GCS_BUCKET)
    storage_object = storage_bucket.blob(gcs_filename)

    storage_object.upload_from_filename(filename)
    storage_client.close()
    print(f'Uploaded {filename} to {gcs_filename}')
    return gcs_filename


@task(log_prints=True)
def load_to_bq(gcs_filename: str):
    bq_client = bigquery.Client.from_service_account_json(os.path.join(os.path.pardir, SA_FILENAME))

    bq_table_fqn = f'{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BQ_TABLE_ID_PREFIX}{BQ_TABLE_ID}'

    # Drop & create table
    try:
        bq_table = bq_client.get_table(bq_table_fqn)
        bq_client.delete_table(bq_table)
        print(f'Table dropped: {bq_table_fqn}')
    except NotFound:
        pass
    schema = [
        bigquery.SchemaField(k, v) for k, v in BQ_TABLE_COLS.items()
    ]
    table = bigquery.Table(bq_table_fqn, schema=schema)
    bq_table = bq_client.create_table(table)
    print(f'Table created: {bq_table_fqn}')

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.schema = schema

    load_job = bq_client.load_table_from_uri(f'gs://{GCS_BUCKET}/{gcs_filename}', bq_table, job_config=job_config)
    load_job.result()
    bq_client.close()
    print(f'Loaded {gcs_filename} to {bq_table_fqn}')


@task(log_prints=True)
def cleanup(filename: str):
    os.remove(filename)
    print(f'Removed {filename}')


@flow
def gsheet__split_bill():
    filename = fetch(GSHEET_ID)

    gcs_filename = upload_to_gcs(filename)

    load_to_bq(gcs_filename)

    cleanup(filename)


if __name__ == '__main__':
    gsheet__split_bill()
