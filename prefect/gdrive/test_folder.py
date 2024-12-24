import dotenv
import io
import os

from datetime import datetime, timezone
from google.api_core.exceptions import NotFound
from googleapiclient import discovery
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from prefect import flow, task
from prefect.logging import get_run_logger

dotenv.load_dotenv()

SA_FILENAME = os.environ['SA_FILENAME']

OUTPUT_DIR = os.environ['OUTPUT_DIR']

GCS_BUCKET = os.environ['GCS_BUCKET']
GCS_BASE_PATH_DATALAKE = os.environ['GCS_BASE_PATH_DATALAKE']
GCS_BASE_PATH_GDRIVE = os.environ['GCS_BASE_PATH_GDRIVE']

BQ_PROJECT_ID = os.environ['BQ_PROJECT_ID']
BQ_DATASET_ID = os.environ['BQ_DATASET_ID']

SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive.readonly']
GDRIVE_ID = '1Ku4kKpJ1WHkUmrUrTIRRe885uLLvc0JO'
BQ_TABLE_ID_PREFIX = 'gdrive__'
BQ_TABLE_ID = 'test_folder'
BQ_TABLE_COLS = {
    'id': 'INTEGER',
    'firstname': 'STRING',
    'lastname': 'STRING',
    'email': 'STRING',
    'email2': 'STRING',
    'profession': 'STRING',
    'Coun Try': 'STRING',
    'floater': 'FLOAT64',
    'when': 'DATE',
    'GUILD': 'STRING',
}

TODAY = datetime.now(timezone.utc).strftime('%Y-%m-%d')


@task
def fetch(gdrive_folder_id: str) -> str:
    logger = get_run_logger()

    filedir = os.path.join(OUTPUT_DIR, f'{gdrive_folder_id}_{TODAY}')
    if not os.path.exists(filedir):
        os.makedirs(filedir)
        logger.debug(f'Created directory {filedir}')

    creds = service_account.Credentials.from_service_account_file(SA_FILENAME, scopes=SCOPES)
    service = discovery.build('drive', 'v3', credentials=creds)
    result = service.files().list(q=f"'{gdrive_folder_id}' in parents and trashed=false", fields='files(id)').execute()
    gdrive_files = result.get('files', [])

    if not gdrive_files:
        raise ValueError(f'No files found in {gdrive_folder_id}')

    # Save to local
    filenames = []
    for gdrive_file in gdrive_files:
        gdrive_file_id = gdrive_file['id']
        filename = os.path.join(filedir, f'{gdrive_file_id}.csv')
        request = service.files().get_media(fileId=gdrive_file_id)
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            logger.debug(f'Downloading {gdrive_file_id} {int(status.progress() * 100)}%')
        with open(filename, 'wb') as f:
            f.write(file.getvalue())
        logger.info(f'Downloaded file {gdrive_file_id} into {filename}')
        filenames.append(filename)

    return filenames


@task
def upload_to_gcs(filenames: list[str]) -> str:
    logger = get_run_logger()

    gcs_filenames = []
    storage_client = storage.Client.from_service_account_json(SA_FILENAME)
    storage_bucket = storage_client.bucket(GCS_BUCKET)
    for filename in filenames:
        gcs_filename = f'{GCS_BASE_PATH_DATALAKE}/{GCS_BASE_PATH_GDRIVE}/{TODAY}/{os.path.basename(filename)}'
        storage_object = storage_bucket.blob(gcs_filename)

        storage_object.upload_from_filename(filename)
        storage_client.close()

        gcs_filenames.append(gcs_filename)
        logger.info(f'Uploaded {filename} to {gcs_filename}')
    return gcs_filenames


@task
def load_to_bq(gcs_filenames: list[str]):
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
    bq_table = bq_client.create_table(table)
    logger.info(f'Table created: {bq_table_fqn}')

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.schema = schema

    for gcs_filename in gcs_filenames:
        load_job = bq_client.load_table_from_uri(f'gs://{GCS_BUCKET}/{gcs_filename}', bq_table, job_config=job_config)
        load_job.result()
        bq_client.close()
        logger.info(f'Loaded {gcs_filename} to {bq_table_fqn}')


@task
def cleanup(filenames: list[str]):
    logger = get_run_logger()

    for filename in filenames:
        os.remove(filename)
        logger.info(f'Removed {filename}')


@flow
def gdrive__test_file():
    filenames = fetch(GDRIVE_ID)

    gcs_filenames = upload_to_gcs(filenames)

    load_to_bq(gcs_filenames)

    cleanup(filenames)


if __name__ == '__main__':
    gdrive__test_file()
