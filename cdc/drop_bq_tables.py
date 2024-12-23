import dotenv
import os

from google.cloud import bigquery
from loguru import logger

dotenv.load_dotenv()

SA_FILENAME = os.environ['SA_FILENAME']

CDC_DB_NAME = os.environ['CDC_DB_NAME']

BQ_LOG_DATASET_PREFIX = os.environ['BQ_LOG_DATASET_PREFIX']

client = bigquery.Client.from_service_account_json(SA_FILENAME)
all_tables = client.list_tables(f'{BQ_LOG_DATASET_PREFIX}{CDC_DB_NAME}')
for table in all_tables:
    logger.info(f'Dropping {table.table_id}')
    client.delete_table(table)
