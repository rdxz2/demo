import dotenv
import os

from google.cloud import bigquery
from loguru import logger

dotenv.load_dotenv()

SA_FILENAME = os.environ['SA_FILENAME']

client = bigquery.Client.from_service_account_json(os.path.join(os.path.pardir, SA_FILENAME))
all_tables = client.list_tables('log__stream')
for table in all_tables:
    logger.info(f'Dropping {table.table_id}')
    client.delete_table(table)
