import glob
import json
import logging
import os
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import sys
import time
import traceback

from concurrent.futures import ThreadPoolExecutor
from data import PgColumn, PgTable, BqColumn, BqTable
from datetime import datetime, timezone
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from loguru import logger
from textwrap import dedent
from typing import Any

from common import send_message, generate_random_string
from config import settings

# Configure logging
if settings.DEBUG == 1:
    logging.basicConfig(level=logging.DEBUG)
logger.add(os.path.join(settings.LOG_DIR, f'{os.path.basename(__file__)}.log'), rotation='00:00', retention='7 days', level='INFO')


class Merger:
    def __init__(self, host: str, port: str, user: str, password: str, database: str, application_name: str = f'cdc-merger-{settings.STREAMER_DB_NAME}-{generate_random_string(alphanum=True)}', **kwargs) -> None:
        self.dsn = psycopg2.extensions.make_dsn(host=host, port=port, user=user, password=password, database=database, application_name=application_name, **kwargs)

        # PostgreSQL connection
        self.conn = psycopg2.connect(self.dsn, connection_factory=psycopg2.extras.LogicalReplicationConnection)
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.ReplicationCursor)
        logger.debug(f'Connected to db: {settings.STREAMER_DB_NAME}')

        self.bq_client = bigquery.Client.from_service_account_json(settings.MERGER_SA_FILENAME)
        self.dataset_id_log = f'{settings.UPLOADER_BQ_LOG_DATASET_PREFIX}{settings.STREAMER_DB_NAME}'

        # Create migration table if not exists
        self.cursor.execute('SELECT COUNT(1) FROM information_schema.tables WHERE table_schema || \'.\' || table_name = %s', ('cdc.migration', ))
        if not self.cursor.fetchone()[0]:
            self.cursor.execute(dedent(
                '''
                CREATE TABLE cdc.migration (
                    file_name VARCHAR(15) PRIMARY KEY NOT NULL,
                    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                '''
            ).strip())
            self.conn.commit()
            logger.info('Create migration table')

        # Apply metadata migrations
        logger.debug('Checking for migrations...')
        self.cursor.execute('SELECT file_name FROM cdc.migration;')
        applied_migrations = {file_name for file_name, in self.cursor.fetchall()}
        migrations = {os.path.basename(filename) for filename in glob.glob(os.path.join(os.path.dirname(__file__), 'migrations', '*.sql'))}
        for unapplied_migration in migrations - applied_migrations:
            self.cursor.execute(open(os.path.join(os.path.dirname(__file__), 'migrations', unapplied_migration), 'r').read())
            self.cursor.execute('INSERT INTO cdc.migration (file_name) VALUES (%s);', (unapplied_migration, ))
            self.conn.commit()
            logger.info(f'Apply migration: {unapplied_migration}')

    def merge(self, pg_table_fqn: str):
        # Get metadata
        self.cursor.execute('SELECT is_active, bq_cluster_cols, latest_cutoff_ts FROM cdc.merge_table WHERE pg_table_fqn = %s', (pg_table_fqn, ))
        row = self.cursor.fetchone()
        if row is None:
            logger.warning(f'{pg_table_fqn}: not in cdc.merge_table, skipping')
            return
        is_active, bq_cluster_cols, latest_cutoff_ts = row
        if not is_active:
            logger.warning(f'{pg_table_fqn}: not active, skipping')
            return
        
        pg_table_db, pg_table_schema, pg_table_name = pg_table_fqn.split('.')

        # Create BQ table if not exists
        bq_table_fqn_log = f'{settings.UPLOADER_BQ_PROJECT_ID}.{self.dataset_id_log}.{pg_table_schema}__{pg_table_name}'
        bq_table_fqn_main = f'{settings.UPLOADER_BQ_PROJECT_ID}.{settings.STREAMER_DB_NAME}.{pg_table_schema}__{pg_table_name}'

        pass


if __name__ == '__main__':
    thread_pool_executor = ThreadPoolExecutor(max_workers=settings.UPLOADER_THREADS)
    logger.info('Starting cdc merger...')
    merger = Merger(host=settings.STREAMER_DB_HOST, port=settings.STREAMER_DB_PORT, user=settings.STREAMER_DB_USER, password=settings.STREAMER_DB_PASS, database=settings.STREAMER_DB_NAME)
    latest_file_ts = datetime.now(timezone.utc)
    latest_no_file_print_ts = datetime.now(timezone.utc)
    logger.info(f'Listening to folder: {settings.MERGER_OUTPUT_DIR}')
    while True:
        if filenames := glob.glob(f'{settings.MERGER_OUTPUT_DIR}/*.txt'):

            # Process each group in a separate thread
            futures = [(pg_table_fqn, filenames, thread_pool_executor.submit(merger.merge, filename.removesuffix('.txt'))) for filename in filenames]
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
