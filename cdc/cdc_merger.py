import dotenv
import glob
import logging
import os
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import threading
import uuid

from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from loguru import logger
from queue import Queue, Empty
from threading import Thread

dotenv.load_dotenv()

if os.environ['DEBUG'] == '1':
    logging.basicConfig(level=logging.DEBUG)

SA_FILENAME = os.environ['SA_FILENAME']

META_DB_HOST = os.environ['META_DB_HOST']
META_DB_PORT = int(os.environ['META_DB_PORT'])
META_DB_USER = os.environ['META_DB_USER']
META_DB_PASS = os.environ['META_DB_PASS']
META_DB_NAME = os.environ['META_DB_NAME']

REPL_DB_HOST = os.environ['REPL_DB_HOST']
REPL_DB_PORT = int(os.environ['REPL_DB_PORT'])
REPL_DB_USER = os.environ['REPL_DB_USER']
REPL_DB_PASS = os.environ['REPL_DB_PASS']
REPL_DB_NAME = os.environ['REPL_DB_NAME']

BQ_PROJECT_ID = os.environ['BQ_PROJECT_ID']
BQ_LOG_TABLE_PREFIX = os.environ['BQ_LOG_TABLE_PREFIX']

MERGER_THREADS = int(os.environ['MERGER_THREADS'])

MIGRATION_TABLE = 'public.migration'


class Merger:
    def __init__(self, cutoff_ts: datetime):
        self.cutoff_ts = cutoff_ts
        self.cutoff_ts_us = int(self.cutoff_ts.timestamp() * 1000000)  # Microseconds

        dsn = psycopg2.extensions.make_dsn(host=META_DB_HOST, port=META_DB_PORT, user=META_DB_USER, password=META_DB_PASS, database=META_DB_NAME, application_name=f'cdc-merger-{META_DB_NAME}-{uuid.uuid4()}')
        self.meta_conn = psycopg2.connect(dsn)
        self.meta_cursor = self.meta_conn.cursor()

        dsn = psycopg2.extensions.make_dsn(host=REPL_DB_HOST, port=REPL_DB_PORT, user=REPL_DB_USER, password=REPL_DB_PASS, database=REPL_DB_NAME, application_name=f'cdc-merger-{REPL_DB_NAME}-{uuid.uuid4()}')
        self.repl_conn = psycopg2.connect(dsn)
        self.repl_cursor = self.repl_conn.cursor()

        self.bq_client = bigquery.Client.from_service_account_json(SA_FILENAME)

        self.q_update_last_cutoff_ts = Queue()
        self.q_stop_event = threading.Event()

    def run(self):
        # <<----- START: Apply migrations

        # Create migration table if not exists
        self.meta_cursor.execute(
            f'''
            SELECT COUNT(1)
            FROM information_schema.tables
            WHERE table_schema || '.' || table_name = '{MIGRATION_TABLE}'
            '''
        )
        if not self.meta_cursor.fetchone()[0]:
            self.meta_cursor.execute(f'CREATE TABLE {MIGRATION_TABLE} (created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, id SERIAL PRIMARY KEY, name VARCHAR(50) NOT NULL);')
            self.meta_conn.commit()
            logger.info('Migration table created')

        # Apply migrations
        migrations = {os.path.basename(x) for x in glob.glob('migrations/*.sql')}
        self.meta_cursor.execute(f'SELECT name FROM {MIGRATION_TABLE};')
        executed_migrations = set([x[0] for x in self.meta_cursor.fetchall()])
        for new_migration in migrations - executed_migrations:
            with open(f'migrations/{new_migration}', 'r') as f:
                self.meta_cursor.execute(f.read())
                self.meta_cursor.execute(f'INSERT INTO {MIGRATION_TABLE} (name) VALUES (%s);', (new_migration,))
                self.meta_conn.commit()
                logger.info(f'Migration {new_migration} applied')

        # END: Apply migrations ----->>

        # Get all merger tables
        self.meta_cursor.execute('SELECT "database", "schema", "table", "partition_col", "cluster_cols", "last_cutoff_ts" FROM public.merger WHERE "database" = %s AND "is_active"', (REPL_DB_NAME, ))
        tables = self.meta_cursor.fetchall()

        # Get PK columns
        self.map_pk = {}
        for _, schema, table, _, _, _ in tables:
            self.repl_cursor.execute(
                '''
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass
                    AND i.indisprimary
                ORDER BY a.attnum
                ''',
                (f'{schema}.{table}',)
            )
            self.map_pk[f'{schema}.{table}'] = [x[0] for x in self.repl_cursor.fetchall()]

        thread_last_cutoff_ts_updator = Thread(target=self.update_last_cutoff_ts, daemon=True)
        thread_last_cutoff_ts_updator.start()

        # Execute
        thread_pool_executor = ThreadPoolExecutor(max_workers=MERGER_THREADS)
        futures: list[Future] = []
        for database, schema, table, partition_col, cluster_cols, last_cutoff_ts in tables:
            futures.append(thread_pool_executor.submit(self.merge, database, schema, table, partition_col, cluster_cols, last_cutoff_ts))
        [f.result() for f in futures]  # Wait for all threads to finish

        self.q_stop_event.set()

        thread_last_cutoff_ts_updator.join()

    def merge(
        self,
        database: str,
        schema: str,
        table: str,
        partition_col: str,
        cluster_cols: list[str],
        last_cutoff_ts: datetime
    ):
        bq_table_log_fqn = f'{BQ_PROJECT_ID}.{BQ_LOG_TABLE_PREFIX}{database}.{schema}__{table}'
        bq_table_main_fqn = f'{BQ_PROJECT_ID}.{database}.{schema}__{table}'
        logger.info(f'{bq_table_main_fqn}: merging...')

        if last_cutoff_ts >= self.cutoff_ts:
            logger.warning(f'{bq_table_main_fqn}: last merge timestamp {last_cutoff_ts} is same or later than cutoff timestamp {self.cutoff_ts}')
            return

        # <<----- START: Detect schema changes

        bq_table_log = self.bq_client.get_table(bq_table_log_fqn)
        bq_table_log_schema = [x for x in bq_table_log.schema if not x.name.startswith('__')]
        try:
            bq_table_main = self.bq_client.get_table(bq_table_main_fqn)
        except NotFound:
            bq_table_main = bigquery.Table(bq_table_main_fqn)
            bq_table_main.schema = bq_table_log_schema
            if partition_col:
                bq_table_main.time_partitioning = bigquery.TimePartitioning(field=partition_col)
                bq_table_main.partitioning_type = 'DAY'
            bq_table_main.clustering_fields = cluster_cols
            bq_table_main = self.bq_client.create_table(bq_table_main)
            logger.info(f'{bq_table_main_fqn}: table created')

        # New columns
        bq_table_log_schema = {x.name: x for x in bq_table_log_schema}
        bq_table_main_schema = {x.name: x for x in bq_table_main.schema}
        new_columns = [x for x in bq_table_log_schema.keys() if x not in bq_table_main_schema.keys()]  # Using list instead of set to ensure column order
        for new_column in new_columns:
            bq_table_main.schema.append(bq_table_log_schema[new_column])
            logger.info(f'{bq_table_main_fqn}: new columns added: {new_columns}')

        # END: Detect schema changes ----->>

        # Merge
        last_cutoff_ts_us = int(last_cutoff_ts.timestamp() * 1000000)  # Microseconds
        on_str = ' AND '.join([f'T.{x} = S.{x}' for x in self.map_pk[f'{schema}.{table}']])
        pks_str = ', '.join([f'`{x}`' for x in self.map_pk[f'{schema}.{table}']])
        cols_str = ', '.join([f'`{x.name}`' for x in bq_table_main_schema.values()])
        cols_update_str = ', '.join([f'T.`{x.name}` = S.`{x.name}`' for x in bq_table_main_schema.values()])
        self.bq_client.query_and_wait(
            f'''
            MERGE `{bq_table_main_fqn}` T
            USING (
                WITH t1 AS (  -- Get the latest truncate operation
                    SELECT COALESCE(MAX(`__tx_commit_ts`), TIMESTAMP_MILLIS(0)) AS `latest_truncate_ts`
                    FROM `{bq_table_log_fqn}`
                    WHERE `__tx_commit_ts` > TIMESTAMP_MICROS({last_cutoff_ts_us})
                        AND `__tx_commit_ts` <= TIMESTAMP_MICROS({self.cutoff_ts_us})
                        AND `__m_op` = 'T'
                )
                SELECT `__m_op`, {cols_str}
                FROM `{bq_table_log_fqn}`
                WHERE `__tx_commit_ts` > (SELECT `latest_truncate_ts` FROM t1)  -- Ignore all data before the latest truncate operation
                    AND `__tx_commit_ts` > TIMESTAMP_MICROS({last_cutoff_ts_us})
                    AND `__tx_commit_ts` <= TIMESTAMP_MICROS({self.cutoff_ts_us})
                QUALIFY ROW_NUMBER() OVER(PARTITION BY {pks_str} ORDER BY `__m_ord` DESC) = 1
                    AND `__m_op` IN ('I', 'U', 'D')
            ) S
            ON {on_str}
            WHEN NOT MATCHED AND S.`__m_op` = 'I' THEN INSERT ({cols_str}) VALUES ({cols_str})
            WHEN MATCHED AND S.`__m_op` = 'D' THEN DELETE
            WHEN MATCHED AND S.`__m_op` = 'U' THEN UPDATE SET {cols_update_str}
            '''
        )
        logger.info(f'{bq_table_main_fqn}: merged')

        # Instruct to update cutoff ts serially
        self.q_update_last_cutoff_ts.put((bq_table_main_fqn, database, schema, table))

    def update_last_cutoff_ts(self):
        """
        Updating last cutoff timestamp serially to prevent race condition from multiple threads using a single connection
        """

        while not self.q_stop_event.is_set() or self.q_update_last_cutoff_ts.qsize() > 0:
            try:
                bq_table_main_fqn, database, schema, table = self.q_update_last_cutoff_ts.get(timeout=1)
                self.meta_cursor.execute('UPDATE public.merger SET "update_ts" = CURRENT_TIMESTAMP, "last_cutoff_ts" = %s WHERE "database" = %s AND "schema" = %s AND "table" = %s;', (self.cutoff_ts, database, schema, table))
                self.meta_conn.commit()
                logger.debug(f'{bq_table_main_fqn}: last cutoff timestamp updated to {self.cutoff_ts}')
            except Empty:
                pass


if __name__ == '__main__':
    cutoff_ts = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    logger.info('Starting cdc merger...')
    merger = Merger(cutoff_ts)
    merger.run()
    logger.info('Exiting cdc merger')
