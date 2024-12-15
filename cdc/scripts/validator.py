import dotenv
import os
import psycopg
import time

from datetime import datetime, timezone
from google.cloud import bigquery
from loguru import logger

dotenv.load_dotenv()

SA_FILENAME = os.environ['SA_FILENAME']

REPL_DB_HOST = os.environ['REPL_DB_HOST']
REPL_DB_PORT = os.environ['REPL_DB_PORT']
REPL_DB_USER = os.environ['REPL_DB_USER']
REPL_DB_PASS = os.environ['REPL_DB_PASS']
REPL_DB_NAME = os.environ['REPL_DB_NAME']

STREAM_FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S = int(os.environ['STREAM_FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S'])
STREAM_FILEWRITER_NO_MESSAGE_WAIT_TIME_S = int(os.environ['STREAM_FILEWRITER_NO_MESSAGE_WAIT_TIME_S'])

BQ_PROJECT_ID = os.environ['BQ_PROJECT_ID']
BQ_LOG_TABLE_PREFIX = os.environ['BQ_LOG_TABLE_PREFIX']

if __name__ == '__main__':
    client = bigquery.Client.from_service_account_json(os.path.join(os.path.pardir, SA_FILENAME))

    conn = psycopg.connect(f'postgresql://{REPL_DB_USER}:{REPL_DB_PASS}@{REPL_DB_HOST}:{REPL_DB_PORT}/{REPL_DB_NAME}')
    cursor = conn.cursor()

    # Get all tables
    all_tables = cursor.execute(
        f'''
        SELECT table_schema
            , table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        '''
    ).fetchall()

    # PG
    pg_data = []
    for table_schema, table_name in sorted(all_tables, key=lambda x: f'{x[0]}.{x[1]}'):
        logger.info(f'Get PG {table_schema}.{table_name}...')

        # Get PG data
        ms, max_id, v_pg = cursor.execute(
            f'''
                WITH t1 AS (
                    SELECT max(id) AS max_id FROM {table_schema}.{table_name}
                )
                SELECT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000000 AS ms, (SELECT max_id FROM t1) AS max_id, SUM(id::DECIMAL / (SELECT max_id FROM t1)) AS v
                FROM {table_schema}.{table_name}
            '''
        ).fetchone()
        if not v_pg:
            logger.warning(f'Table {table_schema}.{table_name} empty')
            continue
        ms = int(ms)
        v_pg = round(float(v_pg), 10)

        pg_data.append((table_schema, table_name, ms, max_id, v_pg))

    # BQ
    max_interval_s = max(STREAM_FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S, STREAM_FILEWRITER_NO_MESSAGE_WAIT_TIME_S)
    mismatches = []
    for table_schema, table_name, ms, max_id, v_pg in pg_data:

        logger.info(f'Get BQ {table_schema}.{table_name}...')

        # Wait until BQ data is ready (streamer & uploader)
        diff = (datetime.now(timezone.utc) - datetime.fromtimestamp(ms / 1000000, timezone.utc)).total_seconds()
        if diff < max_interval_s:
            logger.info(f'Waiting for {max_interval_s - diff} seconds...')
            time.sleep(max_interval_s - diff)

        # Get BQ data
        v_bq, = list(client.query_and_wait(
            f'''
                WITH t1 AS (  -- Get the latest truncate operation
                    SELECT COALESCE(MAX(__tx_commit_ts), TIMESTAMP_MILLIS(0)) AS max__tx_commit_ts
                    FROM `{BQ_PROJECT_ID}.{BQ_LOG_TABLE_PREFIX}{REPL_DB_NAME}.{table_schema}__{table_name}`
                    WHERE __tx_commit_ts <= TIMESTAMP_MICROS({ms})
                        AND __m_op = 'T'
                )
                , t2 AS (
                    SELECT id
                    FROM `{BQ_PROJECT_ID}.{BQ_LOG_TABLE_PREFIX}{REPL_DB_NAME}.{table_schema}__{table_name}`
                    WHERE __tx_commit_ts > (SELECT max__tx_commit_ts FROM t1)  -- Ignore all data before the latest truncate operation
                        AND __tx_commit_ts <= TIMESTAMP_MICROS({ms})
                    QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY __tx_commit_ts DESC) = 1
                        AND __m_op NOT IN ('D')
                )
                , t3 AS (
                    SELECT MAX(id) AS max_id FROM t2
                )
                SELECT SUM(id / (SELECT max_id FROM t3)) AS v
                FROM t2
            '''
        ))[0]
        v_bq = round(v_bq, 10)

        logger.debug(f'Compare table: {table_schema}.{table_name} -- MS: {ms} -- MAX_ID: {max_id} -- V_PG: {v_pg} -- V_BQ: {v_bq}')
        if v_pg != v_bq:
            x = 10
            is_matched = False
            while x > 0:
                x -= 1
                logger.warning(f'Validation failed for {table_schema}.{table_name}, retying with round {x} --> V_PG: {round(v_pg, x)} V_BQ: {round(v_bq, x)}')
                if round(v_pg, x) == round(v_bq, x):
                    logger.warning('Success')
                    is_matched = True
                    break

            if not is_matched:
                logger.error(f'Validation failed for {table_schema}.{table_name}')
                mismatches.append((table_schema, table_name, ms, max_id, v_pg, v_bq))

    for mismatch in mismatches:
        logger.error(f'Mismatched: {mismatch}')
