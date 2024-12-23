import dotenv
import os
import psycopg
import time

from datetime import datetime, timezone
from google.cloud import bigquery
from prefect import flow, task
from prefect.logging import get_run_logger
from textwrap import dedent
from typing import Any
from utill.my_string import generate_random_string, replace_nonnumeric

dotenv.load_dotenv()

SA_FILENAME = os.environ['SA_FILENAME']

META_DB_HOST = os.environ['META_DB_HOST']
META_DB_PORT = int(os.environ['META_DB_PORT'])
META_DB_USER = os.environ['META_DB_USER']
META_DB_PASS = os.environ['META_DB_PASS']
META_DB_NAME = os.environ['META_DB_NAME']

STREAM_FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S = int(os.environ['STREAM_FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S'])
STREAM_FILEWRITER_NO_MESSAGE_WAIT_TIME_S = int(os.environ['STREAM_FILEWRITER_NO_MESSAGE_WAIT_TIME_S'])

BQ_PROJECT_ID = os.environ['BQ_PROJECT_ID']
BQ_LOG_DATASET_PREFIX = os.environ['BQ_LOG_DATASET_PREFIX']

VALIDATION_INITIAL_ROUND_PRECISION = int(os.environ['VALIDATION_INITIAL_ROUND_PRECISION'])

RANDOM_STRING = generate_random_string()


def get_meta_connection():
    conn = psycopg.connect(f'postgresql://{META_DB_USER}:{META_DB_PASS}@{META_DB_HOST}:{META_DB_PORT}/{META_DB_NAME}?application_name=cdc-validator-log-{META_DB_NAME}-{RANDOM_STRING}')
    cursor = conn.cursor()
    return conn, cursor


def get_number_representation_pg(col: str, dtype: str) -> str:
    match dtype:
        case ('bigint' | 'integer' | 'smallint' | 'numeric' | 'double precision'):
            return f'"{col}"::DECIMAL'
        case ('date' | 'timestamp with time zone' | 'timestamp without time zone'):
            return f'FLOOR(EXTRACT(EPOCH FROM "{col}"))'
        case ('time with time zone' | 'time without time zone'):
            return f'EXTRACT(\'hour\' FROM "{col}") + EXTRACT(\'minute\' FROM "{col}") + EXTRACT(\'second\' FROM "{col}")'
        case ('boolean' | 'bool'):
            return f'"{col}"::INTEGER'
        case ('json' | 'jsonb'):
            return f'LENGTH("{col}"::VARCHAR)'
        case _:
            return f'LENGTH("{col}")'


def get_number_representation_bq(col: str, dtype: str) -> str:
    match dtype:
        case ('bigint' | 'integer' | 'smallint' | 'numeric' | 'double precision'):
            return f'CAST(`{col}` AS DECIMAL)'
        case ('date' | 'timestamp with time zone' | 'timestamp without time zone'):
            return f'UNIX_SECONDS(TIMESTAMP(`{col}`))'
        case ('time with time zone' | 'time without time zone'):
            return f'EXTRACT(HOUR FROM `{col}`) + EXTRACT(MINUTE FROM `{col}`) + EXTRACT(SECOND FROM `{col}`)'
        case ('boolean' | 'bool'):
            return f'CAST(`{col}` AS INTEGER)'
        case ('json' | 'jsonb'):
            return f'LENGTH(TO_JSON_STRING(`{col}`))'
        case _:
            return f'LENGTH(`{col}`)'


@task
def validate_log(db: str):
    logger = get_run_logger()
    logger.info(f'Validating {db}...')

    client = bigquery.Client.from_service_account_json(SA_FILENAME)

    meta_conn, meta_cursor = get_meta_connection()

    db_env = replace_nonnumeric(db.upper(), '_')
    db_user, db_pass, db_host, db_port, db_name = (
        os.environ[f'__{db_env}_DB_USER'],
        os.environ[f'__{db_env}_DB_PASS'],
        os.environ[f'__{db_env}_DB_HOST'],
        int(os.environ[f'__{db_env}_DB_PORT']),
        os.environ[f'__{db_env}_DB_NAME'],
    )
    db_conn = psycopg.connect(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}?application_name=cdc-validator-log-{db}-{RANDOM_STRING}')
    db_cursor = db_conn.cursor()

    # Get all tables
    all_tables = meta_cursor.execute('SELECT "schema", "table", "validate_cols" FROM public.merger WHERE "is_active" AND "database" = %s ORDER BY 1, 2, 3;', (db, )).fetchall()

    # Fetch validation cols dtypes
    logger.debug('Fetching validation cols dtypes...')
    map__validate_cols__dtype = {}
    for schema, table, validate_cols in all_tables:
        for validate_col in validate_cols:
            map__validate_cols__dtype[f'{schema}.{table}.{validate_col}'] = \
                db_cursor.execute('SELECT "data_type" FROM information_schema.columns WHERE "table_schema" = %s AND "table_name" = %s AND "column_name" = %s;', (schema, table, validate_col)).fetchone()[0]

    # PG
    pg_results: list[tuple[str, str, dict[str, Any]]] = []
    for schema, table, validate_cols in all_tables:
        # Skip if table is empty
        if not db_cursor.execute(f'SELECT 1 FROM {schema}.{table} LIMIT 1;').fetchone():
            logger.info(f'Table {schema}.{table} empty')
            continue

        logger.info(f'Get PG {schema}.{table}...')

        # Get PG data
        validate_cols_representations = [get_number_representation_pg(validate_col, map__validate_cols__dtype[f'{schema}.{table}.{validate_col}']) for validate_col in validate_cols]
        validate_cols_query_str = ', '.join([f'AVG({repr}) AS "col"' for col, repr in zip(validate_cols, validate_cols_representations)])
        result = db_cursor.execute(
            f'''
            SELECT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000000 AS ms, {validate_cols_query_str}
            FROM {schema}.{table};
            '''
        ).fetchone()
        db_conn.commit()
        result = {
            'ms': int(result[0]),
            **{x: float(y) for x, y in zip(validate_cols, result[1:])}
        }
        print(result['ms'])
        pg_results.append((schema, table, result))

    # BQ
    max_interval_s = max(STREAM_FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S, STREAM_FILEWRITER_NO_MESSAGE_WAIT_TIME_S)
    mismatches = []
    for schema, table, pg_result in pg_results:
        logger.info(f'Get BQ {schema}.{table}...')

        ms: int = pg_result.pop('ms')

        # Wait until BQ data is ready (streamer & uploader)
        diff = (datetime.now(timezone.utc) - datetime.fromtimestamp(ms / 1000000, timezone.utc)).total_seconds()
        if diff < max_interval_s:
            logger.info(f'Waiting for {max_interval_s - diff} seconds...')
            time.sleep(max_interval_s - diff)

        # Get BQ data
        bq_table_fqn = f'{BQ_PROJECT_ID}.{BQ_LOG_DATASET_PREFIX}{db}.{schema}__{table}'
        cols_str = ', '.join([f'`{col}`' for col in pg_result.keys()])
        validate_cols_representations = [get_number_representation_bq(validate_col, map__validate_cols__dtype[f'{schema}.{table}.{validate_col}']) for validate_col in pg_result.keys()]
        validate_cols_query_str = ', '.join([f'AVG({repr}) AS `{col}`' for col, repr in zip(pg_result.keys(), validate_cols_representations)])
        bq_result = {x: float(y) for x, y in list(client.query_and_wait(dedent(
            f'''
                WITH t1 AS (  -- Get the latest truncate operation
                    SELECT COALESCE(MAX(`__tx_commit_ts`), TIMESTAMP_MILLIS(0)) AS `latest_truncate_ts`
                    FROM `{bq_table_fqn}`
                    WHERE `__tx_commit_ts` <= TIMESTAMP_MICROS({ms})
                        AND `__m_op` = 'T'
                )
                , t2 AS (
                    SELECT {cols_str}
                    FROM `{bq_table_fqn}`
                    WHERE `__tx_commit_ts` > (SELECT `latest_truncate_ts` FROM t1)  -- Ignore all data before the latest truncate operation
                        AND `__tx_commit_ts` <= TIMESTAMP_MICROS({ms})
                        AND `__m_op` != 'B'  -- Ignore BEFORE record
                    QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY `__tx_commit_ts` DESC, `__m_ord` DESC) = 1
                        AND `__m_op` NOT IN ('D')
                )
                SELECT {validate_cols_query_str}
                FROM t2
            '''
        )))[0].items()}

        # Compare each validate cols
        for validate_col, pg_value in pg_result.items():
            bq_value = bq_result[validate_col]

            # If not matched, try to validate using lesser round precision
            is_matched = False
            for i in range(VALIDATION_INITIAL_ROUND_PRECISION, -1, -1):
                pg_value_rounded = round(pg_value, i)
                bq_value_rounded = round(bq_value, i)
                logger.debug(f'Validate {schema}.{table}.{validate_col} -- PG: {pg_value_rounded} -- BQ: {bq_value_rounded} -- Decimal precision: {i}')
                if pg_value_rounded == bq_value_rounded:
                    is_matched = True
                    logger.info(f'Validate success for {schema}.{table}.{validate_col} -- PG: {pg_value_rounded} -- BQ: {bq_value_rounded} -- Decimal precision: {i}')
                    break
            if not is_matched:
                mismatches.append((schema, table, validate_col, ms, pg_value, bq_value))

    for schema, table, validate_col, ms, pg_value, bq_value in mismatches:
        logger.error(f'Mismatched: {schema}.{table}.{validate_col} -- PG: {pg_value} -- BQ: {bq_value} -- ms: {ms}')

    meta_cursor.close()
    meta_conn.close()

    if mismatches:
        raise ValueError('Mismatched value detected, please check the ERROR logs')


@flow
def cdc__validator_log():
    logger = get_run_logger()

    conn, cursor = get_meta_connection()
    dbs = [x[0] for x in cursor.execute('SELECT DISTINCT "database" FROM public.merger WHERE "is_active";').fetchall()]
    cursor.close()
    conn.close()

    logger.info('Starting cdc validator (log)...')
    futures = [validate_log.with_options(name=f'validate-log-{db}').submit(db) for db in dbs]
    for future in futures:
        future.result()
    logger.info('Exiting cdc validator (log)')


if __name__ == '__main__':
    cdc__validator_log()
