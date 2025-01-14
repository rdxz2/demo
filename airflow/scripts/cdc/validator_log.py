import time
import logging

from datetime import datetime, timezone
from textwrap import dedent
from typing import Any
from utill.my_string import generate_random_string
from airflow.providers.postgres.hooks.postgres import PostgresHook

from config.settings import SA_FILENAME, BQ_PROJECT_ID, CDC__STREAM_FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S, CDC__STREAM_FILEWRITER_NO_MESSAGE_WAIT_TIME_S, CDC__BQ_LOG_DATASET_PREFIX, CDC__VALIDATION_INITIAL_ROUND_PRECISION
from plugins.constants.conn import CONN_ID_PG_CDC
from plugins.hooks.bigquery import BigQueryHook

RANDOM_STRING = generate_random_string()

logger = logging.getLogger(__name__)


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


def validate_log(conn_id: str):
    logger.info(f'Validating {conn_id}...')

    bigquery_hook = BigQueryHook(SA_FILENAME)

    postgres_hook_meta = PostgresHook(CONN_ID_PG_CDC)
    meta_conn = postgres_hook_meta.get_conn()
    meta_cursor = meta_conn.cursor()

    postgres_hook_db = PostgresHook(conn_id)
    db_conn = postgres_hook_db.get_conn()
    db_cursor = db_conn.cursor()
    db_name = postgres_hook_db.get_connection(conn_id).schema

    # Get all tables
    meta_cursor.execute('SELECT "schema", "table", "validate_cols" FROM public.merger WHERE "is_active" AND "database" = %s ORDER BY 1, 2, 3;', (db_name, ))
    all_tables = meta_cursor.fetchall()

    if not all_tables:
        logger.warning('No table to validate')
        return

    # Fetch validation cols dtypes
    logger.debug('Fetching validation cols dtypes...')
    map__validate_cols__dtype = {}
    for schema, table, validate_cols in all_tables:
        for validate_col in validate_cols:
            db_cursor.execute('SELECT "data_type" FROM information_schema.columns WHERE "table_schema" = %s AND "table_name" = %s AND "column_name" = %s;', (schema, table, validate_col))
            map__validate_cols__dtype[f'{schema}.{table}.{validate_col}'] = db_cursor.fetchone()[0]

    # PG
    pg_results: list[tuple[str, str, dict[str, Any]]] = []
    for schema, table, validate_cols in all_tables:
        # Skip if table is empty
        db_cursor.execute(f'SELECT 1 FROM {schema}.{table} LIMIT 1;')
        if not db_cursor.fetchone():
            logger.info(f'Table {schema}.{table} empty')
            continue

        logger.info(f'Get PG {schema}.{table}...')

        # Get PG data
        validate_cols_representations = [get_number_representation_pg(validate_col, map__validate_cols__dtype[f'{schema}.{table}.{validate_col}']) for validate_col in validate_cols]
        validate_cols_query_str = ', '.join([f'AVG({repr}) AS "col"' for col, repr in zip(validate_cols, validate_cols_representations)])
        db_cursor.execute(dedent(
            f'''
                SELECT EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000000 AS ms, {validate_cols_query_str}
                FROM {schema}.{table};
            '''
        ))
        result = db_cursor.fetchone()
        db_conn.commit()
        result = {
            'ms': int(result[0]),
            **{x: float(y) if y is not None else None for x, y in zip(validate_cols, result[1:])}  # Handle None value
        }
        logger.debug(f'Result: {result}')
        pg_results.append((schema, table, result))

    # BQ
    max_interval_s = max(CDC__STREAM_FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S, CDC__STREAM_FILEWRITER_NO_MESSAGE_WAIT_TIME_S)
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
        bq_table_fqn = f'{BQ_PROJECT_ID}.{CDC__BQ_LOG_DATASET_PREFIX}{db_name}.{schema}__{table}'
        cols_str = ', '.join([f'`{col}`' for col in pg_result.keys()])
        validate_cols_representations = [get_number_representation_bq(validate_col, map__validate_cols__dtype[f'{schema}.{table}.{validate_col}']) for validate_col in pg_result.keys()]
        validate_cols_query_str = ', '.join([f'AVG({repr}) AS `{col}`' for col, repr in zip(pg_result.keys(), validate_cols_representations)])
        bq_result = {x: float(y) if y is not None else None for x, y in list(bigquery_hook.execute_query(dedent(
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
        logger.debug(f'Result: {bq_result}')

        # Compare each validate cols
        for validate_col, pg_value in pg_result.items():
            bq_value = bq_result[validate_col]

            if pg_value is None and bq_value is None:
                logger.info(f'Column {schema}.{table}.{validate_col} has no value')
                continue

            # If not matched, try to validate using lesser round precision
            is_matched = False
            for i in range(CDC__VALIDATION_INITIAL_ROUND_PRECISION, -1, -1):
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
