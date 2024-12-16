import dotenv
import json
import os
import psycopg2
import psycopg2.extensions
import psycopg2.extras
import random
import string
import threading
import time
import traceback
import uuid

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from loguru import logger

dotenv.load_dotenv()

stop_event = threading.Event()

REPL_DB_HOST = os.environ['REPL_DB_HOST']
REPL_DB_PORT = int(os.environ['REPL_DB_PORT'])
REPL_DB_USER = 'postgres'
REPL_DB_PASS = '12321'
REPL_DB_NAME = os.environ['REPL_DB_NAME']


def connect():
    dsn = psycopg2.extensions.make_dsn(host=REPL_DB_HOST, port=REPL_DB_PORT, user=REPL_DB_USER, password=REPL_DB_PASS, database=REPL_DB_NAME, application_name=f'producer-{REPL_DB_NAME}-{uuid.uuid4()}')
    conn = psycopg2.connect(dsn)
    cursor = conn.cursor()
    return conn, cursor


def randomize_sleep_time(): return random.randint(2, 10)


def generate_random_string(length: int = 16, is_alphanum: bool = True):
    letters = string.ascii_letters
    if not is_alphanum:
        letters += r'1234567890!@#$%^&*()-=_+[]{};\':",./<>?'
    return ''.join(random.choice(letters) for i in range(length))


def all_dtype():
    """
    Generate tables with all data types
    """

    conn, cursor = connect()
    logger.info(f'{all_dtype.__name__} started')

    try:
        # Create table
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS all_dtype (
                id SERIAL PRIMARY KEY,
                t_smallint SMALLINT,
                t_int INTEGER,
                t_bigint BIGINT,
                t_varchar VARCHAR(100),
                t_text TEXT,
                t_json JSON,
                t_double DOUBLE PRECISION,
                t_bool BOOLEAN,
                t_ts TIMESTAMPTZ,
                t_dt TIMESTAMP,
                t_date DATE,
                t_time TIME,
                t_byte BYTEA
            );
            ALTER TABLE all_dtype REPLICA IDENTITY FULL;
            '''
        )
        conn.commit()

        while not stop_event.is_set():
            cursor.execute(
                f'''
                INSERT INTO all_dtype (t_smallint, t_int, t_bigint, t_varchar, t_text, t_json, t_double, t_bool, t_ts, t_dt, t_date, t_time, t_byte) VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s),
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s),
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                ''',
                (
                    # Row 1
                    random.randint(-32768, 32767),
                    random.randint(-2147483648, 2147483647),
                    random.randint(-9223372036854775808, 9223372036854775807),
                    'asd 🚫 bcd 😕🤓😿🐼🐖   xxxxxx\n🦕🐌🏟️🏡',
                    generate_random_string(1000),
                    json.dumps({'key': 'value'}),
                    random.random(),
                    random.choice([True, False, None]),
                    datetime.now(timezone.utc),
                    datetime.now(),
                    datetime.now().date(),
                    datetime.now().time(),
                    bytes(random.randint(0, 255)),
                    # Row 2
                    random.randint(-32768, 32767),
                    random.randint(-2147483648, 2147483647),
                    random.randint(-9223372036854775808, 9223372036854775807),
                    generate_random_string(100),
                    generate_random_string(1000),
                    json.dumps({'key': 'value'}),
                    random.random(),
                    random.choice([True, False, None]),
                    datetime.now(timezone.utc),
                    datetime.now(),
                    datetime.now().date(),
                    datetime.now().time(),
                    bytes(random.randint(0, 255)),
                    # Row 3
                    random.randint(-32768, 32767),
                    random.randint(-2147483648, 2147483647),
                    random.randint(-9223372036854775808, 9223372036854775807),
                    generate_random_string(100),
                    generate_random_string(1000),
                    json.dumps({
                        generate_random_string(10, 50): generate_random_string(100, 500)
                        for _ in range(random.randint(1, 100))
                    }),
                    random.random(),
                    random.choice([True, False, None]),
                    datetime.now(timezone.utc),
                    datetime.now(),
                    datetime.now().date(),
                    datetime.now().time(),
                    bytes(random.randint(0, 255))
                ),
            )
            conn.commit()
            logger.debug(f'{all_dtype.__name__} inserted')
            time.sleep(randomize_sleep_time())
        logger.warning(f'{all_dtype.__name__} gracefully stopping')
    except Exception as e:
        stop_event.set()
        logger.error(f'{all_dtype.__name__} stopping for error --> {e}\t{traceback.format_exc()}')
        raise


def gen100():
    """
    Generate 100 tables and insert data into them
    """

    conn, cursor = connect()
    logger.info(f'{gen100.__name__} started')

    try:
        for i in range(100):
            cursor.execute(
                f'''
                CREATE TABLE IF NOT EXISTS gen_{i} (
                    id SERIAL PRIMARY KEY,
                    value1 INTEGER,
                    value2 VARCHAR(10),
                    value3 TIMESTAMPTZ
                );
                ALTER TABLE gen_{i} REPLICA IDENTITY FULL;
                ''',
            )
            conn.commit()

        while not stop_event.is_set():
            ranges = [x for x in range(100)]
            random.shuffle(ranges)
            for i in ranges:
                cursor.execute(
                    f'''INSERT INTO gen_{i} (value1, value2, value3) VALUES (%s, %s, %s);''',
                    (
                        random.randint(1, 10000),
                        generate_random_string(10),
                        datetime.now(timezone.utc),
                    ),
                )
                conn.commit()
            logger.debug(f'{gen100.__name__} inserted')
            time.sleep(randomize_sleep_time())
        logger.warning(f'{gen100.__name__} gracefully stopping')
    except Exception as e:
        stop_event.set()
        logger.error(f'{gen100.__name__} stopping for error --> {e}\t{traceback.format_exc()}')
        raise


def truncate():
    """
    Generate table, insert values, and truncate
    """

    conn, cursor = connect()
    logger.info(f'{truncate.__name__} started')

    try:
        # Create table
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS ttruncate (
                id SERIAL PRIMARY KEY,
                value1 INTEGER
            );
            ALTER TABLE ttruncate REPLICA IDENTITY FULL;
            ''',
        )
        conn.commit()

        while not stop_event.is_set():
            cursor.execute(
                f'''INSERT INTO ttruncate (value1) VALUES (%s), (%s), (%s), (%s), (%s);''',
                (
                    random.randint(1, 1000),
                    random.randint(1, 1000),
                    random.randint(1, 1000),
                    random.randint(1, 1000),
                    random.randint(1, 1000),
                ),
            )
            conn.commit()
            logger.debug(f'{truncate.__name__} inserted')
            time.sleep(randomize_sleep_time())

            cursor.execute(
                f'''TRUNCATE TABLE ttruncate;''',
            )
            conn.commit()
            logger.debug(f'{truncate.__name__} truncated')
            time.sleep(randomize_sleep_time())
        logger.warning(f'{truncate.__name__} gracefully stopping')
    except Exception as e:
        stop_event.set()
        logger.error(f'{truncate.__name__} stopping for error --> {e}\t{traceback.format_exc()}')
        raise


def delete():
    """
    Generate table, insert values, and delete
    """

    conn, cursor = connect()
    logger.info(f'{delete.__name__} started')

    try:
        # Create table
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS tdelete (
                id SERIAL PRIMARY KEY,
                value1 INTEGER
            );
            ALTER TABLE tdelete REPLICA IDENTITY FULL;
            ''',
        )

        while not stop_event.is_set():
            # Insert
            cursor.execute(
                f'''INSERT INTO tdelete (value1) VALUES (%s);''',
                (
                    random.randint(1, 1000),
                )
            )
            conn.commit()
            logger.debug(f'{delete.__name__} inserted')
            time.sleep(randomize_sleep_time())

            # Delete
            cursor.execute(
                f'''DELETE FROM tdelete;''',
            )
            conn.commit()
            logger.debug(f'{delete.__name__} deleted')
            time.sleep(randomize_sleep_time())
        logger.warning(f'{delete.__name__} gracefully stopping')
    except Exception as e:
        stop_event.set()
        logger.error(f'{delete.__name__} stopping for error --> {e}\t{traceback.format_exc()}')
        raise


def update():
    """
    Generate table, insert values, and update
    """

    conn, cursor = connect()
    logger.info(f'{update.__name__} started')

    try:
        # Create table
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS tupdate (
                id SERIAL PRIMARY KEY,
                value1 INTEGER
            );
            ALTER TABLE tupdate REPLICA IDENTITY FULL;
            ''',
        )

        while not stop_event.is_set():
            # Insert
            cursor.execute(
                f'''INSERT INTO tupdate (value1) VALUES (%s);''',
                (
                    random.randint(1, 1000),
                ),
            )
            conn.commit()
            logger.debug(f'{update.__name__} inserted')
            time.sleep(randomize_sleep_time())

            # Update
            cursor.execute(
                f'''UPDATE tupdate SET value1 = %s WHERE id > (SELECT MAX(id) - 10 FROM tupdate);''',
                (
                    random.randint(1, 1000),
                ),
            )
            conn.commit()
            logger.debug(f'{update.__name__} updated')
            time.sleep(randomize_sleep_time())
        logger.warning(f'{update.__name__} gracefully stopping')
    except Exception as e:
        stop_event.set()
        logger.error(f'{update.__name__} stopping for error --> {e}\t{traceback.format_exc()}')
        raise


def batch():
    """
    Generate large batch of rows
    """

    conn, cursor = connect()
    logger.info(f'{batch.__name__} started')

    try:
        # Create table
        cursor.execute(
            f'''
            CREATE TABLE IF NOT EXISTS batch (
                id SERIAL PRIMARY KEY,
                value1 INTEGER,
                value2 VARCHAR(1000),
                value3 VARCHAR(1000),
                value4 VARCHAR(1000),
                value5 VARCHAR(1000),
                value6 VARCHAR(1000)
            );
            ALTER TABLE batch REPLICA IDENTITY FULL;
            ''',
        )
        conn.commit()

        while not stop_event.is_set():
            time.sleep(3600)
            # Insert
            with open('./batch.csv', 'w') as f:
                f.write('value1,value2,value3,value4,value5,value6\n')
                for x in range(1_000_000):
                    f.write(f"{random.randint(-2147483648, 2147483647)},{generate_random_string(100)},{generate_random_string(100)},{generate_random_string(100)},{generate_random_string(100)},{generate_random_string(100)}\n")
            cursor.copy_expert(f"COPY batch (value1,value2,value3,value4,value5,value6) FROM STDIN WITH CSV HEADER", open('./batch.csv', 'r'))
            conn.commit()
            os.remove('./batch.csv')
            logger.debug(f'{batch.__name__} copied')
        logger.warning(f'{batch.__name__} gracefully stopping')
    except Exception as e:
        stop_event.set()
        logger.error(f'{batch.__name__} stopping for error --> {e}\t{traceback.format_exc()}')
        raise


if __name__ == '__main__':
    logger.info('Start producing...')
    functions = [
        all_dtype,
        gen100,
        truncate,
        delete,
        update,
        batch,
    ]

    # Execute
    with ThreadPoolExecutor(max_workers=len(functions)) as executor:
        fs = [executor.submit(f) for f in functions]
        [f.result() for f in fs]

    logger.info('Stop producing')
