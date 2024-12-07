import json
import multiprocessing
import random
import sys
import time

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from loguru import logger

from utill.my_string import generate_random_string
from utill.my_pg_v2 import PG

logger.remove()
logger.add(sys.stdout, level='INFO')

stop = multiprocessing.Value('b', False)

PG_CONN_NAME = 'stream-local-postgres'


def randomize_sleep_time(): return random.randint(2, 10)


class AllDtype(multiprocessing.Process):
    """
    Generate tables with all data types
    """

    def __init__(self) -> None:
        super().__init__()
        self.pg = PG(PG_CONN_NAME)

    def run(self):
        logger.info(f'{AllDtype.__name__} started')

        try:
            # Create table
            self.pg.execute_query(
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
                ''',
                return_df=False
            )

            while True:
                self.pg.execute_query(
                    f'''
                    INSERT INTO all_dtype (t_smallint, t_int, t_bigint, t_varchar, t_text, t_json, t_double, t_bool, t_ts, t_dt, t_date, t_time, t_byte) VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s),
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s),
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    ''',
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
                    bytes(random.randint(0, 255)),
                    #
                    return_df=False
                )
                time.sleep(randomize_sleep_time())
        except:
            stop.value = True
            logger.error('Stopping for error')
            raise


class Gen100(multiprocessing.Process):
    """
    Generate 100 tables and insert data into them
    """

    def __init__(self) -> None:
        super().__init__()
        self.pg = PG(PG_CONN_NAME)

    def insert(self, i: int):
        # Create table
        self.pg.execute_query(
            f'''
            CREATE TABLE IF NOT EXISTS gen_{i} (
                id SERIAL PRIMARY KEY,
                value1 INTEGER,
                value2 VARCHAR(10),
                value3 TIMESTAMPTZ
            );
            ALTER TABLE gen_{i} REPLICA IDENTITY FULL;
            ''',
            return_df=False
        )

        while True:
            if stop.value:
                logger.warning(f'Exiting thread Gen100: {i}')
                break

            self.pg.execute_query(
                f'''INSERT INTO gen_{i} (value1, value2, value3) VALUES (%s, %s, %s);''',
                random.randint(1, 10000),
                generate_random_string(10),
                datetime.now(timezone.utc),
                #
                return_df=False,
            )
            time.sleep(randomize_sleep_time())

    def run(self):
        logger.info(f'{Gen100.__name__} started')

        try:
            with ThreadPoolExecutor(max_workers=100) as executor:
                futures = [executor.submit(self.insert, i) for i in range(100)]
                [f.result() for f in futures]
        except:
            stop.value = True
            logger.error('Stopping for error')
            raise


# class BigText(multiprocessing.Process):
#     """
#     Generate rows with big amount of text
#     """

#     def __init__(self) -> None:
#         super().__init__()
#         self.pg = PG(PG_CONN_NAME)

#     def run(self):
#         logger.info(f'{BigText.__name__} started')

#         try:

#             self.pg.execute_query(
#                 f'''
#                 CREATE TABLE IF NOT EXISTS big_text (
#                     id SERIAL PRIMARY KEY,
#                     value1 TEXT
#                 );
#                 ALTER TABLE big_text REPLICA IDENTITY FULL;
#                 ''',
#                 return_df=False
#             )

#             while True:
#                 self.pg.execute_query(
#                     f'''INSERT INTO big_text (value1) VALUES (%s);''',
#                     generate_random_string(random.randint(100_000, 1_000_000)),
#                     #
#                     return_df=False
#                 )
#                 time.sleep(600)
#         except:
#             stop.value = True
#             logger.error('Stopping for error')
#             raise


class Truncate(multiprocessing.Process):
    """
    Generate table, insert values, and truncate
    """

    def __init__(self) -> None:
        super().__init__()
        self.pg = PG(PG_CONN_NAME)

    def run(self):
        logger.info(f'{Truncate.__name__} started')

        try:
            # Create table
            self.pg.execute_query(
                f'''
                CREATE TABLE IF NOT EXISTS ttruncate (
                    id SERIAL PRIMARY KEY,
                    value1 INTEGER
                );
                ALTER TABLE ttruncate REPLICA IDENTITY FULL;
                ''',
                return_df=False
            )

            while True:
                self.pg.execute_query(
                    f'''INSERT INTO ttruncate (value1) VALUES (%s), (%s), (%s), (%s), (%s);''',
                    random.randint(1, 1000),
                    random.randint(1, 1000),
                    random.randint(1, 1000),
                    random.randint(1, 1000),
                    random.randint(1, 1000),
                    #
                    return_df=False,
                )
                time.sleep(randomize_sleep_time())

                self.pg.execute_query(
                    f'''TRUNCATE TABLE ttruncate;''',
                    return_df=False
                )
                time.sleep(randomize_sleep_time())
        except:
            stop.value = True
            logger.error('Stopping for error')
            raise


class Delete(multiprocessing.Process):
    """
    Generate table, insert values, and delete
    """

    def __init__(self) -> None:
        super().__init__()
        self.pg = PG(PG_CONN_NAME)

    def run(self):
        logger.info(f'{Delete.__name__} started')

        try:
            # Create table
            self.pg.execute_query(
                f'''
                CREATE TABLE IF NOT EXISTS tdelete (
                    id SERIAL PRIMARY KEY,
                    value1 INTEGER
                );
                ALTER TABLE tdelete REPLICA IDENTITY FULL;
                ''',
                return_df=False
            )

            while True:
                # Insert
                self.pg.execute_query(
                    f'''INSERT INTO tdelete (value1) VALUES (%s);''',
                    random.randint(1, 1000),
                    #
                    return_df=False,
                )
                time.sleep(randomize_sleep_time())

                # Delete
                self.pg.execute_query(
                    f'''DELETE FROM tdelete;''',
                    return_df=False
                )
                time.sleep(randomize_sleep_time())
        except:
            stop.value = True
            logger.error('Stopping for error')
            raise


class Update(multiprocessing.Process):
    """
    Generate table, insert values, and update
    """

    def __init__(self) -> None:
        super().__init__()
        self.pg = PG(PG_CONN_NAME)

    def run(self):
        logger.info(f'{Update.__name__} started')

        try:
            # Create table
            self.pg.execute_query(
                f'''
                CREATE TABLE IF NOT EXISTS tupdate (
                    id SERIAL PRIMARY KEY,
                    value1 INTEGER
                );
                ALTER TABLE tupdate REPLICA IDENTITY FULL;
                ''',
                return_df=False
            )

            while True:
                # Insert
                self.pg.execute_query(
                    f'''INSERT INTO tupdate (value1) VALUES (%s);''',
                    random.randint(1, 1000),
                    #
                    return_df=False,
                )
                time.sleep(randomize_sleep_time())

                # Update
                self.pg.execute_query(
                    f'''UPDATE tupdate SET value1 = %s;''',
                    random.randint(1, 1000),
                    #
                    return_df=False
                )
                time.sleep(randomize_sleep_time())
        except:
            stop.value = True
            logger.error('Stopping for error')
            raise


if __name__ == '__main__':
    ps = [
        AllDtype(),
        Gen100(),
        # BigText(),
        Truncate(),
        Delete(),
        Update(),
    ]
    for p in ps:
        p.start()

    for p in ps:
        p.join()
        logger.warning('Joined')

    logger.info('Exiting')
    quit()
