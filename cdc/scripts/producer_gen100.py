import time
import sys
import random
import json
from loguru import logger
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime, timezone

from utill.my_string import generate_random_string
from utill.my_pg_v2 import PG

logger.remove()
logger.add(sys.stdout, level='INFO')

pg = PG('stream-local-postgres')

logger.info('Creating tables')

for i in range(100):
    # Create all the tables
    pg.execute_query(
        f'''
        CREATE TABLE IF NOT EXISTS gen_{i} (
            id SERIAL PRIMARY KEY,
            value1 INTEGER,
            value2 VARCHAR(10),
            value3 VARCHAR(50),
            value4 JSON,
            value5 BOOLEAN NOT NULL,
            value6 TIMESTAMPTZ NOT NULL,
            value7 TIMESTAMP NOT NULL
        );
        ALTER TABLE gen_{i} REPLICA IDENTITY FULL;
        ''', return_df=False
    )

logger.info('Tables created')


def insert(i):
    while True:
        random_json = json.dumps({generate_random_string(8): generate_random_string(12)})
        pg.execute_query(
            f'''INSERT INTO gen_{i} (value1, value2, value3, value4, value5, value6, value7) VALUES (%s, %s, %s, %s, %s, %s, %s);''',
            random.randint(1, 10000),
            generate_random_string(10),
            generate_random_string(50),
            random_json,
            random.choice([True, False]),
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
            return_df=False
        )

logger.info('Inserting...')

with ThreadPoolExecutor(max_workers=100) as executor:
    futures = [executor.submit(insert, i) for i in range(100)]
    [f.result() for f in futures]
