import time
import sys
import random
import json
from loguru import logger

from datetime import datetime, timezone

from utill.my_string import generate_random_string
from utill.my_pg_v2 import PG

logger.remove()
logger.add(sys.stdout, level='INFO')

pg = PG('stream-local-postgres')

pg.execute_query(
    '''
CREATE TABLE IF NOT EXISTS auto_stream_a (
    id SERIAL PRIMARY KEY,
    value_int INTEGER,
    value_str1 VARCHAR(100),
    value_str2 VARCHAR(200),
    value_json JSON,
    value_bool BOOLEAN NOT NULL,
    value_ts TIMESTAMPTZ NOT NULL,
    value_ts2 TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    value_dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    value_dt2 TIMESTAMP NOT NULL
);
ALTER TABLE auto_stream_a REPLICA IDENTITY FULL;
''', return_df=False
)

pg.execute_query(
    '''
CREATE TABLE IF NOT EXISTS auto_stream_b (
    id SERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    value FLOAT
);
ALTER TABLE auto_stream_b REPLICA IDENTITY FULL;
''', return_df=False
)

while True:
    # Insert into a
    if random.random() > 0.5:
        random_json = json.dumps({generate_random_string(8): generate_random_string(12)})
        pg.execute_query(
            f'''
            INSERT INTO auto_stream_a (
                value_int,
                value_str1,
                value_str2,
                value_json,
                value_bool,
                value_ts,
                value_dt2
            ) VALUES (
                {random.randint(0, 100000)},
                'str1',
                'str2',
                '{random_json}',
                {random.choice(['true', 'false'])},
                '{datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}',
                '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'
            );
            ''', return_df=False
        )

    # Insert into b
    if random.random() > 0.6:
        pg.execute_query(
            f'''
            INSERT INTO auto_stream_b (
                value
            ) VALUES (
                {random.random()}
            ), (
                {random.random()}
            );
            ''', return_df=False
        )

    # time.sleep(.1)
