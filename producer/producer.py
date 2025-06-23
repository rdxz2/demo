import os
import sys
import threading
import traceback

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from loguru import logger
from random import randint, uniform, choice
from textwrap import dedent
from time import sleep
from utill.my_pg import PG
from utill.my_string import generate_random_string

logger.remove()
logger.add(sys.stderr, level='INFO')

__PG_CONNECTION_FILE = os.path.join(os.path.dirname(__file__), 'pg.json')
__PG_CONNECTION_NAME = 'stream'
__SERVICE_NAME = os.path.basename(__file__).removesuffix('.py')

__LOAN_DURATION_CHOICE = [n * 30 for n in range(1, 12)]  # 1 ~ 12 months

__STOP_EVENT = threading.Event()


def __random_sleep():
    """
    Just a random sleep time
    """

    sleep(randint(10, 120))


def __is_table_exists(pg: PG, table_name: str):
    """
    Check if table exists in database
    """

    return pg.execute_query('select count(1) from information_schema.tables where table_schema || \'.\' || table_name = %s', (table_name, )).fetchone()[0] > 0


def simulate_user():
    """
    Simulate user data
    """

    pg = PG(__PG_CONNECTION_NAME, config_source=__PG_CONNECTION_FILE, application_name=f'{__SERVICE_NAME}-{simulate_user.__name__}')
    logger.info(f'{simulate_user.__name__}: Start')

    try:
        # Create table if not exists
        if not __is_table_exists(pg, 'public.user'):
            pg.execute_query(dedent(
                f'''
                create table public.user (
                    id serial not null primary key,
                    created_at timestamptz not null default current_timestamp,
                    updated_at timestamptz not null default current_timestamp,
                    username varchar(20) not null,
                    email varchar(50) not null,
                    password varchar(200) not null,
                    last_login timestamptz,
                    is_active boolean not null
                );
                alter table public.user replica identity full;
                create index ix_{generate_random_string(6, True)} on public.user(is_active);
                '''
            ))
            logger.info(f'{simulate_user.__name__}: Create TABLE')

        # Infinitely run
        while not __STOP_EVENT.is_set():
            probability = randint(0, 100)
            if 0 <= probability <= 60:
                # 60% chance create user
                id = pg.execute_query('insert into public.user (username, email, password, is_active) values (%s, %s, %s, %s) returning id', (
                    generate_random_string(randint(5, 20), True),  # Username
                    f'{generate_random_string(randint(5, 35))}@{generate_random_string(randint(5, 10), True)}.{generate_random_string(3, True)}',  # Email
                    generate_random_string(50),  # Password
                    True,  # Is active
                )).fetchone()[0]
                logger.info(f'{simulate_user.__name__}: Create {id}')
            elif 60 <= probability <= 99:
                # 30% chance update user
                # Can update 1 to 5 user at a time
                # Update the last login
                ids = pg.execute_query(f'select id from public.user where is_active = %s order by random() limit {randint(1, 5)}', (True, )).fetchall()
                ids = [x[0] for x in ids]
                pg.execute_query('update public.user set updated_at = current_timestamp, last_login = current_timestamp where id = any(%s)', (ids, ))
                logger.info(f'{simulate_user.__name__}: Update {ids}')
            elif 99 <= probability <= 100:
                # 1% chance soft-delete user
                id = pg.execute_query('select id from public.user where is_active = %s limit 1', (True, )).fetchone()[0]
                if id is not None:
                    pg.execute_query('update public.user set updated_at = current_timestamp, is_active = %s where id = %s', (False, id))
                    logger.info(f'{simulate_user.__name__}: Soft-delete {id}')
            else:
                raise ValueError('Probability out of range!')

            __random_sleep()

        logger.warning(f'{simulate_user.__name__}: Gracefully stopping')
    except Exception as e:
        __STOP_EVENT.set()
        logger.error(f'{simulate_user.__name__}: Stopping for error --> {e}\t{traceback.format_exc()}')
        raise


def simulate_loan():
    """
    Simulate loan transaction
    """

    pg = PG(__PG_CONNECTION_NAME, config_source=__PG_CONNECTION_FILE, application_name=f'{__SERVICE_NAME}-{simulate_loan.__name__}')
    logger.info(f'{simulate_loan.__name__}: Start')

    try:
        # Create table
        if not __is_table_exists(pg, 'public.loan'):
            pg.execute_query(dedent(
                '''
                create table public.loan (
                    id bigserial not null primary key,
                    created_at timestamptz not null default current_timestamp,
                    updated_at timestamptz not null default current_timestamp,
                    user_id varchar(20) not null,
                    product_id smallint not null,
                    interest_rate int not null,
                    amount decimal(10, 2) not null,
                    interest decimal(10, 2) not null,
                    duration int not null,
                    first_due_date date not null,
                    last_due_date date not null,
                    is_paid_off boolean not null
                );
                alter table public.loan replica identity full;
                '''
            ))
            logger.info(f'{simulate_loan.__name__}: Create TABLE')

        # Infinitely run
        while not __STOP_EVENT.is_set():
            user_id = pg.execute_query('select id from public.user where is_active = %s order by random() limit 1', (True, )).fetchone()
            if user_id is not None:
                amount = round(uniform(10.0, 10000.0), 2)
                interest_rate = randint(1, 10)
                interest = amount * interest_rate
                duration = choice(__LOAN_DURATION_CHOICE)
                first_due_date = datetime.now(timezone.utc).date()
                last_due_date = first_due_date + timedelta(days=duration)
                id = pg.execute_query('insert into public.loan (user_id, product_id, interest_rate, amount, interest, duration, first_due_date, last_due_date, is_paid_off) values (%s, %s, %s, %s, %s, %s, %s, %s, %s) returning id', (
                    user_id,  # User ID
                    randint(1, 5),  # Product ID
                    interest_rate,  # Interest rate
                    amount,  # Amount
                    interest,  # Interest
                    duration,  # Duration
                    first_due_date,  # First due date
                    last_due_date,  # Last due date
                    False,  # Is paid off
                )).fetchone()[0]
                logger.info(f'{simulate_loan.__name__}: Create {id}')
            else:
                logger.warning(f'{simulate_loan.__name__}: Noop')

            __random_sleep()

        logger.warning(f'{simulate_loan.__name__}: Gracefully stopping')
    except Exception as e:
        __STOP_EVENT.set()
        logger.error(f'{simulate_loan.__name__}: Stopping for error --> {e}\t{traceback.format_exc()}')
        raise


def simulate_repayment():
    """
    Simulate loan repayment
    """

    pg = PG(__PG_CONNECTION_NAME, config_source=__PG_CONNECTION_FILE, application_name=f'{__SERVICE_NAME}-{simulate_repayment.__name__}')
    logger.info(f'{simulate_repayment.__name__}: Start')

    try:
        # Create table if not exists
        if not __is_table_exists(pg, 'public.repayment'):

            # Make sure loan table exists
            while not __is_table_exists(pg, 'public.loan'):
                logger.add(f'{simulate_repayment.__name__}: Wait loan table creation...')
                sleep(10)

            pg.execute_query(dedent(
                f'''
                create table public.repayment (
                    id serial not null primary key,
                    created_at timestamptz not null default current_timestamp,
                    updated_at timestamptz not null default current_timestamp,
                    loan_id bigint not null references public.loan(id),
                    amount decimal(10, 2) not null
                );
                alter table public.repayment replica identity full;
                '''
            ))
            logger.info(f'{simulate_repayment.__name__}: Create TABLE')

        # Infinitely run
        while not __STOP_EVENT.is_set():
            # Search for outstanding loan
            # Not created today
            row = pg.execute_query(dedent(
                '''
                select l.id
                    , (l.amount + l.interest) as total
                    , (l.amount + l.interest) - sum(r.amount) as outstanding
                from public.loan l
                join public.repayment r on r.loan_id = l.id
                where l.is_paid_off = %s
                    and l.created_at != current_timestamp::date
                group by l.id, l.amount, l.interest
                order by random()
                limit 1
                '''
            ), (False, )).fetchone()
            if row is not None:
                loan_id, total, outstanding = row
                repayment_amount = 0
                probability = randint(0, 100)
                if 0 <= probability <= 80:
                    # 80% full repayment
                    repayment_amount = outstanding
                elif 80 <= probability <= 100:
                    # 20% partial payment
                    repayment_amount = outstanding - min(outstanding, uniform(0.01, outstanding))
                else:
                    raise ValueError('Probability out of range!')
                id = pg.execute_query('insert into public.repayment (loan_id, amount) values (%s, %s) returning id', (loan_id, repayment_amount)).fetchone()[0]

                # Loan paid off
                if total == outstanding + repayment_amount:
                    pg.execute_query('update public.loan set updated_at = current_timestamp, is_paid_off = %s where loan_id = %s', (True, loan_id))
                    logger.info(f'{simulate_repayment.__name__}: Paid off {loan_id}')

                logger.info(f'{simulate_repayment.__name__}: Create {id}')
            else:
                logger.warning(f'{simulate_repayment.__name__}: Noop')

            __random_sleep()

        logger.warning(f'{simulate_repayment.__name__}: Gracefully stopping')
    except Exception as e:
        __STOP_EVENT.set()
        logger.error(f'{simulate_repayment.__name__}: Stopping for error --> {e}\t{traceback.format_exc()}')
        raise


if __name__ == '__main__':
    logger.info('Start producing...')

    # Execute
    functions = [
        simulate_user,
        simulate_loan,
        simulate_repayment,
    ]
    with ThreadPoolExecutor(max_workers=len(functions)) as executor:
        fs = [executor.submit(f) for f in functions]
        [f.result() for f in fs]

    logger.info('Stop producing')
