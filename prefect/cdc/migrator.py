import dotenv
import glob
import os
import psycopg

from prefect import flow, task
from prefect.logging import get_run_logger
from utill.my_string import generate_random_string

dotenv.load_dotenv()

META_DB_HOST = os.environ['META_DB_HOST']
META_DB_PORT = os.environ['META_DB_PORT']
META_DB_USER = os.environ['META_DB_USER']
META_DB_PASS = os.environ['META_DB_PASS']
META_DB_NAME = os.environ['META_DB_NAME']

MIGRATION_TABLE = 'public.migration'

APPLICATION_NAME = f'cdc-migrate-{generate_random_string()}'


@task
def migrate():
    conn = psycopg.connect(f'postgresql://{META_DB_USER}:{META_DB_PASS}@{META_DB_HOST}:{META_DB_PORT}/{META_DB_NAME}?application_name={APPLICATION_NAME}')
    cursor = conn.cursor()

    logger = get_run_logger()

    # <<----- START: Apply migrations

    # Create migration table if not exists
    cursor.execute(
        f'''
        SELECT COUNT(1)
        FROM information_schema.tables
        WHERE table_schema || '.' || table_name = '{MIGRATION_TABLE}'
        '''
    )
    if not cursor.fetchone()[0]:
        cursor.execute(f'CREATE TABLE {MIGRATION_TABLE} (created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, id SERIAL PRIMARY KEY, name VARCHAR(50) NOT NULL);')
        conn.commit()
        logger.info('Migration table created')

    # Apply migrations
    migrations = {os.path.basename(x) for x in glob.glob('migrations/*.sql')}
    executed_migrations = set([x[0] for x in cursor.execute(f'SELECT name FROM {MIGRATION_TABLE};').fetchall()])
    for new_migration in migrations - executed_migrations:
        with open(f'migrations/{new_migration}', 'r') as f:
            cursor.execute(f.read())
            cursor.execute(f'INSERT INTO {MIGRATION_TABLE} (name) VALUES (%s);', (new_migration,))
            conn.commit()
            logger.info(f'Migration {new_migration} applied')
    if not (migrations - executed_migrations):
        logger.info('No new migrations to apply')

    # END: Apply migrations ----->>

    cursor.close()
    conn.close()


@flow
def cdc__migrator():
    migrate()


if __name__ == '__main__':
    cdc__migrator()
