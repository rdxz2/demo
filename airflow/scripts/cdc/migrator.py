import glob
import os
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from plugins.constants.conn import CONN_ID_PG_CDC
from utill.my_string import generate_random_string


MIGRATION_TABLE = 'public.migration'
APPLICATION_NAME = f'cdc-migrate-{generate_random_string()}'

logger = logging.getLogger(__name__)


def migrate():
    postgres_hook = PostgresHook(CONN_ID_PG_CDC)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

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
    migrations = {os.path.basename(x) for x in glob.glob(os.path.join(os.path.dirname(__file__), 'migrations', '*.sql'))}
    cursor.execute(f'SELECT name FROM {MIGRATION_TABLE};')
    executed_migrations = set([x[0] for x in cursor.fetchall()])
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


if __name__ == '__main__':
    migrate()
