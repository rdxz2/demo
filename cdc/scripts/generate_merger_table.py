import dotenv
import os
import psycopg2
import psycopg2.extensions
import psycopg2.extras

from utill.my_string import generate_random_string

dotenv.load_dotenv(os.path.join(os.path.pardir, '.env.metabase_readonly'))

CDC_DB_HOST = os.environ['CDC_DB_HOST']
CDC_DB_PORT = os.environ['CDC_DB_PORT']
CDC_DB_USER = os.environ['CDC_DB_USER']
CDC_DB_PASS = os.environ['CDC_DB_PASS']
CDC_DB_NAME = os.environ['CDC_DB_NAME']

APPLICATION_NAME = f'cdc-generate-merger-{CDC_DB_NAME}-{generate_random_string()}'

if __name__ == '__main__':
    dsn = psycopg2.extensions.make_dsn(host=CDC_DB_HOST, port=CDC_DB_PORT, user=CDC_DB_USER, password=CDC_DB_PASS, database=CDC_DB_NAME, application_name=APPLICATION_NAME)
    conn = psycopg2.connect(dsn)
    cursor = conn.cursor()

    # Scan for all tables in schema
    cursor.execute('SELECT table_catalog, table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN (%s, %s) AND table_type != %s ORDER BY 1, 2, 3;', ('pg_catalog', 'information_schema', 'VIEW'))
    all_tables = cursor.fetchall()
    for table_catalog, table_schema, table_name in all_tables:
        # Get PK for default cluster key
        cursor.execute(
            '''
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid
                AND a.attnum = ANY(i.indkey)
            WHERE i.indisprimary
                AND i.indrelid = %s::regclass
            ORDER BY a.attnum
            ''',
            (f'{table_schema}.{table_name}',)
        )
        pks = [x[0] for x in cursor.fetchall()]

        # Get columns for default validate key
        cursor.execute(
            '''
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_catalog = %s
                AND table_schema = %s
                AND table_name = %s
            ORDER BY ordinal_position
            ''',
            (table_catalog, table_schema, table_name)
        )
        all_cols = [x for x in cursor.fetchall() if x[0] not in pks]
        cols = [x[0] for x in all_cols][:3]  # Just take the first 3

        # Get first column with type timestamptz as default partition key
        partition_col = None
        for col in all_cols:
            if col[1] == 'timestamp with time zone':
                partition_col = col[0]
                break
        partition_col = 'null' if partition_col is None else f'\'{partition_col}\''

        # Construct SQL insert query
        cluster_cols_str = ', '.join([f'\'{x}\'' for x in pks])
        cluster_cols_str = 'null' if cluster_cols_str == '' else f'ARRAY[{cluster_cols_str}]'
        validate_cols_str = ', '.join([f'\'{x}\'' for x in cols])
        validate_cols_str = f'ARRAY[{validate_cols_str}]'
        q = f'INSERT INTO public.merger("database", "schema", "table", "partition_col", "cluster_cols", "validate_cols") VALUES (\'{table_catalog}\', \'{table_schema}\', \'{table_name}\', {partition_col}, {cluster_cols_str}, {validate_cols_str});'
        print(q)
