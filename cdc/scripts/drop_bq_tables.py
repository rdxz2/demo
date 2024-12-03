from google.cloud import bigquery

client = bigquery.Client.from_service_account_json('/home/ubuntu/repos/xz2/demo/secrets/xz2-demo-9fc7663fdfac.json')
all_tables = client.list_tables('log__stream')
for table in all_tables:
    print(f'Dropping {table.table_id}')
    client.delete_table(table)
