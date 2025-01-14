import os

from dotenv import load_dotenv

from plugins.constants.bigquery import BigQueryDataType, LoadStrategy

load_dotenv()

SA_FILENAME = os.environ['SA_FILENAME']

OUTPUT_DIR = os.environ['OUTPUT_DIR']

GCS_BUCKET = os.environ['GCS_BUCKET']

BQ_PROJECT_ID = os.environ['BQ_PROJECT_ID']
BQ_DATASET_ID = os.environ['BQ_DATASET_ID']
BQ_DATASET_LOCATION = os.environ['BQ_DATASET_LOCATION']

# <<----- START: CDC related

CDC__STREAM_FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S = os.environ['CDC__STREAM_FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S']
CDC__STREAM_FILEWRITER_NO_MESSAGE_WAIT_TIME_S = os.environ['CDC__STREAM_FILEWRITER_NO_MESSAGE_WAIT_TIME_S']

CDC__MERGER_THREADS = int(os.environ['CDC__MERGER_THREADS'])
CDC__MERGER_TARGET_CONN_IDS = os.environ['CDC__MERGER_TARGET_CONN_IDS'].split(',')
CDC__BQ_LOG_DATASET_PREFIX = os.environ['CDC__BQ_LOG_DATASET_PREFIX']

CDC__VALIDATION_INITIAL_ROUND_PRECISION = os.environ['CDC__VALIDATION_INITIAL_ROUND_PRECISION']

# END: CDC related ----->>

# <<----- START: GSheet related

GSHEET__TARGETS_DAILY_CC_USAGE = [
    {
        'task_group_id': 'daily_far',
        'gsheet': {
            'id': '1pTwHma-f_OSiq2jTZN4-o04EBbMLZlM0WbXGTJLuurQ',
            'sheet': 'daily far 2025',
            'range': 'A2:J1000',
        },
        'bq_table': {
            'fqn': 'xz2-demo.datalake.gsheet__daily_far',
            'cols': {
                'year': BigQueryDataType.INTEGER,
                'month': BigQueryDataType.INTEGER,
                'day': BigQueryDataType.INTEGER,
                'date': 'DATE',
                'item': 'STRING',
                'category': 'STRING',
                'price': 'FLOAT64',
                'payment_method': 'STRING',
                'notes': 'STRING',
                'billed_on': BigQueryDataType.INTEGER,
            },
            'partition_col': 'date',
            'cluster_cols': [],
            'load_strategy': LoadStrategy.OVERWRITE,
        },
    },
    {
        'task_group_id': 'daily_rd',
        'gsheet': {
            'id': '1pTwHma-f_OSiq2jTZN4-o04EBbMLZlM0WbXGTJLuurQ',
            'sheet': 'daily rd 2025',
            'range': 'A2:J1000',
        },
        'bq_table': {
            'fqn': 'xz2-demo.datalake.gsheet__daily_rd',
            'cols': {
                'year': BigQueryDataType.INTEGER,
                'month': BigQueryDataType.INTEGER,
                'day': BigQueryDataType.INTEGER,
                'date': 'DATE',
                'item': 'STRING',
                'category': 'STRING',
                'price': 'FLOAT64',
                'payment_method': 'STRING',
                'notes': 'STRING',
                'billed_on': BigQueryDataType.INTEGER,
            },
            'partition_col': 'date',
            'cluster_cols': [],
            'load_strategy': LoadStrategy.OVERWRITE,
        },
    },
]

# END: GSheet related ----->>

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
