from plugins.constants.bigquery import BigQueryDataType, LoadStrategy

TARGETS_DAILY_CC_USAGE = [
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
