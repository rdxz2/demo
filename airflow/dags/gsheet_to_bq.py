from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from config.settings import GCS_BUCKET
from config.gsheet_to_bq import TARGETS_DAILY_CC_USAGE
from plugins.operators.bigquery import GCSToBigQueryOperator
from plugins.operators.gsheet import GSheetToGCSOperator
from scripts.gsheet_transformer import transform_daily_cc_usage

with DAG(
    dag_id='gsheet_to_bq',
    start_date=datetime(2025, 1, 1),
    schedule='0 6 * * *',
    catchup=False,
):
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    tgs = []
    for target in TARGETS_DAILY_CC_USAGE:
        with TaskGroup(target['task_group_id']) as tg:
            gsheet_to_gcs = GSheetToGCSOperator(
                task_id=f'gsheet_to_gcs',
                id=target['gsheet']['id'],
                sheet=target['gsheet']['sheet'],
                range=target['gsheet']['range'],
                gcs_bucket=GCS_BUCKET,
            )

            transform = PythonOperator(
                task_id=f'transform',
                python_callable=transform_daily_cc_usage,
                op_kwargs={
                    'gcs_filename': "{{{{ task_instance.xcom_pull(task_ids='{}', key='gcs_filename') }}}}".format(gsheet_to_gcs.task_id),
                    'cols': target['bq_table']['cols'],
                },
                provide_context=True,
            )

            gcs_to_bq = GCSToBigQueryOperator(
                task_id=f'gcs_to_bq',
                bq_table_fqn=target['bq_table']['fqn'],
                cols=target['bq_table']['cols'],
                partition_col=target['bq_table']['partition_col'],
                cluster_cols=target['bq_table']['cluster_cols'],
                load_strategy=target['bq_table']['load_strategy'],
                xcom_task_id=gsheet_to_gcs.task_id,
            )

            gsheet_to_gcs >> transform >> gcs_to_bq

            tgs.append(tg)

    start >> tgs >> end
