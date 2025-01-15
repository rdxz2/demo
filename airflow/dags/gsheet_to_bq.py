from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from config.settings import GCS_BUCKET, GSHEET__TARGETS_DAILY_CC_USAGE
from plugins.operators.bigquery import GCSToBigQueryOperator
from plugins.operators.gsheet import GSheetToGCSOperator
from scripts.gsheet_transformer import transform_daily_cc_usage

with DAG(
    dag_id='gsheet_to_bq',
    start_date=datetime(2025, 1, 1),
    schedule='0 7-22 * * *',
    catchup=False,
):
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    tgs = []
    for target in GSHEET__TARGETS_DAILY_CC_USAGE:
        with TaskGroup(target['task_group_id']) as tg:
            task_gsheet_to_gcs = GSheetToGCSOperator(
                task_id='gsheet_to_gcs',
                id=target['gsheet']['id'],
                sheet=target['gsheet']['sheet'],
                range=target['gsheet']['range'],
                gcs_bucket=GCS_BUCKET,
            )

            task_transform = PythonOperator(
                task_id='transform',
                python_callable=transform_daily_cc_usage,
                op_kwargs={
                    'gcs_filename': "{{{{ task_instance.xcom_pull(task_ids='{}', key='gcs_filename') }}}}".format(task_gsheet_to_gcs.task_id),
                    'cols': target['bq_table']['cols'],
                },
                provide_context=True,
            )

            task_gcs_to_bq = GCSToBigQueryOperator(
                task_id='gcs_to_bq',
                bq_table_fqn=target['bq_table']['fqn'],
                cols=target['bq_table']['cols'],
                partition_col=target['bq_table']['partition_col'],
                cluster_cols=target['bq_table']['cluster_cols'],
                load_strategy=target['bq_table']['load_strategy'],
                xcom_task_id=task_gsheet_to_gcs.task_id,
            )

            task_gsheet_to_gcs >> task_transform >> task_gcs_to_bq

            tgs.append(tg)
