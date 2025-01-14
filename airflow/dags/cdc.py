from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timezone

from config.settings import CDC__MERGER_TARGET_CONN_IDS
from scripts.cdc.merger import merge
from scripts.cdc.validator_log import validate_log

with DAG(
    dag_id='cdc',
    start_date=datetime(2025, 1, 1),
    schedule='0 7 * * *',
    catchup=False,
):
    tgs = []
    for conn_id in CDC__MERGER_TARGET_CONN_IDS:
        with TaskGroup(conn_id['task_group_id']) as tg:
            task_merge = PythonOperator(
                task_id='merge',
                python_callable=merge,
                op_kwargs={
                    'conn_id': conn_id,
                    'cutoff_ts': datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
                },
                provide_context=True,
            )

            task_validate_log = PythonOperator(
                task_id='validate_log',
                python_callable=validate_log,
                op_kwargs={
                    'conn_id': conn_id,
                },
                provide_context=True,
            )

            task_merge >> task_validate_log

            tgs.append(tg)
