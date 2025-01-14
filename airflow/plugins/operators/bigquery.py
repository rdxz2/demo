import logging

from airflow.models import BaseOperator

from config.settings import SA_FILENAME
from plugins.constants.bigquery import BigQueryDataType, LoadStrategy
from plugins.hooks.bigquery import BigQueryHook

logger = logging.getLogger(__name__)


class GCSToBigQueryOperator(BaseOperator):
    def __init__(
        self,
        bq_table_fqn: str,
        cols: dict[str, BigQueryDataType],
        xcom_task_id: str,
        partition_col: str = None,
        cluster_cols: list[str] = [],
        load_strategy: LoadStrategy = None,
        replace: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.bq_table_fqn = bq_table_fqn
        self.cols = cols
        self.partition_col = partition_col
        self.cluster_cols = cluster_cols
        self.load_strategy = load_strategy
        self.replace = replace

        self.xcom_task_id = xcom_task_id

    def execute(self, context):
        gcs_filename = context['task_instance'].xcom_pull(task_ids=self.xcom_task_id, key='gcs_filename')

        bigquery_hook = BigQueryHook(SA_FILENAME)
        bigquery_hook.load_from_gcs(gcs_filename, self.bq_table_fqn, self.cols, self.partition_col, self.cluster_cols, self.load_strategy)
