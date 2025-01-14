
import logging

from google.cloud import bigquery
from airflow.hooks.base import BaseHook
from google.api_core.exceptions import NotFound

from plugins.constants.bigquery import BigQueryDataType, LoadStrategy

logger = logging.getLogger(__name__)


class BigQueryHook(BaseHook):
    def __init__(self, sa_filename: str):
        super().__init__()

        self.bq_client = bigquery.Client.from_service_account_json(sa_filename)

    def load_from_gcs(
        self,
        gcs_filename: str,
        bq_table_fqn: str,
        cols: dict[str, BigQueryDataType],
        partition_col: str = None,
        cluster_cols: list[str] = [],
        load_strategy: str = LoadStrategy.OVERWRITE,
        replace: bool = False
    ) -> str:
        if load_strategy not in LoadStrategy:
            raise ValueError('Invalid mode')

        schema = [bigquery.SchemaField(k, v) for k, v in cols.items()]
        schema_set = {f'{col.name}-{col.field_type}' for col in schema}

        try:
            bq_table = bigquery.Table(bq_table_fqn)

            if replace:
                self.bq_client.delete_table(bq_table)
                logger.info(f'Table dropped: {bq_table_fqn}')
            else:
                # Raise for schema mismatch
                for col in bq_table.schema:
                    if f'{col.name}-{col.field_type}' not in schema_set:
                        raise ValueError(f'Existing mismatch: {col.name} ({col.field_type})')

                # Raise for difference in partitioning
                if bq_table.time_partitioning:
                    if bq_table.time_partitioning.field != partition_col:
                        raise ValueError(f'Partitioning mismatch: {bq_table.time_partitioning.field} != {partition_col}')

                # Raise for different in clustering
                if bq_table.clustering_fields:
                    existing_clustering_fields = set(bq_table.clustering_fields)
                    new_clustering_fields = set(cluster_cols)

                    if existing_clustering_fields != new_clustering_fields:
                        raise ValueError(f'Clustering mismatch: {sorted(existing_clustering_fields)} != {sorted(new_clustering_fields)}')

                if load_strategy == LoadStrategy.OVERWRITE:
                    self.bq_client.query_and_wait(f'TRUNCATE TABLE `{bq_table_fqn}`')
        except NotFound:
            # Warning for schema mismatch
            table = bigquery.Table(bq_table_fqn, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(field=partition_col)
            table.partitioning_type = 'DAY'
            bq_table = self.bq_client.create_table(table)
        logger.info(f'Table created: {bq_table_fqn}')

        # Load job
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1
        job_config.schema = schema

        load_job = self.bq_client.load_table_from_uri(f'gs://{gcs_filename}', bq_table, job_config=job_config)
        load_job.result()
        logger.info(f'Loaded {gcs_filename} to {bq_table_fqn}')
