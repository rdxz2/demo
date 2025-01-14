
import logging
import humanize

from google.cloud import bigquery
from airflow.hooks.base import BaseHook
from google.api_core.exceptions import NotFound

from plugins.constants.bigquery import BigQueryDataType, LoadStrategy

logger = logging.getLogger(__name__)


class BigQueryHook(BaseHook):
    def __init__(self, sa_filename: str):
        super().__init__()

        self.bq_client = bigquery.Client.from_service_account_json(sa_filename)

    def get_conn(self) -> bigquery.Client:
        return self.bq_client

    def close(self):
        self.bq_client.close()

    def execute_query(self, query: str, dry_run: bool = False) -> bigquery.QueryJob:
        multistatement = type(query) == list
        if multistatement:
            query = '\n'.join([x if str(x).strip().endswith(';') else x + ';' for x in query if x])  # noqa
        else:
            query = query.strip()

        logger.debug(f'🔎 Query:\n{query}')
        query_job_config = bigquery.QueryJobConfig(dry_run=dry_run)
        query_job = self.bq_client.query(query, job_config=query_job_config)
        query_job.result()  # Wait for query execution

        if not multistatement:
            logger.debug(f'[Job ID] {query_job.job_id}, [Processed] {humanize.naturalsize(query_job.total_bytes_processed)}, [Billed] {humanize.naturalsize(query_job.total_bytes_billed)}, [Affected] {query_job.num_dml_affected_rows or 0} row(s)',)
        else:
            logger.debug(f'[Job ID] {query_job.job_id}')
            [logger.debug(f'[Script ID] {job.job_id}, [Processed] {humanize.naturalsize(job.total_bytes_processed)}, [Billed] {humanize.naturalsize(job.total_bytes_billed)}, [Affected] {job.num_dml_affected_rows or 0} row(s)',) for job in self.bq_client.list_jobs(parent_job=query_job.job_id)]

        return query_job

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
