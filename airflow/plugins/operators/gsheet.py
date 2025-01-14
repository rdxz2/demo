import csv
import logging
import os

from airflow.models import BaseOperator
from utill.my_string import replace_nonnumeric

from config.settings import SA_FILENAME, OUTPUT_DIR
from plugins.hooks.gcs import GCSHook
from plugins.hooks.gsheet import GSheetHook

logger = logging.getLogger(__name__)


class GSheetToGCSOperator(BaseOperator):
    def __init__(self, id: str, sheet: str, range: str, gcs_bucket: str, **kwargs):
        super().__init__(**kwargs)
        self.id = id
        self.sheet = sheet
        self.range = range

        self.gcs_bucket = gcs_bucket

    def execute(self, context):
        gsheet_hook = GSheetHook(SA_FILENAME)

        rows: list[list[str]] = gsheet_hook.fetch_rows(self.id, self.sheet, self.range)

        dt_str = context['execution_date'].format('YYYY-MM-DD-HH-mm-ss')  # Pendulum object

        # Scan for total column count
        total_columns = 0
        for row in rows:
            total_columns = max(total_columns, len(row))
        logger.info(f'Total columns: {total_columns}')

        # Add missing columns
        for row in rows:
            for _ in range(total_columns - len(row)):
                row.append(None)

        # Save to local
        filename = os.path.join(OUTPUT_DIR, f'{self.id}_{replace_nonnumeric(self.sheet, "-")}_{replace_nonnumeric(self.range, "-")}.csv')
        with open(filename, 'w') as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow([f'col_{i}' for i in range(total_columns)])  # Header
            csv_writer.writerows(rows)

        # Upload to GCS
        gcs_hook = GCSHook(SA_FILENAME)
        gcs_filename = gcs_hook.upload_from_filename(filename, self.gcs_bucket, f'gsheet/{dt_str}/{os.path.basename(filename)}')
        os.remove(filename)
        logger.info(f'Uploaded {len(rows)} rows to GCS: {gcs_filename}')

        # Return value
        context['task_instance'].xcom_push('gcs_filename', f'{self.gcs_bucket}/{gcs_filename}')
