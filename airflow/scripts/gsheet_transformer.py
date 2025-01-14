from plugins.constants.bigquery import BigQueryDataType


def transform_daily_cc_usage(gcs_filename: str, cols: dict[str, BigQueryDataType]):
    import logging
    import csv

    from datetime import timedelta
    from io import StringIO

    from config.settings import SA_FILENAME
    from plugins.hooks.gcs import GCSHook
    from plugins.constants.gsheet import GSHEET_EPOCH

    logger = logging.getLogger(__name__)

    logger.info(f'gcs_filename: {gcs_filename}')
    logger.info(f'cols: {cols}')
    gcs_bucket, gcs_filename = gcs_filename.split('/', 1)

    gcs_hook = GCSHook(SA_FILENAME)

    # Download the file
    rows = list(csv.reader(gcs_hook.download_to_memory(gcs_bucket, gcs_filename).decode('utf-8').splitlines()))
    logger.info(f'Downloaded {gcs_filename}: {len(rows)} rows')

    # Transform
    cols_length = len(cols)
    for i in range(1, len(rows)):
        for j, dtype in enumerate(cols.values()):
            # Prevent index not found
            if not cols_length > j:
                continue
            if not rows[i][j]:
                continue

            match dtype:
                case 'INTEGER':
                    rows[i][j] = int(rows[i][j])
                case 'DATE':
                    rows[i][j] = (GSHEET_EPOCH + timedelta(days=int(rows[i][j]))).strftime('%Y-%m-%d')
                case _:
                    pass
    logger.info('Transformed')

    # Write back to GCS
    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerows(rows)
    csv_buffer.seek(0)
    gcs_hook.upload_from_memory(csv_buffer.getvalue(), gcs_bucket, gcs_filename)
    logger.info(f'Uploaded {len(rows)} rows to {gcs_filename}')
