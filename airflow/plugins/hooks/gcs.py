from airflow.hooks.base import BaseHook


class GCSHook(BaseHook):
    def __init__(self, sa_filename: str):
        super().__init__()

        from google.cloud import storage
        self.storage_client = storage.Client.from_service_account_json(sa_filename)

    def download_to_memory(self, gcs_bucket: str, gcs_filename: str) -> bytes:
        storage_bucket = self.storage_client.bucket(gcs_bucket)
        storage_object = storage_bucket.blob(gcs_filename)

        return storage_object.download_as_string()

    def upload_from_memory(self, data: str, gcs_bucket: str, gcs_filename: str) -> str:
        storage_bucket = self.storage_client.bucket(gcs_bucket)
        storage_object = storage_bucket.blob(gcs_filename)

        storage_object.upload_from_string(data)

        return storage_object.name

    def upload_from_filename(self, filename: str, gcs_bucket: str, gcs_filename: str) -> str:
        storage_bucket = self.storage_client.bucket(gcs_bucket)
        storage_object = storage_bucket.blob(gcs_filename)

        storage_object.upload_from_filename(filename)

        return storage_object.name

    # def upload_csv_rows(self, rows: list[list[str]], gcs_bucket: str, gcs_filename: str) -> str:
    #     storage_bucket = self.storage_client.bucket(gcs_bucket)
    #     storage_object = storage_bucket.blob(gcs_filename)

    #     storage_object.upload_from_string('\n'.join([','.join(row) for row in rows]), content_type='text/csv')

    #     return storage_object.name
