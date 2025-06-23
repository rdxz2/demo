from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    DEBUG: int = 0

    # Configure streamer (target database)
    STREAM_DB_HOST: str
    STREAM_DB_PORT: int
    STREAM_DB_USER: str
    STREAM_DB_PASS: str
    STREAM_DB_NAME: str

    LOG_DIR: str = f'logs/{STREAM_DB_NAME}'

    # Configure streamer
    STREAM_OUTPUT_DIR: str = f'output/{STREAM_DB_NAME}/stream'
    STREAM_PUBLICATION_NAME: str
    STREAM_REPLICATION_SLOT_NAME: str
    STREAM_NO_MESSAGE_REPORT_INTERVAL_S: Optional[int] = 60  # If no message received for this time, report it
    STREAM_DELAY_PRINT_INTERVAL_S: Optional[int] = 1
    STREAM_CONSUMER_QUEUE_MAX_SIZE: Optional[int] = 1000
    STREAM_CONSUMER_POLL_INTERVAL_S: Optional[int] = 1  # Number of seconds to wait if there's no message in the queue
    STREAM_FILEWRITER_MAX_FILE_SIZE_B: Optional[int] = 2000000  # If a single file exceeds this size, close all files
    STREAM_FILEWRITER_ALL_FILE_MAX_OPENED_TIME_S: Optional[int] = 600  # If a file is opened for this time, close all files
    STREAM_FILEWRITER_NO_MESSAGE_WAIT_TIME_S: Optional[int] = 600  # If no message received for this time, close all files

    UPLOAD_OUTPUT_DIR: str = f'output/{STREAM_DB_NAME}/upload'
    UPLOAD_SA_FILENAME: str
    UPLOAD_BQ_PROJECT_ID: str
    UPLOAD_BQ_DATASET_LOCATION: str
    UPLOAD_BQ_LOG_DATASET_PREFIX: str
    UPLOAD_THREADS: Optional[int] = 10
    UPLOAD_FILE_POLL_INTERVAL_S: Optional[int] = 1
    UPLOAD_STREAM_CHUNK_SIZE_B: Optional[int] = 8000000
    UPLOAD_NO_FILE_REPORT_INTERVAL_S: Optional[int] = 600

    # # Configure PG to BQ
    # PGTOBQ_OUTPUT_DIR: str = f'output/{NAME}/bqtobq'
    # PGTOBQ_UPLOAD_QUEUE_MAX_SIZE = 10
    # PGTOBQ_GCS_BUCKET: str
    # PGTOBQ_GCS_BASE_PATH: str

    # Configure alerts
    DISCORD_WEBHOOK_URL: Optional[str] = None

    class Config:
        env_file = '.env'


settings = Settings()
