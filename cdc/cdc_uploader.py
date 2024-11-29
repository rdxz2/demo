import dotenv
import glob
import gzip
import os
import shutil
import time

from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from io import StringIO
from loguru import logger

dotenv.load_dotenv()

SA_FILENAME = os.environ['SA_FILENAME']

GCS_BUCKET = os.environ['GCS_BUCKET']

REPL_DB_NAME = os.environ['REPL_DB_NAME']

UPLOAD_OUTPUT_DIR = os.path.join('output', REPL_DB_NAME, 'upload')
UPLOADER_THREADS = int(os.environ['UPLOADER_THREADS'])


def compress(src_file: str):
    dst_file = src_file + '.gz'

    os.remove(dst_file) if os.path.exists(dst_file) else None
    with open(src_file, 'rb') as f_in:
        with gzip.open(dst_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    return dst_file


def upload(filename: str, bucket: storage.Bucket):
    # Compress file
    compressed_filename = compress(filename)
    logger.debug(f'Compress: {filename} --> {compressed_filename}')
    compressed_filename_base = os.path.basename(compressed_filename)

    # Determine final path
    stringio = StringIO(compressed_filename_base.removeprefix('.json'))
    year, month, day, hour, _, _, _, table_name = stringio.read(4), stringio.read(2), stringio.read(2), stringio.read(2), stringio.read(2), stringio.read(2), stringio.read(1), stringio.read()
    gcs_path = f'{REPL_DB_NAME}/cdc/{table_name}/{year}/{month}/{day}/{hour}/{compressed_filename_base}'

    # Upload to GCS
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(compressed_filename)
    os.remove(compressed_filename)
    os.remove(filename)

    logger.info(f'Upload: {compressed_filename} --> {blob.name}')


if __name__ == '__main__':
    gcs = storage.Client.from_service_account_json(SA_FILENAME)
    bucket = gcs.bucket(GCS_BUCKET)
    logger.info(f'Connected to GCS: {bucket.name}')

    thread_pool_executor = ThreadPoolExecutor(max_workers=UPLOADER_THREADS)
    while True:
        if filenames := glob.glob(f'{UPLOAD_OUTPUT_DIR}/*.json'):
            for filename in filenames:
                thread_pool_executor.submit(upload, filename, bucket)

        time.sleep(1)
