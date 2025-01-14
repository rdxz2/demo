import os

from dotenv import load_dotenv

load_dotenv()

SA_FILENAME = os.environ['SA_FILENAME']

OUTPUT_DIR = os.environ['OUTPUT_DIR']

GCS_BUCKET = os.environ['GCS_BUCKET']

BQ_PROJECT_ID = os.environ['BQ_PROJECT_ID']
BQ_DATASET_ID = os.environ['BQ_DATASET_ID']

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
