import os
import boto3
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

# Kaggle dataset
DATASET = "austinreese/craigslist-carstrucks-data"



# Settings
RAW_FILE_NAME = "vehicles.csv"
RAW_FILE_NAME_TARGET = "vehicles.parquet"
RAW_DATA_FOLDER = "raw_data"
DOWNLOAD_TEMP = "tmp_download"

# Load credentials from .env file
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
RAW_FILES_CARS = os.getenv("RAW_FILES_CARS")
UNPROCESSED_FILES_CARS=os.getenv("UNPROCESSED_FILES_CARS")
PROCESSED_FILES_CARS=os.getenv("PROCESSED_FILES_CARS")
TEMP_FILES_CARS = os.getenv("TEMP_FILES_CARS")

# List of unprocessed and processed buckets
BUCKET_LIST = [UNPROCESSED_FILES_CARS, PROCESSED_FILES_CARS, TEMP_FILES_CARS]

# Initialize S3 client
S3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

# Schemas used in the project
SCHEMAS = ["prod", "playground"]

# Column to sort by when preparing the raw data
SORT_COLUMN = "posting_date"