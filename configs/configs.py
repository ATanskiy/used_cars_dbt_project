import os
import boto3
from pathlib import Path
from google.cloud import bigquery
from botocore.config import Config
from dotenv import load_dotenv
load_dotenv()

# Kaggle dataset
DATASET = "serge1024/russian-car-market-feb-march-2023"

# File names and paths
# Settings
RAW_FILE_NAMES_LIST = ["1_main.csv", "1_text.csv", "catalogs.csv", "final_geografic.csv"]
RAW_FILE_NAMES_LIST_TARGET = ["main.parquet", "descriptions.parquet", "catalogs.parquet", "geo_info.parquet"]
RAW_DATA_FOLDER = "raw_data"
DOWNLOAD_TEMP = "tmp_download"
RAW_MAIN = "main.parquet"
RAW_DESCRIPTIONS = "descriptions.parquet"
DAILY_ADS = 'ads.parquet'
DAILY_DESCRIPTIONS = 'descriptions.parquet'

# Load MinIO credentials from .env file
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
RAW_FILES_CARS = os.getenv("RAW_FILES_CARS")
RAW_DAILY_VEHICLE_ADS=os.getenv("RAW_DAILY_VEHICLE_ADS")

# Initialize S3 client
S3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(s3={"payload_signing_enabled": False, "checksum_validation": False})
)

# Avoid checksum validation issues for reading from MinIO
STORAGE_OPT = {
    "key": ACCESS_KEY,
    "secret": SECRET_KEY,
    "client_kwargs": {
        "endpoint_url": MINIO_ENDPOINT
    }
}

# Other settings
# Column to sort by when preparing the raw data
SORT_COLUMN = "date"

# Helping column for parsed date
HELPING_TIME_COLUMN = "parsed_date"

# Encoding for reading parquet files
ENCODING = "utf-8-sig"