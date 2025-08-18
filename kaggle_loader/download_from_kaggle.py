"""
Kaggle Dataset Downloader, Cleaner, and Uploader to MinIO/S3

This module automates the retrieval of a specific dataset from Kaggle, 
prepares it for analysis by cleaning and sorting, and then uploads the 
processed file to an S3-compatible storage bucket (such as MinIO).

Workflow:
1. Check if the raw CSV (`RAW_FILE_NAME`) exists locally in `RAW_DATA_FOLDER`.
   - If yes: use it directly.
   - If no: authenticate with the Kaggle API and download the dataset 
     specified by `DATASET` into `DOWNLOAD_TEMP`, unzipping automatically.
2. Read the CSV into a Pandas DataFrame.
3. Validate the presence of the sorting column (`SORT_COLUMN`) in the data.
4. Convert the sorting column to UTC datetime, dropping any rows with 
   invalid or missing values.
5. Sort the data ascending by the sorting column.
6. Save the cleaned data as a Parquet file (Snappy compression) in 
   `RAW_DATA_FOLDER` with the same base name as the CSV (`RAW_FILE_NAME_TARGET`).
7. Upload the Parquet file to the `RAW_FILES_CARS` bucket in S3/MinIO using 
   the `upload_file_to_s3` function from `bucket_funcs`.
8. Remove the local CSV file after processing, and delete the `DOWNLOAD_TEMP` 
   directory if the CSV was downloaded from Kaggle.
"""

import os
import shutil

# Tells the Kaggle API where to find Kaggle credentials (kaggle.json).
os.environ['KAGGLE_CONFIG_DIR'] = os.path.join(os.path.dirname(__file__), '../.kaggle')

from kaggle.api.kaggle_api_extended import KaggleApi
from configs.config import RAW_FILE_NAME, RAW_DATA_FOLDER, RAW_FILE_NAME_TARGET,\
                    DOWNLOAD_TEMP, DATASET, RAW_FILES_CARS, SORT_COLUMN
from functions.bucket_funcs import upload_file_to_s3
from prepare_file import prepare_file


def download_and_prepare_kaggle_files():

    os.makedirs(RAW_DATA_FOLDER, exist_ok=True)

    local_csv = os.path.join(RAW_DATA_FOLDER, RAW_FILE_NAME)
    raw_parquet = os.path.join(RAW_DATA_FOLDER, RAW_FILE_NAME_TARGET)

    downloaded_from_kaggle = False
    source_csv = None

    if os.path.exists(local_csv):
        print(f"Found local CSV at '{local_csv}'. Will transform without downloading.")
        source_csv = local_csv
    else:
        print("Local CSV not found. Downloading from Kaggle...")
        os.makedirs(DOWNLOAD_TEMP, exist_ok=True)
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(DATASET, path=DOWNLOAD_TEMP, unzip=True)

        temp_csv = os.path.join(DOWNLOAD_TEMP, RAW_FILE_NAME)
        if not os.path.exists(temp_csv):
            raise FileNotFoundError(f"Expected CSV not found at: {temp_csv}")
        downloaded_from_kaggle = True
        source_csv = temp_csv

    print("Opening the file, cleaning by 'posting_date', sorting, and saving as Parquet...")

    prepare_file(source_csv, raw_parquet, SORT_COLUMN)

    # Upload Parquet to S3/MinIO
    upload_file_to_s3(raw_parquet, RAW_FILES_CARS, RAW_FILE_NAME_TARGET)
    print(f"Uploaded '{RAW_FILE_NAME_TARGET}' to bucket '{RAW_FILES_CARS}'.")

    # Delete the CSV (local or downloaded) and cleanup temp dir if used
    try:
        os.remove(source_csv)
        print(f"Deleted source CSV '{source_csv}'.")
    except FileNotFoundError:
        pass

    if downloaded_from_kaggle:
        shutil.rmtree(DOWNLOAD_TEMP, ignore_errors=True)
        print("Cleaned up temporary Kaggle download directory.")