"""
Kaggle Dataset Downloader, Cleaner, and Uploader to MinIO/S3

This module automates the retrieval of a specific dataset from Kaggle.

Check if the raw CSV (`RAW_FILE_NAME`) exists locally in `RAW_DATA_FOLDER`.
- If yes: use it directly.
- If no: authenticate with the Kaggle API and download the dataset 
    specified by `DATASET` into `DOWNLOAD_TEMP`, unzipping automatically.
"""

import os
import shutil

# Tells the Kaggle API where to find Kaggle credentials (kaggle.json).
os.environ['KAGGLE_CONFIG_DIR'] = os.path.join(os.path.dirname(__file__), '../.kaggle')

import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi
from configs.configs import RAW_FILE_NAMES_LIST, RAW_DATA_FOLDER, DOWNLOAD_TEMP, DATASET

def download_kaggle_files():
    os.makedirs(RAW_DATA_FOLDER, exist_ok=True)
    os.makedirs(DOWNLOAD_TEMP, exist_ok=True)

    api = KaggleApi()
    api.authenticate()

    for RAW_FILE_NAME in RAW_FILE_NAMES_LIST:
        local_csv = os.path.join(RAW_DATA_FOLDER, RAW_FILE_NAME)
        if os.path.exists(local_csv):
            print(f"✅ Found local CSV at '{local_csv}'. Skipping download.")
            continue

        print(f"⬇️ Downloading {RAW_FILE_NAME} from Kaggle...")

        # this gives you <RAW_FILE_NAME>.zip
        api.dataset_download_file(DATASET, RAW_FILE_NAME, path=DOWNLOAD_TEMP)

        # possible paths after download
        zip_path = os.path.join(DOWNLOAD_TEMP, RAW_FILE_NAME + ".zip")
        raw_path = os.path.join(DOWNLOAD_TEMP, RAW_FILE_NAME)

        if os.path.exists(zip_path):
            # Case 1: Kaggle gave a zip
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(DOWNLOAD_TEMP)
            os.remove(zip_path)
            extracted_csv = raw_path

        elif os.path.exists(raw_path):
            # Case 2: Kaggle gave raw CSV
            extracted_csv = raw_path

        else:
            raise FileNotFoundError("❌ No downloaded file found!")

        # move to final folder
        shutil.move(extracted_csv, local_csv)
        print(f"✅ Saved {RAW_FILE_NAME} ({os.path.getsize(local_csv)/1024/1024:.2f} MB)")

    shutil.rmtree(DOWNLOAD_TEMP, ignore_errors=True)
    print("🧹 Cleaned up temp folder.")