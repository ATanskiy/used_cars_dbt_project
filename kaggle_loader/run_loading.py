"""
Main Orchestration Script: Ensure Kaggle Dataset is Uploaded to S3/MinIO

This module coordinates the end-to-end workflow of ensuring that a processed 
dataset is available in an S3-compatible storage bucket (e.g., MinIO).

Workflow:
1. Ensure the target bucket (`RAW_FILES_CARS`) exists in S3/MinIO. If it does 
   not exist, create it using `ensure_bucket_exists`.
2. Check if the processed target file (`RAW_FILE_NAME_TARGET`) already exists 
   in the bucket:
   - If yes: exit early; nothing to do.
   - If no: proceed to the next step.
3. Check if the processed target file exists locally (`local_target_file_exist`):
   - If yes: upload it directly to the bucket using `upload_local_files`.
   - If no: fetch and prepare the dataset from Kaggle using 
     `download_and_prepare_kaggle_files`, then upload it.
"""

from configs.config import RAW_FILE_NAME_TARGET, RAW_FILES_CARS, RAW_DATA_FOLDER
from functions.bucket_funcs import ensure_bucket_exists, file_exists_in_bucket,\
                    local_target_file_exists, upload_local_files
from download_from_kaggle import download_and_prepare_kaggle_files

def run_loading():
    ensure_bucket_exists(RAW_FILES_CARS)

    if file_exists_in_bucket(RAW_FILES_CARS, RAW_FILE_NAME_TARGET):
        print("The file already exists in S3. Nothing to do.")
        return

    if local_target_file_exists(RAW_DATA_FOLDER, RAW_FILE_NAME_TARGET):
        upload_local_files(RAW_DATA_FOLDER, RAW_FILE_NAME_TARGET)

    else:
        download_and_prepare_kaggle_files()

if __name__ == "__main__":
    run_loading()
    print("Download and upload process completed successfully.")