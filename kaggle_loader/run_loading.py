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
from configs.configs import RAW_FILES_CARS, RAW_FILE_NAMES_LIST_TARGET, RAW_DATA_FOLDER,\
                     RAW_FILE_NAMES_LIST
from functions.bucket_funcs import ensure_bucket_exists, files_exist_in_bucket,\
                    local_files_exist, upload_local_files
from functions.convert_csv_to_parquet import convert_csv_files_to_parquet
from download_from_kaggle import download_kaggle_files

def run_loading():
    ensure_bucket_exists(RAW_FILES_CARS)

    if files_exist_in_bucket(RAW_FILES_CARS, RAW_FILE_NAMES_LIST_TARGET):
        print("The files already exist in S3. Nothing to do.")
        return

    if local_files_exist(RAW_DATA_FOLDER, RAW_FILE_NAMES_LIST_TARGET):
        upload_local_files(RAW_DATA_FOLDER, RAW_FILE_NAMES_LIST_TARGET)
        return

    if local_files_exist(RAW_DATA_FOLDER, RAW_FILE_NAMES_LIST):
        print("Local csv files found. Converting to parquet...")
        convert_csv_files_to_parquet(RAW_DATA_FOLDER, RAW_FILE_NAMES_LIST, RAW_FILE_NAMES_LIST_TARGET)
        print("Uploading to S3...")
        upload_local_files(RAW_DATA_FOLDER, RAW_FILE_NAMES_LIST)
        return

    else:
        download_kaggle_files()
        convert_csv_files_to_parquet(RAW_DATA_FOLDER, RAW_FILE_NAMES_LIST, RAW_FILE_NAMES_LIST_TARGET)
        upload_local_files(RAW_DATA_FOLDER, RAW_FILE_NAMES_LIST_TARGET)
        return
    

if __name__ == "__main__":
    run_loading()
    print("Download and upload process completed successfully.")