"""
S3/MinIO Utility Functions for File Management

This module provides helper functions to interact with an S3-compatible storage 
service (such as AWS S3 or MinIO) using a preconfigured boto3 client. It is 
designed to support data ingestion pipelines by handling bucket existence checks, 
file existence checks, and file uploads.

Functions included:
- bucket_exists(bucket): Check if the given bucket exists in S3.
- file_exists_in_bucket(bucket, key): Check if a specific object exists in the bucket.
- upload_file_to_s3(local_path, bucket, key): Upload a local file to a specified S3 bucket/key.
- ensure_bucket_exists(bucket): Create the bucket if it does not already exist.
- local_target_file_exist(): Check if the processed (target) file exists locally.
- local_raw_file_exist(): Check if the raw (source) file exists locally.
- upload_local_files(): Upload the processed (target) file from local storage to S3.
"""

import os
import pandas as pd
from botocore.exceptions import ClientError
from configs.configs import S3, RAW_FILES_CARS, STORAGE_OPT
def bucket_exists(bucket):
    try:
        S3.head_bucket(Bucket=bucket)
        return True
    except ClientError:
        return False

def file_exists_in_bucket(bucket, key):
    try:
        S3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False
    
def files_exist_in_bucket(bucket, file_names):
    for file_name in file_names:
        if not file_exists_in_bucket(bucket, file_name):
            return False
    return True

def upload_file_to_s3(local_path, bucket, key):
    print(f"Uploading {local_path} to {bucket}/{key}...")
    S3.upload_file(local_path, bucket, key)
    print(f"Uploaded {key}")

def ensure_bucket_exists(bucket):
    if not bucket or not isinstance(bucket, str):
        raise ValueError("Bucket name is missing or invalid in config.")
    if not bucket_exists(bucket):
        print(f"Creating bucket '{bucket}'...")
        S3.create_bucket(Bucket=bucket)
    else:
        print(f"Bucket '{bucket}' already exists.")

def local_file_exists(folder, file_name):
    cars_path = os.path.join(folder, file_name)
    return os.path.exists(cars_path)

def local_files_exist(folder, file_names):
    for file_name in file_names:
        if not local_file_exists(folder, file_name):
            return False
    return True

def upload_local_file(folder, file_name):
    print("Found the file in 'raw_data'. Uploading to S3...")
    cars_path = os.path.join(folder, file_name)
    upload_file_to_s3(cars_path, RAW_FILES_CARS, file_name)

def upload_local_files(folder, file_names):
    for file_name in file_names:
        upload_local_file(folder, file_name)
    print("All local files uploaded successfully.")

def read_parquet_from_s3(bucket: str, key: str):
    return pd.read_parquet(f"s3://{bucket}/{key}", storage_options=STORAGE_OPT)

def make_s3_path(base_bucket: str, date):
    year = date.year
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"
    return f"{base_bucket}/year={year}/month={month}/day={day}/"

def save_df_to_s3(df, bucket: str, date, filename: str):
    df = df.drop(columns=["year", "month", "day"], errors="ignore")
    path = make_s3_path(bucket, date) + filename
    df.to_parquet(
        f"s3://{path}",
        engine="pyarrow",
        index=False,
        storage_options=STORAGE_OPT
    )
    print(f"✅ Saved {filename} → {path}")

def read_partitioned_file_from_s3(bucket: str, date, filename: str):
    path = make_s3_path(bucket, date) + filename
    df = pd.read_parquet(
        f"s3://{path}",
        engine="pyarrow",
        storage_options=STORAGE_OPT
    )
    print(f"📥 Loaded {filename} ← {path} ({len(df):,} rows)")
    return df