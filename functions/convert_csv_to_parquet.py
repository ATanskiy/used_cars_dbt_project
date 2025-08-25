import os
import pandas as pd

def convert_csv_to_parquet(folder, csv_file, parquet_file):
    csv_path = f"{folder}/{csv_file}"

    SEP_MAP = {"catalogs.csv": ";"}
    sep = SEP_MAP.get(csv_file, ",")

    try:
        # let pandas sniff delimiter
        df = pd.read_csv(csv_path, encoding="utf-8-sig", engine="python", sep=sep)
        df = df.astype(str)
    except UnicodeDecodeError:
        print(f"⚠️ UTF-8 failed for {csv_file}, trying utf-8-sig...")

    parquet_path = f"{folder}/{parquet_file}"
    df.to_parquet(parquet_path, index=False, engine="pyarrow", compression="snappy")
    print(f"✅ Saved {parquet_file}")

    # delete original CSV after saving Parquet
    try:
        os.remove(csv_path)
        print(f"🗑️ Removed {csv_file}")
    except Exception as e:
        print(f"⚠️ Could not remove {csv_file}: {e}")

def convert_csv_files_to_parquet(folder, csv_files_list, parquet_files_list):
    if len(csv_files_list) != len(parquet_files_list):
        raise ValueError("The number of CSV files must match the number of Parquet files.")
    
    for csv_file, parquet_file in zip(csv_files_list, parquet_files_list):
        convert_csv_to_parquet(folder, csv_file, parquet_file)
    print("All CSV files converted to Parquet successfully.")