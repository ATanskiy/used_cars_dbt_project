"""
    Read a CSV file, clean and sort it by a specified date column, and save as a Parquet file.

    Steps performed:
    1. Read the source CSV file into a Pandas DataFrame.
    2. Validate that the specified `SORT_COLUMN` exists in the DataFrame.
    3. Convert the `SORT_COLUMN` values to UTC datetime, coercing errors to NaT.
    4. Drop any rows with missing or invalid values in `SORT_COLUMN`.
    5. Sort the DataFrame in ascending order by `SORT_COLUMN`.
    6. Save the cleaned, sorted data to a Parquet file with Snappy compression.
"""

import pandas as pd

def prepare_file(source_csv, raw_parquet, SORT_COLUMN):

    # 1) Read CSV
    df = pd.read_csv(source_csv)

    # 2) Ensure posting_date exists
    if SORT_COLUMN not in df.columns:
        raise ValueError(f"'posting_date' column not found. Available columns: {list(df.columns)}")

    # 3) Coerce, drop missing/invalid
    df["posting_date"] = pd.to_datetime(df["posting_date"], errors="coerce", utc=True)
    before = len(df)
    df = df.dropna(subset=["posting_date"])
    dropped = before - len(df)
    print(f"Dropped {dropped} rows with missing/invalid posting_date.")

    # 4) Sort ascending
    df = df.sort_values("posting_date").reset_index(drop=True)

    # 5) Save as Parquet (snappy), no index
    df.to_parquet(raw_parquet, index=False, engine="pyarrow", compression="snappy")
    print(f"Saved cleaned Parquet to '{raw_parquet}'.")