import pandas as pd
from datetime import datetime

def replace_empty_with_na(df):
    # replace empty strings and common null representations with pd.NA
    return df.replace(["", " ", "  ", "nan", "NaN", "None"], pd.NA)

def filter_by_start_date(df, column):
    start = "2022-01-01"
    start_date = pd.to_datetime(start).date()
    return df[df[column] >= start_date]

def convert_to_datetime(df, column, helping_column):
    # Pre-clean
    df[helping_column] = df[column].replace(["", " ", "nan", "NaN", "None"], pd.NA).astype(str)

    # Split ISO-Z vs others
    mask_iso = df[helping_column].str.endswith("Z", na=False)

    # Parse ISO-Z in vectorized way
    if mask_iso.any():
        df.loc[mask_iso, helping_column] = pd.to_datetime(
            df.loc[mask_iso, helping_column],
            format="%Y-%m-%dT%H:%M:%SZ",  # explicit, faster than infer
            errors="coerce",
            utc=True
        ).dt.tz_convert(None)  # strip tz

    # Parse the rest with dayfirst
    if (~mask_iso).any():
        df.loc[~mask_iso, helping_column] = pd.to_datetime(
            df.loc[~mask_iso, helping_column],
            errors="coerce",
            dayfirst=True
        )

    # Ensure dtype is datetime64[ns]
    df[helping_column] = pd.to_datetime(df[helping_column], errors="coerce")

    # 👉 Convert to date only
    df[helping_column] = df[helping_column].dt.date
    df[column] = df[helping_column]
    # Debug: show only if failures exist
    failed = df.loc[df[helping_column].isna(), column].dropna().unique()
    if len(failed) > 0:
        print(f"⚠️ Failed to parse {len(failed)} unique values in {column}. Examples: {failed[:10]}")

    return df