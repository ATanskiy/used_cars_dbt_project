import uuid
import pandas as pd
from datetime import datetime
from configs.configs_dbs import SCHEMA_DEV, DAILY_LOG_TABLE_NAME, DAILY_LOG_TABLE_COLUMNS
from configs.configs import SORT_COLUMN, HELPING_TIME_COLUMN
from functions.transform_ads import replace_empty_with_na,\
                                             convert_to_datetime
from db_utils.functions import table_exists, create_table_if_not_exists,\
                 create_schema_if_not_exists, get_max_date, insert_row
from functions.create_colors_ads import add_color_columns

def prepare_daily_ads(df):
    records_received = len(df)
    df = replace_empty_with_na(df)
    df = convert_to_datetime(df, SORT_COLUMN, HELPING_TIME_COLUMN)\
                .sort_values(HELPING_TIME_COLUMN, ascending=True)

    df_min_date = df[HELPING_TIME_COLUMN].dropna().min()
    run_id = str(uuid.uuid4())

    if not table_exists(DAILY_LOG_TABLE_NAME, schema=SCHEMA_DEV):
        create_schema_if_not_exists(SCHEMA_DEV)
        create_table_if_not_exists(table_name=DAILY_LOG_TABLE_NAME,
                                columns=DAILY_LOG_TABLE_COLUMNS,
                                schema_name=SCHEMA_DEV)
        processing_day = df_min_date
        print(f"Proessing first date: {df_min_date}")
        
    else:
        max_date = get_max_date(table_name=DAILY_LOG_TABLE_NAME,
                                schema=SCHEMA_DEV, 
                                date_col="processing_day")
        if max_date is not None:
            max_date = pd.to_datetime(max_date).date()
            next_dates = sorted(
                df[df[HELPING_TIME_COLUMN] > max_date][HELPING_TIME_COLUMN].unique()
            )
            if not next_dates:
                print("⚠️ No new dates to process")
                return None, None, None
            processing_day = next_dates[0]
            print(f"Processing date: {processing_day}")
        else:
            processing_day = df_min_date
            print(f"Fallback: processing earliest date {processing_day}")

    print("records_to_insert:", len(df[df[HELPING_TIME_COLUMN] == processing_day]))

    df = df[df[HELPING_TIME_COLUMN] == processing_day].drop(columns=[HELPING_TIME_COLUMN])
    df = add_color_columns(df)
    records_before_insertion = len(df)
    row = {
        "uuid": run_id,
        "date": datetime.utcnow(),
        "processing_day": processing_day,
        "status": "row_created",
        "records_received": records_received,
        "records_inserted": records_before_insertion,
        "inserted_at": pd.Timestamp.utcnow(),
        "updated_at": pd.Timestamp.utcnow(),
    }
    insert_row(DAILY_LOG_TABLE_NAME, row, schema=SCHEMA_DEV)

    return df, run_id, processing_day