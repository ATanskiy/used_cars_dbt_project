from configs.configs import RAW_FILES_CARS, RAW_DAILY_VEHICLE_ADS,\
    RAW_MAIN, RAW_DESCRIPTIONS, DAILY_ADS, DAILY_DESCRIPTIONS
from configs.configs_dbs import SCHEMA_DEV, DAILY_LOG_TABLE_NAME, UPDATED_STATUS
from functions.bucket_funcs import ensure_bucket_exists, read_parquet_from_s3, save_df_to_s3
from functions.prepare_daily_ads import prepare_daily_ads
from functions.prepare_daily_descriptions import prepare_daily_descriptions
from db_utils.functions import update_row_status

def run_daily_load():
    # Ensure bucket exists
    ensure_bucket_exists(bucket=RAW_DAILY_VEHICLE_ADS)

    # Load raw data
    raw_ads = read_parquet_from_s3(bucket=RAW_FILES_CARS, key=RAW_MAIN)
    raw_descriptions = read_parquet_from_s3(bucket=RAW_FILES_CARS, key=RAW_DESCRIPTIONS)

    # Save raw data to the daily vehicle ads bucket
    daily_raw_ads, run_id, date = prepare_daily_ads(df=raw_ads)
    daily_raw_descriptions = \
        prepare_daily_descriptions(descriptions=raw_descriptions, ads=daily_raw_ads)

    # Save partitioned by date
    save_df_to_s3(df=daily_raw_ads, bucket=RAW_DAILY_VEHICLE_ADS,
                  date=date, filename=DAILY_ADS)
    
    save_df_to_s3(df=daily_raw_descriptions, bucket=RAW_DAILY_VEHICLE_ADS, 
                  date=date, filename=DAILY_DESCRIPTIONS)

    update_row_status(table_name=DAILY_LOG_TABLE_NAME, row_id=run_id, 
                      new_status=UPDATED_STATUS, schema=SCHEMA_DEV)

if __name__ == "__main__":
    run_daily_load()
    print("Bucket check and creation process completed successfully.")