from db_utils.postgres_conn import get_engine
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError
from configs.configs_dbs import DB_CONFIG
from configs.configs import DAILY_ADS, DAILY_DESCRIPTIONS
from functions.bucket_funcs import read_partitioned_file_from_s3
import pandas as pd

def table_exists(table_name: str, schema: str = None) -> bool:
    engine = get_engine()
    inspector = inspect(engine)
    schema_name = schema or DB_CONFIG["schema"]
    return inspector.has_table(table_name, schema=schema_name)

def create_schema_if_not_exists(schema_name: str):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
    print(f"✅ Schema {schema_name} is ready")

def create_table_if_not_exists(table_name: str, columns: dict, schema_name: str = None):
    engine = get_engine()

    inspector = inspect(engine)
    if inspector.has_table(table_name, schema=schema_name):
        print(f"ℹ️ Table {schema_name}.{table_name} already exists")
        return

    # Build CREATE TABLE statement
    col_defs = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
    sql = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ({col_defs});"

    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
        print(f"✅ Created table {schema_name}.{table_name}")
    except SQLAlchemyError as e:
        print(f"❌ Error creating table {schema_name}.{table_name}: {e}")
        raise

def insert_row(table_name: str, row: dict, schema: str = None):
    engine = get_engine()
    schema_name = schema or DB_CONFIG["schema"]

    # Build query dynamically
    columns = ", ".join(row.keys())
    placeholders = ", ".join([f":{col}" for col in row.keys()])  # named placeholders
    sql = text(f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({placeholders})")

    try:
        with engine.begin() as conn:
            conn.execute(sql, row)  # row dict matches placeholders
        print(f"✅ Inserted row into {schema_name}.{table_name}")
    except SQLAlchemyError as e:
        print(f"❌ Error inserting into {schema_name}.{table_name}: {e}")
        raise

def get_max_date(table_name: str, schema: str = None, date_col: str = "date"):
    engine = get_engine()
    schema_name = schema or DB_CONFIG["schema"]

    sql = text(f"SELECT MAX({date_col}) FROM {schema_name}.{table_name};")
    with engine.connect() as conn:
        result = conn.execute(sql).scalar()
    return result  # returns datetime or None

def update_row_status(table_name: str, row_id: str, new_status: str, schema: str):
    engine = get_engine()

    sql = text(f"""
        UPDATE {schema}.{table_name}
        SET status = :new_status, 
        updated_at = NOW()
        WHERE uuid = :row_id;
    """)

    try:
        with engine.begin() as conn:
            conn.execute(sql, {"new_status": new_status, "row_id": row_id})
        print(f"✅ Updated status of row {row_id} in {schema}.{table_name} to '{new_status}'")
    except SQLAlchemyError as e:
        print(f"❌ Error updating row {row_id} in {schema}.{table_name}: {e}")
        raise

def upload_file_to_stage(bucket: str, data_schema: dict, file_name: str,
                      schema_name: str, date: str, table_name: str):
    create_schema_if_not_exists(schema_name=schema_name)
    create_table_if_not_exists(table_name=table_name,
                               schema_name=schema_name, 
                               columns=data_schema)

    if file_name == DAILY_ADS:
        df = read_partitioned_file_from_s3(bucket=bucket, date=date, filename=file_name)
    else:
        df = read_partitioned_file_from_s3(bucket=bucket, date=date, filename=file_name)
    # Drop partition columns injected by Arrow
    df = df.drop(columns=["year", "month", "day"], errors="ignore")
    df.columns = [c.lower() for c in df.columns]
    df.index = range(1, len(df) + 1)
    now = pd.Timestamp.utcnow()
    df["inserted_at"] = now
    df["updated_at"] = now
    engine = get_engine()

    # Clean table before inserting
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {schema_name}.{table_name};"))

    # Insert fresh data
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema_name,
        if_exists="append",   # append into empty table
        index=True,
        index_label="row_id",
        method="multi",
        chunksize=5000
    )

    print(f"♻️ Refreshed {schema_name}.{table_name} with {len(df):,} rows")