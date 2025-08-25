import os
from dotenv import load_dotenv
load_dotenv()

# Load PostgreSQL credentials from .env file
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
DATABASE = os.getenv("DATABASE")
SCHEMA_DEV = os.getenv("SCHEMA_DEV")
SCHEMA_STAGING = os.getenv("SCHEMA_STAGING")

# Connection settings (can also load from .env)
DB_CONFIG = {
    "host": HOST,
    "port": PORT,
    "user": USER,
    "password": PASSWORD,
    "database": DATABASE
}

# Configs for daily_vehicle_ads table
DAILY_LOG_TABLE_NAME = "daily_vehicle_load_log"
DAILY_LOG_TABLE_COLUMNS = {
        "id": "SERIAL PRIMARY KEY",
        "uuid": "TEXT",
        "date": "TIMESTAMP",
        "processing_day": "DATE",
        "status": "TEXT",
        "records_received": "INTEGER",
        "records_inserted": "INTEGER",
        "inserted_at": "TIMESTAMP",
        "updated_at": "TIMESTAMP",
    }

UPDATED_STATUS = "processed_successfully"

# Configs for staging area for daily loads
STAGING_TABLE_ADS = "staging_daily_vehicle_ads"
STAGING_TABLE_ADS_COLUMNS = {
    "id": "TEXT PRIMARY KEY",
    "make": "TEXT"
}

STAGING_TABLE_DESCRIPTIONS = "staging_daily_descriptions"
STAGING_TABLE_DESCRIPTIONS_COLUMNS = {
    "row_id": "TEXT PRIMARY KEY",
    "id": "TEXT",
    "description": "TEXT"
}   