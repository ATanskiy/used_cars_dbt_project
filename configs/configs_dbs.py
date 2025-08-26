import os
from dotenv import load_dotenv
load_dotenv()

# Detect if running inside Docker
IN_DOCKER = os.path.exists("/.dockerenv")

# Load PostgreSQL credentials from .env file
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
DATABASE = os.getenv("DATABASE")
SCHEMA_DEV = os.getenv("SCHEMA_DEV")
SCHEMA_STAGING = os.getenv("SCHEMA_STAGING")

# Override host/port when running inside Docker
if IN_DOCKER:
    HOST = "postgres"
    PORT = "5432"

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
STAGING_TABLE_ADS = "staging_daily_ads"
STAGING_TABLE_ADS_COLUMNS = {
    "row_id": "SERIAL PRIMARY KEY",
    "cost": "TEXT",
    "currency": "TEXT",
    "marka": "TEXT",
    "model": "TEXT",
    "year": "TEXT",
    "has_license": "TEXT",
    "place": "TEXT",
    "date": "TIMESTAMP",
    "id": "TEXT",
    "engine": "TEXT",
    "power": "TEXT",
    "gear": "TEXT",
    "probeg": "TEXT",
    "swheel": "TEXT",
    "complectation": "TEXT",
    "transmission": "TEXT",
    "r": "TEXT",
    "g": "TEXT",
    "b": "TEXT",
    "hex_color": "TEXT",
    "color": "TEXT",
    "inserted_at": "TIMESTAMP",
    "updated_at": "TIMESTAMP"
}

STAGING_TABLE_DESCRIPTIONS = "staging_daily_descriptions"
STAGING_TABLE_DESCRIPTIONS_COLUMNS = {
    "row_id": "SERIAL PRIMARY KEY",
    "id": "TEXT",
    "description": "TEXT",
    "inserted_at": "TIMESTAMP",
    "updated_at": "TIMESTAMP"
}   