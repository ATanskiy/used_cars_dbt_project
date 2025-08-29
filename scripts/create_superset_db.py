from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from configs.configs_dbs import DB_SUPERUSER

def create_superset_metadata():
    # Connect to Postgres as superuser
    conn_str = (
        f"postgresql://{DB_SUPERUSER['user']}:{DB_SUPERUSER['password']}"
        f"@{DB_SUPERUSER['host']}:{DB_SUPERUSER['port']}/{DB_SUPERUSER['database']}"
    )
    engine = create_engine(conn_str, future=True)

    with engine.connect() as conn:
        try:
            conn.execute(text("CREATE USER superset WITH PASSWORD 'superset';"))
            print("✅ Executed: CREATE USER")
        except SQLAlchemyError as e:
            print(f"⚠️ Skipped CREATE USER (maybe exists): {e}")

    # Connect to the existing superset DB
    superset_engine = create_engine(
        f"postgresql://superset:superset@{DB_SUPERUSER['host']}:{DB_SUPERUSER['port']}/superset",
        future=True
    )
    with superset_engine.connect() as conn:
        try:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS superset AUTHORIZATION superset;"))
            conn.execute(text("ALTER USER superset SET search_path TO superset;"))
            print("✅ Schema created and search_path set")
        except SQLAlchemyError as e:
            print(f"⚠️ Schema step skipped: {e}")

if __name__ == "__main__":
    create_superset_metadata()
