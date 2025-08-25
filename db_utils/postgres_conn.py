from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
from configs.configs_dbs import DB_CONFIG

def get_engine() -> Engine:
    """
    Create and return a SQLAlchemy engine for Postgres.
    """
    conn_str = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(conn_str, pool_pre_ping=True, echo=False, future=True)

@contextmanager
def get_connection():
    """
    Context manager for a DB connection, sets search_path to schema.
    """
    engine = get_engine()
    conn = engine.connect()
    try:
        # set schema for the session
        conn.execute(text(f"SET search_path TO {DB_CONFIG['schema']}"))
        yield conn
    except SQLAlchemyError as e:
        print(f"❌ DB error: {e}")
        raise
    finally:
        conn.close()