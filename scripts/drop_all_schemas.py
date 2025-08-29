from sqlalchemy import text
from db_utils.postgres_conn import get_engine

def drop_all_tables_in_schemas(schemas: list[str]):
    engine = get_engine()
    with engine.begin() as conn:
        for schema in schemas:
            print(f"🔍 Checking schema: {schema}")
            
            # Get all table names
            result = conn.execute(text(f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = :schema
                  AND table_type = 'BASE TABLE'
            """), {"schema": schema})

            tables = [row[0] for row in result]

            if not tables:
                print(f"ℹ️ No tables found in schema {schema}")
                continue

            # Drop each table
            for table in tables:
                print(f"🗑 Dropping {schema}.{table}")
                conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE'))

            print(f"✅ Dropped all tables in schema: {schema}")


if __name__ == "__main__":
    # Example usage
    schemas_to_clean = ["staging", "dev"]   # 👈 put schemas here
    drop_all_tables_in_schemas(schemas_to_clean)