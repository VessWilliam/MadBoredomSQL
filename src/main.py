
from sqlalchemy import create_engine, text, inspect
import config


def get_tables(engine):
    """"Fetch All Base Table From Information_Scema.Tables."""
    with engine.connect() as conn:
        result = conn.execute(text("""
         SELECT TABLE_SCHEMA, TABLE_NAME
         FROM  INFORMATION_SCHEMA.TABLES
         WHERE TABLE_TYPE = 'BASE TABLE'
         ORDER BY TABLE_SCHEMA, TABLE_NAME                          
        """))
        return [(row[0], row[1]) for row in result]


def has_account_id(engine, schema, table):
    """Check If Table Has An AccountId Column"""
    with engine.connect() as conn:
        result = conn.execute(text("""
          SELECT 1 
          FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = :SCHEMA
            AND TABLE_NAME = :TABLE
            AND COLUMN_NAME = 'AccountId'                          
                                   
        """), {"SCHEMA": schema, "TABLE": table})
        return result.scalar() is not None


def main():
    engine = create_engine(config.DB_URL)

    tables = get_tables(engine)

    for schema, table in tables:

        full_table = f"[{schema}].[{table}]"

        if full_table in config.EXCEPTION_LIST:
            print(f"Skipping {full_table} (exception list)")
            continue

        try:

            if has_account_id(engine, schema, table):
                query = f"""DELETE FROM {full_table} WHERE AccountId <> 
              {config.ACCOUNT_ID_TO_KEEP} OR AccountId IS NULL"""

                if config.PRINT_ONLY is False:
                    print(f"[EXECUTE] {query}")
                    with engine.begin() as conn:
                        conn.execute(text(query))

                print(f"[PRINT ONLY] {query}")

            else:
                query = f"TRUNCATE TABLE {full_table}"

                if config.PRINT_ONLY is False:
                    print(f"[EXECUTE] {query}")
                    with engine.begin() as conn:
                        conn.execute(text(query))

                print(f"[PRINT ONLY] {query}")

        except Exception as e:
            print(f"Error Processing {full_table}: {e}")


if __name__ == "__main__":
    main()
