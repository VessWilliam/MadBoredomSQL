
from sqlalchemy import create_engine, text
import config


def get_engine():
    db_choice = config.CHOSEN_DB
    db_link = config.DB_URLS.get(db_choice)
    if not db_link:
        raise ValueError(f"Unknown DB choice : {db_choice}")
    print(f"Using db : {db_choice}")
    return create_engine(db_link)


def format_table(schema, table, db_choice):
    """Quote table names depending on database type."""

    match db_choice:
        case "mssql":
            return f"[{schema}].[{table}]"
        case "postgres":
            return f'"{schema}"."{table}"'
        case _:
            return f"{schema}.{table}"


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
            AND COLUMN_NAME = :COLUMN                          
                                   
        """), {"SCHEMA": schema, "TABLE": table, "COLUMN": config.FILTER_COLUMN})
        return result.scalar() is not None


def execute_or_print(engine, query):
    """Centralized print vs execute handling."""
    action = "[PRINT ONLY]" if config.PRINT_ONLY else "[EXECUTE]"
    print(f"{action} {query}")
    if not config.PRINT_ONLY:
        with engine.begin() as conn:
            conn.execute(text(query))


def main():
    engine = get_engine()

    tables = get_tables(engine)

    for schema, table in tables:

        full_table = format_table(schema, table, config.CHOSEN_DB)

        if full_table in config.EXCEPTION_LIST:
            print(f"Skipping {full_table} (exception list)")
            continue

        try:

            if has_account_id(engine, schema, table):
                query = (
                    f"DELETE FROM {full_table} "
                    f"WHERE {config.FILTER_COLUMN} <> {config.FILTER_VALUE} "
                    f"OR {config.FILTER_COLUMN} IS NULL"
                )
                execute_or_print(engine, query)
            else:

                match config.CHOSEN_DB:
                    case "mssql":
                        query = f"TRUNCATE TABLE {full_table}"

                    case "postgres":
                        query = f"TRUNCATE TABLE {full_table} CASCADE"

                execute_or_print(engine, query)

        except Exception as e:
            print(f"Error Processing {full_table}: {e}")


if __name__ == "__main__":
    main()
