import os
import pandas as pd
from sqlalchemy import create_engine, text
import config


def get_engine():
    """Create a database engine from config."""
    db_choice = config.CHOSEN_DB
    db_link = config.DB_URLS.get(db_choice)
    if not db_link:
        raise ValueError(f"Unknown DB choice: {db_choice}")
    print(f"Using database: {db_choice}")
    return create_engine(db_link)


def format_table(schema, table, db_choice):
    """Format schema.table based on database type."""
    match db_choice:
        case "mssql":
            return f"[{schema}].[{table}]"
        case "postgres":
            return f'"{schema}"."{table}"'
        case _:
            return f"{schema}.{table}"


def get_tables(engine):
    """Fetch all user tables, excluding system schemas."""
    db_choice = config.CHOSEN_DB
    system_schemas = {
        "postgres": ["pg_catalog", "information_schema"],
        "mssql": ["sys", "INFORMATION_SCHEMA"]
    }
    exclude_schemas = system_schemas.get(db_choice, [])

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
        """))
        return [
            (row[0], row[1])
            for row in result
            if row[0] not in exclude_schemas
        ]


def has_account_id(engine, schema, table):
    """Check if table has the configured any in config.py column."""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 1
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :SCHEMA
              AND TABLE_NAME = :TABLE
              AND COLUMN_NAME = :COLUMN
        """), {"SCHEMA": schema, "TABLE": table, "COLUMN": config.FILTER_COLUMN})
        return result.scalar() is not None


def get_row_count(engine, full_table):
    """Return row count for a given table, or None if fails."""
    try:
        with engine.connect() as conn:
            return conn.execute(text(f"SELECT COUNT(*) FROM {full_table}")).scalar()
    except Exception as e:
        print(f"Failed to count rows for {full_table}: {e}")
        return None


def execute_or_print(engine, query):
    """Run or print a query depending on config.PRINT_ONLY."""
    action = "[PRINT ONLY]" if config.PRINT_ONLY else "[EXECUTE]"
    print(f"{action} {query}")
    if not config.PRINT_ONLY:
        with engine.begin() as conn:
            conn.execute(text(query))


def generate_excel_report(records, folder="reports", filename="table_actions_report.xlsx"):
    """Generate an Excel report with results."""
    if not records:
        print("No records to write to Excel.")
        return

    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    reports_path = os.path.join(root_dir, folder)
    os.makedirs(reports_path, exist_ok=True)

    file_path = os.path.join(reports_path, filename)
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
        except PermissionError:
            print(f"Cannot overwrite {file_path}. Make sure it is closed.")
            return

    pd.DataFrame(records).to_excel(file_path, index=False)
    print(f"Excel report generated: {file_path}")


def main():
    engine = get_engine()
    tables = get_tables(engine)
    results = []

    for schema, table in tables:
        full_table = format_table(schema, table, config.CHOSEN_DB)

        # Skip exceptions
        if full_table in config.EXCEPTION_LIST:
            print(f"Skipping {full_table} (exception list)")
            results.append({
                "Table": full_table,
                "Action": "SKIPPED",
                "HasAccountId": None,
                "RowCount": None
            })
            continue

        row_count = get_row_count(engine, full_table)

        try:
            if has_account_id(engine, schema, table):
                query = (
                    f"DELETE FROM {full_table} "
                    f"WHERE {config.FILTER_COLUMN} <> {config.FILTER_VALUE} "
                    f"OR {config.FILTER_COLUMN} IS NULL"
                )
                execute_or_print(engine, query)
                action_taken, has_acc = "DELETE FILTERED", True
            else:
                match config.CHOSEN_DB:
                    case "mssql":
                        query = f"TRUNCATE TABLE {full_table}"
                    case "postgres":
                        query = f"TRUNCATE TABLE {full_table} CASCADE"
                execute_or_print(engine, query)
                action_taken, has_acc = "TRUNCATED", False

            results.append({
                "Table": full_table,
                "Action": action_taken,
                "HasAccountId": has_acc,
                "RowCount": row_count
            })

        except Exception as e:
            print(f"Error processing {full_table}: {e}")
            results.append({
                "Table": full_table,
                "Action": f"ERROR: {e}",
                "HasAccountId": None,
                "RowCount": row_count
            })

    generate_excel_report(results)


if __name__ == "__main__":
    main()
