import os
import pandas as pd
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
    match db_choice:
        case "mssql":
            return f"[{schema}].[{table}]"
        case "postgres":
            return f'"{schema}"."{table}"'
        case _:
            return f"{schema}.{table}"


def get_tables(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
         SELECT TABLE_SCHEMA, TABLE_NAME
         FROM INFORMATION_SCHEMA.TABLES
         WHERE TABLE_TYPE = 'BASE TABLE'
         ORDER BY TABLE_SCHEMA, TABLE_NAME
        """))
        return [(row[0], row[1]) for row in result]


def has_account_id(engine, schema, table):
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
    action = "[PRINT ONLY]" if config.PRINT_ONLY else "[EXECUTE]"
    print(f"{action} {query}")
    if not config.PRINT_ONLY:
        with engine.begin() as conn:
            conn.execute(text(query))


def generate_excel_report(records, folder="reports", filename="table_actions_report.xlsx"):
    """
    Generate an Excel report and save it in the specified folder at the project root.
    """
    if not records:
        print("No records to write to Excel.")
        return

    script_dir = os.path.dirname(os.path.abspath(__file__))
    print("dir", script_dir)
    root_dir = os.path.dirname(script_dir)
    reports_path = os.path.join(root_dir, folder)

    os.makedirs(reports_path, exist_ok=True)

    file_path = os.path.join(reports_path, filename)
    df = pd.DataFrame(records)
    df.to_excel(file_path, index=False)
    print(f"Excel report generated: {file_path}")


def main():
    engine = get_engine()
    tables = get_tables(engine)

    excel_records = []

    for schema, table in tables:
        full_table = format_table(schema, table, config.CHOSEN_DB)

        if full_table in config.EXCEPTION_LIST:
            print(f"Skipping {full_table} (exception list)")
            excel_records.append({
                "Table": full_table,
                "Action": "SKIPPED",
                "HasAccountId": None
            })
            continue

        try:
            if has_account_id(engine, schema, table):
                action_taken = f"DELETE WHERE {config.FILTER_COLUMN} <> {config.FILTER_VALUE} OR IS NULL"
                query = (
                    f"DELETE FROM {full_table} "
                    f"WHERE {config.FILTER_COLUMN} <> {config.FILTER_VALUE} "
                    f"OR {config.FILTER_COLUMN} IS NULL"
                )
                execute_or_print(engine, query)
                has_acc = True
            else:
                match config.CHOSEN_DB:
                    case "mssql":
                        query = f"TRUNCATE TABLE {full_table}"
                    case "postgres":
                        query = f"TRUNCATE TABLE {full_table} CASCADE"
                execute_or_print(engine, query)
                action_taken = "TRUNCATED"
                has_acc = False

            excel_records.append({
                "Table": full_table,
                "Action": action_taken,
                "HasAccountId": has_acc
            })

        except Exception as e:
            print(f"Error Processing {full_table}: {e}")
            excel_records.append({
                "Table": full_table,
                "Action": f"ERROR: {e}",
                "HasAccountId": None
            })

    # Generate Excel in the reports folder
    generate_excel_report(excel_records)


if __name__ == "__main__":
    main()
