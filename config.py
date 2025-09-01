DB_URLS = {
    "mssql": (
        "mssql+pyodbc://sa:root123@VESSWILLIAM/BoredomDb"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes"
        "&TrustServerCertificate=yes"
    ),
    "postgres": "postgresql+psycopg2://postgres:root123@localhost:5433/checkin_db?sslmode=disable",
}


CHOSEN_DB = "mssql"


FILTER_COLUMN = "AccountId"
FILTER_VALUE = 8

EXCEPTION_LIST = [
    '"public"."schema_migrations"'
]

PRINT_ONLY = True
