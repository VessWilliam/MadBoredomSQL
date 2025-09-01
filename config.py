DB_URLS = {
    "mssql": (
        "mssql+pyodbc://sa:root123@VESSWILLIAM/BoredomDb"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes"
        "&TrustServerCertificate=yes"
    ),
    "postgres": "postgresql+psycopg2://user:password@localhost:5432/mydb",
}


CHOSEN_DB= "mssql"


FILTER_COLUMN = "AccountId"
FILTER_VALUE = 8

EXCEPTION_LIST = [
    "[dbo].[EntityRole]"
]

PRINT_ONLY = False
