import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

POSTGRES_URL = os.getenv("POSTGRES_URL")
MYSQL_URL = os.getenv("MYSQL_URL")

XLSX = os.environ.get("SYNTHETIC_STORE_XLSX", "synthetic_store.xlsx")

# mapping sheet -> (pg schema, pg table, mysql table)
TABLES = {
    "Orders": ("sales", "orders", "orders"),
    "Regional Managers": ("ref", "regional_managers", "regional_managers"),
    "Returns": ("ref", "returns", "returns"),
    "State_Managers": ("ref", "state_managers", "state_managers"),
    "Segment_Managers": ("ref", "segment_managers", "segment_managers"),
    "Category_Managers": ("ref", "category_managers", "category_managers"),
    "Customer_Succces_Managers": ("ref", "customer_succces_managers", "customer_succces_managers"),
}

def ensure_postgres_schemas(pg):
    for schema in {"sales","ref"}:
        pg.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

def load():
    pg = create_engine(POSTGRES_URL, future=True).connect()
    my = create_engine(MYSQL_URL, future=True).connect()

    # run DDLs
    with open(os.path.join(os.path.dirname(__file__), "ddl_postgres.sql")) as f:
        pg.execute(text(f.read()))
    with open(os.path.join(os.path.dirname(__file__), "ddl_mysql.sql")) as f:
        for stmt in f.read().split(";"):
            s = stmt.strip()
            if s:
                my.execute(text(s))

    ensure_postgres_schemas(pg)

    xls = pd.ExcelFile(XLSX)
    for sheet, (pg_schema, pg_table, my_table) in TABLES.items():
        df = pd.read_excel(xls, sheet_name=sheet, dtype=str).fillna("")
        # normalize column names as-is (quoted identifiers used in SQL)
        # write to both databases
        df.to_sql(pg_table, pg, schema=pg_schema, if_exists="append", index=False)
        df.to_sql(my_table, my, if_exists="append", index=False)
        print(f"Loaded {sheet}: {len(df)} rows")

    pg.close(); my.close()

if __name__ == "__main__":
    load()

