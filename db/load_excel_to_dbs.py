import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import DATE, INTEGER, NUMERIC, TEXT
from dotenv import load_dotenv

load_dotenv()

POSTGRES_URL = os.getenv("POSTGRES_URL")
if not POSTGRES_URL:
    print("ERROR: POSTGRES_URL is not set in environment (e.g., postgresql+psycopg://user:pass@localhost:5432/synthetic_store)")
    sys.exit(1)

# You used this exact casing; keep it configurable:
XLSX = os.environ.get("SYNTHETIC_STORE_XLSX", "Synthetic_Store.xlsx")

# mapping sheet -> (pg schema, pg table)
TABLES = {
    "Orders": ("sales", "orders"),
    "Regional Managers": ("ref", "regional_managers"),
    "Returns": ("ref", "returns"),
    "State_Managers": ("ref", "state_managers"),
    "Segment_Managers": ("ref", "segment_managers"),
    "Category_Managers": ("ref", "category_managers"),
    "Customer_Success_Managers": ("ref", "customer_succces_managers"),
}

# Expected columns for each sheet (order matters)
EXPECTED = {
    "Orders": [
        "Row ID","Order ID","Order Date","Ship Date","Ship Mode","Customer ID","Customer Name",
        "Segment","Country/Region","City","State/Province","Postal Code","Region","Product ID",
        "Category","Sub-Category","Product Name","Sales","Quantity","Discount","Profit"
    ],
    "Regional Managers": ["Regional Manager","Regions"],
    "Returns": ["Returned","ID"],
    "State_Managers": ["State/Province","Manager"],
    "Segment_Managers": ["Segment","Manager"],
    "Category_Managers": ["Category","Manager"],
    "Customer_Success_Managers": ["Regions","Manager"],
}

# Type coercion hints for Orders
DATE_COLS = ["Order Date", "Ship Date"]
NUM_COLS  = ["Sales", "Discount", "Profit"]
INT_COLS  = ["Quantity"]

# DTYPE map for parameter binding (doesn't change existing table DDL; it helps inserts)
PG_DTYPE_ORDERS = {
    "Row ID": TEXT(),
    "Order ID": TEXT(),
    "Order Date": DATE(),
    "Ship Date": DATE(),
    "Ship Mode": TEXT(),
    "Customer ID": TEXT(),
    "Customer Name": TEXT(),
    "Segment": TEXT(),
    "Country/Region": TEXT(),
    "City": TEXT(),
    "State/Province": TEXT(),
    "Postal Code": TEXT(),
    "Region": TEXT(),
    "Product ID": TEXT(),
    "Category": TEXT(),
    "Sub-Category": TEXT(),
    "Product Name": TEXT(),
    "Sales": NUMERIC(12, 2),
    "Quantity": INTEGER(),
    "Discount": NUMERIC(6, 3),
    "Profit": NUMERIC(12, 2),
}

def ensure_postgres_schemas(conn):
    """Create required schemas if they don't exist."""
    for schema in {"sales", "ref"}:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

def coerce_types(df: pd.DataFrame, sheet: str) -> pd.DataFrame:
    """
    Clean and coerce types so inserts bind to DATE/NUMERIC/INTEGER correctly.
    Only the Orders sheet needs special treatment.
    """
    out = df.copy()

    # Trim column names and cell whitespace; normalize empties to None
    out.columns = [c.strip() for c in out.columns]
    for c in out.columns:
        if out[c].dtype == object:
            out[c] = (
                out[c]
                .astype(str)
                .str.strip()
                .replace({"": None, "nan": None, "NaT": None})
            )

    if sheet == "Orders":
        # Dates
        for c in DATE_COLS:
            if c in out.columns:
                out[c] = pd.to_datetime(out[c], errors="coerce", infer_datetime_format=True).dt.date

        # Numerics
        for c in NUM_COLS:
            if c in out.columns:
                out[c] = pd.to_numeric(out[c], errors="coerce")

        # Nullable integer
        for c in INT_COLS:
            if c in out.columns:
                out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")

    return out

def reorder_and_fill(df: pd.DataFrame, expected_cols: list[str]) -> pd.DataFrame:
    """
    Ensure all expected columns exist; add missing as None; preserve extras at the end.
    """
    out = df.copy()
    missing = [c for c in expected_cols if c not in out.columns]
    for m in missing:
        out[m] = None
    # final order: expected cols first, then any extras present
    extras = [c for c in out.columns if c not in expected_cols]
    out = out[expected_cols + extras]
    return out

def load():
    # Connect once; use a transaction per run
    engine = create_engine(POSTGRES_URL, future=True)

    if not os.path.exists(XLSX):
        print(f"ERROR: Excel file not found at: {XLSX}")
        sys.exit(1)

    xls = pd.ExcelFile(XLSX)

    with engine.begin() as conn:  # transactional
        # Create schemas and base DDL for Postgres
        ensure_postgres_schemas(conn)
        ddl_path = os.path.join(os.path.dirname(__file__), "ddl_postgres.sql")
        if os.path.exists(ddl_path):
            conn.execute(text(open(ddl_path, "r", encoding="utf-8").read()))
        else:
            print(f"WARNING: {ddl_path} not found — proceeding assuming tables already exist.")

        for sheet, (pg_schema, pg_table) in TABLES.items():
            if sheet not in xls.sheet_names:
                raise RuntimeError(f"Sheet '{sheet}' not found in {XLSX}")

            # Read as strings, then coerce (prevents pandas guessing wrong types)
            df_raw = pd.read_excel(xls, sheet_name=sheet, dtype=str)
            df_raw = df_raw.where(pd.notnull(df_raw), None)

            # Enforce expected columns & order
            df_norm = reorder_and_fill(df_raw, EXPECTED[sheet])

            # Coerce data types
            df_clean = coerce_types(df_norm, sheet)

            # dtype map only for Orders to ensure correct parameter binding
            dtype_map = PG_DTYPE_ORDERS if sheet == "Orders" else None

            # Write
            schema_arg = None if pg_schema == "public" else pg_schema
            print(f"Loading sheet '{sheet}' -> {pg_schema}.{pg_table} ({len(df_clean)} rows)")
            df_clean.to_sql(
                pg_table,
                con=conn,
                schema=schema_arg,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
                dtype=dtype_map,
            )

    print("All sheets loaded successfully.")

if __name__ == "__main__":
    load()
