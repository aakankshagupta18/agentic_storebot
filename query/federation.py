import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
PG = create_engine(os.getenv("POSTGRES_URL"), future=True)
MY = create_engine(os.getenv("MYSQL_URL"), future=True)

def run_sql(engine_name: str, sql: str, params=None) -> pd.DataFrame:
    eng = PG if engine_name=="postgres" else MY
    with eng.connect() as c:
        return pd.read_sql(text(sql), c, params=params or {})

def stitch(left: pd.DataFrame, right: pd.DataFrame, on_left: str, on_right: str, how="left"):
    return left.merge(right, left_on=on_left, right_on=on_right, how=how, suffixes=("","_r"))

def summarize(df: pd.DataFrame, limit=25) -> str:
    return df.head(limit).to_markdown(index=False)

