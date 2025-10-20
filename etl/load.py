import sqlite3
from pathlib import Path
import pandas as pd

def ensure_schema(conn, table_name: str):
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT,
        event_date TEXT,
        event_time TEXT,
        sku TEXT,
        price REAL,
        quantity INTEGER,
        amount REAL
    )
    """)
    conn.commit()

def upsert_dataframe(conn, table_name: str, df: pd.DataFrame, pk: str = "order_id"):
    if df.empty:
        return 0
    ensure_schema(conn, table_name)
    # Upsert row-by-row for readability (OK for demo scale)
    rows = 0
    for _, r in df.iterrows():
        cols = list(df.columns)
        placeholders = ",".join(["?"] * len(cols))
        update_assignments = ",".join([f"{c}=excluded.{c}" for c in cols if c != pk])
        sql = f"""INSERT INTO {table_name} ({','.join(cols)})
                 VALUES ({placeholders})
                 ON CONFLICT({pk}) DO UPDATE SET {update_assignments}
              """
        conn.execute(sql, tuple(r[c] for c in cols))
        rows += 1
    conn.commit()
    return rows
