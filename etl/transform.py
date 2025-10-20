import pandas as pd
import numpy as np

def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    # Basic coercions
    if "order_id" in out.columns:
        out["order_id"] = out["order_id"].astype(str)
    if "customer_id" in out.columns:
        out["customer_id"] = out["customer_id"].astype(str)
    if "price" in out.columns:
        out["price"] = pd.to_numeric(out["price"], errors="coerce").fillna(0.0)
    if "quantity" in out.columns:
        out["quantity"] = pd.to_numeric(out["quantity"], errors="coerce").fillna(0).astype(int)
    # Parse datetime
    if "event_time" in out.columns:
        out["event_time"] = pd.to_datetime(out["event_time"], errors="coerce")
    return out

def validate_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = coerce_types(df)
    # Rules
    if "price" in out.columns:
        out = out[out["price"] >= 0]
    if "quantity" in out.columns:
        out = out[out["quantity"] >= 1]
    # Drop rows with invalid event_time if present
    if "event_time" in out.columns:
        out = out[~out["event_time"].isna()]
    # De-duplicate by order_id (last write wins)
    if "order_id" in out.columns:
        out = out.sort_values(by="event_time").drop_duplicates(subset=["order_id"], keep="last")
    # Derived metrics
    if {"price", "quantity"} <= set(out.columns):
        out["amount"] = out["price"] * out["quantity"]
    return out.reset_index(drop=True)
