import argparse, json
from pathlib import Path
import sqlite3
import pandas as pd
from datetime import datetime

from .extract import extract_csvs
from .transform import validate_and_clean
from .load import upsert_dataframe, ensure_schema

def run(process_date: str, settings_path: str):
    settings = json.loads(Path(settings_path).read_text(encoding="utf-8"))
    raw_glob = settings["raw_glob"]
    processed_dir = Path(settings["processed_dir"])
    processed_dir.mkdir(parents=True, exist_ok=True)
    warehouse_path = settings["warehouse_path"]
    table_name = settings["table_name"]
    pk = settings["primary_key"]

    # 1) Extract
    df = extract_csvs(raw_glob, process_date=process_date)

    # 2) Transform
    clean = validate_and_clean(df)

    # 3) Save processed snapshot
    snapshot = processed_dir / f"sales_clean_{process_date}.csv"
    clean.to_csv(snapshot, index=False)

    # 4) Load (UPSERT to SQLite)
    conn = sqlite3.connect(warehouse_path)
    ensure_schema(conn, table_name)
    inserted = upsert_dataframe(conn, table_name, clean, pk=pk)

    # 5) Log run
    manifest_path = Path("config/run_log.json")
    run_log = json.loads(manifest_path.read_text(encoding="utf-8"))
    run_log.setdefault("runs", []).append({
        "process_date": process_date,
        "rows_in": len(df),
        "rows_clean": len(clean),
        "rows_upserted": inserted,
        "snapshot": str(snapshot)
    })
    manifest_path.write_text(json.dumps(run_log, indent=2), encoding="utf-8")

    return {"rows_in": len(df), "rows_clean": len(clean), "rows_upserted": inserted, "snapshot": str(snapshot)}

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--process-date", type=str, default=datetime.today().strftime("%Y-%m-%d"))
    p.add_argument("--settings", type=str, default="config/settings.json")
    args = p.parse_args()
    res = run(args.process_date, args.settings)
    print(json.dumps(res, indent=2))
