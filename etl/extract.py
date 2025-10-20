from pathlib import Path
import pandas as pd
from glob import glob

def extract_csvs(pattern: str, process_date=None):
    # Read multiple CSVs and filter by event_date <= process_date (if provided)
    files = sorted(glob(pattern))
    if not files:
        return pd.DataFrame()
    frames = []
    for f in files:
        df = pd.read_csv(f)
        if "event_date" in df.columns and process_date is not None:
            df = df[df["event_date"] <= str(process_date)]
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
