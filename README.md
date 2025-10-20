# DDA ETL Demo — Inspired by *Designing Data‑Intensive Applications*

A tiny, practical ETL demo that applies core ideas from Martin Kleppmann’s *Designing Data‑Intensive Applications* (DDA):
**idempotent batch jobs, schema-on-write, and incremental loads**.

> Goal: Extract CSV "sales" data, validate + clean with pandas, and load into a **SQLite** warehouse.
We include a simple **incremental upsert** (based on `order_id`) and a **data quality report**.

## Why this design (DDA takeaways)
- **Idempotency:** The load step is an *upsert* keyed by `order_id`. Re-running the same batch won’t duplicate rows.
- **Schema-on-write:** We enforce types and ranges before loading to the warehouse.
- **Backfill-friendly:** Batch job can reprocess a date range without breaking the current state.
- **Lineage / auditability:** We keep a processed CSV snapshot per run and log a minimal run manifest (`config/run_log.json`).

## Project structure
```
dda-etl-demo/
├── etl/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── run_etl.py
│   └── utils.py
├── data/
│   ├── raw/            # source CSVs arrive here
│   └── processed/      # cleaned snapshots (per run)
├── warehouse/          # SQLite db + schema
├── config/
│   ├── settings.json
│   └── run_log.json
└── tests/
    └── test_transform.py
```

## Quickstart
```bash
# 1) (Optional) create and activate a venv
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 2) Install deps
pip install -r requirements.txt

# 3) Run a demo batch for a given processing date (defaults to today)
python etl/run_etl.py --process-date 2025-01-01
```

This will:
1. Read all `data/raw/sales_*.csv` with `event_date <= process-date`
2. Validate + clean the data
3. Write a processed snapshot CSV under `data/processed/`
4. Upsert rows into `warehouse/warehouse.db` (`fact_sales` table)

## Configuration
Edit `config/settings.json` to change file patterns, warehouse path, and table name.

## Incremental Loads
The load step performs an **UPSERT** on `order_id`. If a row with the same `order_id` exists, it's replaced (latest wins).
This mirrors an append-only log + compaction mindset from DDA.

## Minimal Data Quality Rules
- `price >= 0`, `quantity >= 1`
- valid `event_time` parse
- drop duplicate `order_id`s (last write wins)
- coerce `customer_id` to string

A simple run manifest is stored in `config/run_log.json`.

## Testing
We include one simple test to illustrate transform behavior:
```bash
pytest -q
```

## Notes
This is a minimal educational demo, not production code. It favors readability and the DDA concepts over hyper-optimized performance.
