import pandas as pd
from etl.transform import validate_and_clean

def test_amount_and_rules():
    df = pd.DataFrame({
        "order_id": ["1", "1", "2"],
        "customer_id": ["c1", "c1", "c2"],
        "event_date": ["2025-01-01"]*3,
        "event_time": ["2025-01-01 10:00:00", "2025-01-01 11:00:00", "2025-01-02 09:00:00"],
        "sku": ["A","A","B"],
        "price": [10, 10, -5],
        "quantity": [2, 3, 1]
    })
    clean = validate_and_clean(df)
    # last write wins for order_id=1, negative price row should be filtered
    assert len(clean) == 1
    row = clean.iloc[0]
    assert row["order_id"] == "1"
    assert row["amount"] == 30
