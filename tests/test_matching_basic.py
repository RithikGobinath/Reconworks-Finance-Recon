import pandas as pd
from reconworks.matching import build_candidates, choose_matches

def test_basic_exact_match():
    ft = pd.DataFrame([{
        "txn_id": "t1",
        "vendor_canonical": "Amazon",
        "vendor_id": "v1",
        "date": "2025-12-02",
        "amount_cents": 4827,
    }])
    fp = pd.DataFrame([{
        "pay_id": "p1",
        "vendor_canonical": "Amazon",
        "vendor_id": "v1",
        "date": "2025-12-02",
        "amount_cents": 4827,
    }])

    cand = build_candidates(
        batch_id="b1",
        fact_transactions=ft,
        fact_vendor_payments=fp,
        date_window_days=3,
        amount_tolerance_cents=0,
        w_vendor=0.6,
        w_date=0.3,
        w_amount=0.1,
    )
    assert len(cand) == 1
    matches = choose_matches(cand, min_score=0.85)
    assert len(matches) == 1
    assert matches.iloc[0]["match_type"] == "exact"
