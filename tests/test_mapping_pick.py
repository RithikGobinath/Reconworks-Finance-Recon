from reconworks.mapping import _pick_column

def test_pick_column_sanitizes_candidates():
    cols = ["merchant", "post_date", "amount"]
    assert _pick_column(cols, ["Merchant", "Merchant Name"]) == "merchant"
    assert _pick_column(cols, ["Post Date"]) == "post_date"
    assert _pick_column(cols, ["Amount"]) == "amount"
