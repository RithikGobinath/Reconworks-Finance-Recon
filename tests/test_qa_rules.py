import pandas as pd
from reconworks.qa_checks import run_qa_for_batch, PolicyRule

def test_policy_rule_triggers():
    ft = pd.DataFrame([{
        "txn_id": "t1",
        "vendor_canonical": "Amazon",
        "vendor_id": "v1",
        "date": "2025-01-01",
        "amount_cents": 60000,
        "source_file": "x",
        "source_row_number": 1,
        "row_hash": "h1",
        "is_weekend": 0,
    }])
    fp = pd.DataFrame([])
    rules = [PolicyRule(flag_code="POLICY_HIGH_AMOUNT", field="amount_cents", op=">", value="50000", severity="warning", message="hi", applies_to="transactions")]
    flags = run_qa_for_batch("b1", ft, fp, rules)
    assert (flags["flag_code"] == "POLICY_HIGH_AMOUNT").any()
