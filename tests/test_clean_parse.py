from reconworks.cleaning import _parse_amount_cents, _parse_date_iso

def test_parse_amount_cents():
    assert _parse_amount_cents("48.27")[0] == 4827
    assert _parse_amount_cents("$1,234.56")[0] == 123456
    assert _parse_amount_cents("(12.00)")[0] == -1200
    assert _parse_amount_cents("-12.00")[0] == -1200
    assert _parse_amount_cents("")[0] is None

def test_parse_date_iso():
    assert _parse_date_iso("2025-12-02")[0] == "2025-12-02"
    assert _parse_date_iso("12/2/25")[0] == "2025-12-02"
    assert _parse_date_iso("")[0] is None
