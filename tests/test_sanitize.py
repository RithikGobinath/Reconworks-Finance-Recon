from reconworks.util import sanitize_columns

def test_sanitize_columns_unique():
    cols, mapping = sanitize_columns(["A", "A", "a", "a ", "1col"])
    assert len(cols) == 5
    assert len(set(cols)) == 5
    assert mapping["1col"].startswith("c_")
