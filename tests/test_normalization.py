import re
from reconworks.normalization import vendor_clean_text, canonicalize_vendor

def test_vendor_clean_text_basic():
    assert vendor_clean_text("STARBUCKS #04921 MADISON") == "starbucks madison"
    assert vendor_clean_text("AMZN Mktp US*2H3K21") == "amzn"
    assert vendor_clean_text("UBER TRIP HELP.UBER.COM") == "uber trip uber"

def test_canonicalize_vendor_alias():
    rules = [(re.compile("AMZN|AMAZON", re.I), "Amazon", "AMZN|AMAZON")]
    canon, method, conf, notes = canonicalize_vendor("AMZN Mktp US*2H3K21", rules)
    assert canon == "Amazon"
    assert method == "alias_regex"
    assert conf >= 0.9
