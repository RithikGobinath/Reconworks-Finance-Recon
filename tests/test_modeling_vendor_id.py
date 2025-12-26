from reconworks.modeling import _vendor_id

def test_vendor_id_stable():
    assert _vendor_id("Amazon") == _vendor_id("amazon")
