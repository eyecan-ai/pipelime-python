def test_fast_option_registered(pytestconfig):
    # --fast defaults to False unless passed or PIPELIME_TEST_FAST is set
    assert pytestconfig.getoption("--fast") in (True, False)


def test_slow_marker_registered(pytestconfig):
    markers = pytestconfig.getini("markers")
    assert any(m.startswith("slow") for m in markers)
