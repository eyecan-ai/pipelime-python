def test_fast_option_registered(pytestconfig):
    # --fast defaults to False unless passed or PIPELIME_TEST_FAST is set
    assert pytestconfig.getoption("--fast") in (True, False)


def test_slow_marker_registered(pytestconfig):
    markers = pytestconfig.getini("markers")
    assert any(m.startswith("slow") for m in markers)


import importlib


def test_fast_params_collapses(monkeypatch):
    monkeypatch.setenv("PIPELIME_TEST_FAST", "1")
    import tests._fast as f

    importlib.reload(f)
    assert f.fast_params([0, 2]) == [0]
    assert f.fast_params([2, 4], fast=[2]) == [2]


def test_fast_params_full_when_not_fast(monkeypatch):
    monkeypatch.delenv("PIPELIME_TEST_FAST", raising=False)
    import tests._fast as f

    importlib.reload(f)
    assert f.fast_params([0, 2]) == [0, 2]
    assert f.fast_params([2, 4], fast=[2]) == [2, 4]
