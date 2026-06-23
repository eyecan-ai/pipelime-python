# Running the tests

Full suite (default):

    pytest

Fast suite — high coverage, low runtime. It collapses the combinatorial
`nproc`/`prefetch`/`lazy` parametrization to one representative (serial)
combination and deselects tests marked `@pytest.mark.slow`:

    pytest --fast
    # or
    PIPELIME_TEST_FAST=1 pytest

`PIPELIME_TEST_FAST` and `--fast` are equivalent; the env var also drives the
import-time parametrization collapse in `tests/_fast.py`, so prefer it when you
want the smaller matrix to take effect during collection.

## Baseline (measured 2026-06-22, develop venv, Python 3.12)

| Mode | Result | Wall-clock | Coverage |
|------|--------|-----------|----------|
| full | 2393 passed, 7 failed*, 10 skipped | 4988s (1:23:07) | 88% |
| fast | 1835 passed, 7 failed*, 11 skipped | 169s (0:02:49) | 88% |

Fast mode is ~29x faster with a ~0.2% coverage delta.

\* The 7 failures are pre-existing and unrelated to the test tooling: the
interactive variable-resolution tests in `tests/pipelime/cli/test_base.py`
(`test_missing_var/for/switch...`) fail with
`AttributeError: 'Console' object has no attribute '_live_stack'`, a `rich`
version incompatibility. Treat "7 failed" as the known baseline.

## Markers

- `slow` — heavy tests (e.g. nested multiprocessing grabber) deselected under
  `--fast`. Tag a test `@pytest.mark.slow` only when it is both slow and
  redundant with cheaper coverage of the same code path.
