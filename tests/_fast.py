import os

FAST = bool(os.environ.get("PIPELIME_TEST_FAST"))


def fast_params(full, *, fast=None):
    """Return the parametrize values to use.

    In FAST mode (PIPELIME_TEST_FAST set), return `fast` if provided,
    else the first element of `full`. Otherwise return `full` unchanged.
    """
    if not FAST:
        return list(full)
    if fast is not None:
        return list(fast)
    return list(full)[:1]
