import os
import platform

import pytest


class CounterCM:
    def __init__(self, value):
        self.value = value

    def __enter__(self):
        self.value *= 2

    def __exit__(self, exc_type, exc_value, traceback):
        self.value *= 2


def test_context_manager_list():
    from pipelime.utils.context_managers import ContextManagerList

    cms = [CounterCM(1), CounterCM(2)]
    with ContextManagerList(*cms):
        assert all(cms[i].value == 2 * (i + 1) for i in range(2))
    assert all(cms[i].value == 4 * (i + 1) for i in range(2))


@pytest.mark.skipif(platform.system() != "Linux", reason="Linux only")
@pytest.mark.parametrize(
    "to_catch,to_raise,should_interrupt",
    [
        (None, "SIGINT", True),
        (None, "SIGTERM", True),
        (None, "SIGUSR1", False),
        (["SIGUSR1"], "SIGUSR1", True),
        (["SIGUSR1"], "SIGUSR2", False),
    ],
)
def test_catch_signals(to_catch, to_raise, should_interrupt):
    from pipelime.utils.context_managers import CatchSignals
    import signal

    if to_catch:
        to_catch = [getattr(signal, sig) for sig in to_catch]
    to_raise = getattr(signal, to_raise)

    with CatchSignals(to_catch) as catcher:
        assert not catcher.interrupted
        os.kill(os.getpid(), to_raise)
        assert catcher.interrupted is should_interrupt
