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
    import signal

    from pipelime.utils.context_managers import CatchSignals

    if to_catch:
        to_catch = [getattr(signal, sig) for sig in to_catch]
    to_raise = getattr(signal, to_raise)

    if to_raise not in [signal.SIGINT, signal.SIGTERM]:
        if (to_catch is None) or (to_raise not in to_catch):
            # catch the signal with standard library to avoid process termination

            def handler(signum, frame):
                pass

            signal.signal(to_raise, handler)

    with CatchSignals(to_catch) as catcher:
        assert not catcher.interrupted
        os.kill(os.getpid(), to_raise)
        assert catcher.interrupted is should_interrupt
