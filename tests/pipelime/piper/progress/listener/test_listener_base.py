import time
from typing import List, Optional

from pipelime.piper.progress.listener.base import (
    Listener,
    ListenerCallback,
    ProgressReceiver,
)
from pipelime.piper.progress.model import OperationInfo, ProgressUpdate


class MockProgressReceiver(ProgressReceiver):
    def __init__(self, token: str, op_info: OperationInfo) -> None:
        super().__init__(token)
        self._op_info = op_info
        self._progress = 0

    def receive(self) -> Optional[ProgressUpdate]:
        if self._progress < self._op_info.total:
            self._progress += 1
        time.sleep(0.01)
        return ProgressUpdate(op_info=self._op_info, progress=self._progress)


class MockListenerCallback(ListenerCallback):
    def __init__(self) -> None:
        super().__init__(False)
        self.on_start_called = 0
        self.on_update_called: List[ProgressUpdate] = []
        self.on_stop_called = 0

    def on_start(self) -> None:
        self.on_start_called += 1

    def on_update(self, prog: ProgressUpdate) -> None:
        self.on_update_called.append(prog)

    def on_stop(self) -> None:
        self.on_stop_called += 1


class TestListener:
    def test_listener(self):
        # Create listener
        op_info = OperationInfo(
            token="token", node="node", chunk=1, message="message", total=10
        )
        receiver = MockProgressReceiver("token", op_info)
        callback = MockListenerCallback()
        listener = Listener(receiver, callback)

        # Start listener
        listener.start()

        # Wait some time
        time.sleep(0.2)

        # Stop listener
        listener.stop()

        # Assert that listener was called correctly
        assert callback.on_start_called == 1
        assert len(callback.on_update_called) > 0
        assert callback.on_stop_called == 1

        assert callback.on_update_called[-1].op_info == op_info
        assert callback.on_update_called[-1].progress > 0
