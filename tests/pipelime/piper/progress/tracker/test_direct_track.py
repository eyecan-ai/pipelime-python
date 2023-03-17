from pipelime.piper.progress.tracker.direct import DirectTrackCallback
from pipelime.piper.progress.listener.base import ListenerCallback
from pipelime.piper.progress.model import ProgressUpdate, OperationInfo


class MockListenerCallback(ListenerCallback):
    def __init__(self):
        super().__init__(False)
        self.on_start_called = False
        self.on_update_called = False
        self.on_stop_called = False

    def on_start(self):
        self.on_start_called = True

    def on_update(self, prog: ProgressUpdate):
        self.on_update_called = True

    def on_stop(self):
        self.on_stop_called = True


class TestDirectTrackCallback:
    def test_callback(self):
        listener = MockListenerCallback()
        callback = DirectTrackCallback(listener)

        # Create a progress update
        op_info = OperationInfo(
            token="token", node="node", chunk=1, message="msg", total=10
        )
        progress_update = ProgressUpdate(op_info=op_info, progress=0, finished=False)

        callback.update(progress_update)
        callback.stop_callbacks()

        assert listener.on_start_called
        assert listener.on_update_called
        assert listener.on_stop_called
