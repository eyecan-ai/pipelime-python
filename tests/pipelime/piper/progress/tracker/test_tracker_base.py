from typing import List
import pytest
from pipelime.piper.progress.tracker.base import TrackCallback, Tracker
from pipelime.piper.progress.model import ProgressUpdate, OperationInfo


class MockTrackCallback(TrackCallback):
    def __init__(self):
        super().__init__()
        self.on_start_calls: List[ProgressUpdate] = []
        self.on_advance_calls: List[ProgressUpdate] = []
        self.on_finish_calls: List[ProgressUpdate] = []

    def on_start(self, prog: ProgressUpdate) -> None:
        self.on_start_calls.append(prog)

    def on_advance(self, prog: ProgressUpdate) -> None:
        self.on_advance_calls.append(prog)

    def on_finish(self, prog: ProgressUpdate) -> None:
        self.on_finish_calls.append(prog)


class TestTrackCallback:
    def test_track_callback(self):
        # Create callback
        N = 10
        callback = MockTrackCallback()

        # Assert that callback raises an error if not setup
        with pytest.raises(RuntimeError):
            callback.advance()
        with pytest.raises(RuntimeError):
            callback.finish()

        # Setup callback
        op_info = OperationInfo(
            token="token", node="node", chunk=1, message="message", total=N
        )
        callback.start(op_info)

        # Assert that callback raises an error if already setup
        with pytest.raises(RuntimeError):
            callback.start(op_info)

        # Advance callback
        for _ in range(N // 2):
            callback.advance(2)

        # Finish callback
        callback.finish()

        # Assert that callback was called correctly
        assert len(callback.on_start_calls) == 1
        assert len(callback.on_advance_calls) == N // 2
        assert len(callback.on_finish_calls) == 1

        assert callback.on_advance_calls[-1].progress == N


class TestTracker:
    def test_tracker(self):
        # Create tracker
        callback = MockTrackCallback()
        tracker = Tracker("token", "node", callback)

        # Create a list of stuff
        seq = list(range(10, 20, 2))

        # Track the list
        decorated = tracker.track(seq)

        # Assert that the list was not modified by tracking
        assert list(decorated) == seq

        # Assert that callback was called correctly
        assert len(callback.on_start_calls) == 1
        assert len(callback.on_advance_calls) == len(seq)
        assert len(callback.on_finish_calls) == 1

        assert callback.on_advance_calls[-1].progress == len(seq)
