from typing import List
import pytest
from pipelime.piper.progress.tracker.base import TrackCallback, PiperTask, Tracker
from pipelime.piper.progress.model import ProgressUpdate, OperationInfo


class MockTrackCallback(TrackCallback):
    def __init__(self):
        super().__init__()
        self.calls: List[ProgressUpdate] = []

    def update(self, prog: ProgressUpdate) -> None:
        self.calls.append(prog)


class TestPiperTask:
    def test_track_callback(self):
        # Create callback
        N = 10
        callback = MockTrackCallback()
        op_info = OperationInfo(
            token="token", node="node", chunk=1, message="message", total=3 * N
        )
        task = PiperTask(op_info, [callback])

        task.restart()

        # Advance
        for _ in range(N):
            task.advance(2)

        # Finish
        task.finish()

        # Assert that callback was called correctly
        assert len([p for p in callback.calls if p.progress == 0]) == 1
        assert (
            len([p for p in callback.calls if p.progress != 0 and not p.finished]) == N
        )
        assert len([p for p in callback.calls if p.finished]) == 1

        assert callback.calls[-1].progress == 2 * N


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
        assert len([p for p in callback.calls if p.progress == 0]) == 1
        assert len(
            [p for p in callback.calls if p.progress != 0 and not p.finished]
        ) == len(seq)
        assert len([p for p in callback.calls if p.finished]) == 1

        assert callback.calls[-1].progress == len(seq)
