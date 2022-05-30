from pipelime.piper.progress.tracker.factory import TrackCallbackFactory
from pipelime.piper.progress.tracker.base import TrackCallback


class TestTrackCallbackFactory:
    def test_get_callback(self):
        callback = TrackCallbackFactory.get_callback()
        assert isinstance(callback, TrackCallback)
