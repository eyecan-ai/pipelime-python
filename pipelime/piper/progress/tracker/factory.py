from pipelime.piper.progress.tracker.base import TrackCallback
from pipelime.piper.progress.tracker.loguru import LoguruTrackCallback
from pipelime.piper.progress.tracker.zmq import ZmqTrackCallback


class TrackCallbackFactory:
    """Factory for `TrackCallback`s"""

    DEFAULT_CALLBACK_TYPE = "ZMQ"

    CLASS_MAP = {
        "ZMQ": ZmqTrackCallback,
        "LOGURU": LoguruTrackCallback,
    }

    @classmethod
    def get_callback(
        cls, type_: str = DEFAULT_CALLBACK_TYPE, **kwargs
    ) -> TrackCallback:
        return cls.CLASS_MAP[type_](**kwargs)