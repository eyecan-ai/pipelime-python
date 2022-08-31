from pipelime.piper.progress.listener.base import ListenerCallback, ProgressReceiver
from pipelime.piper.progress.listener.receiver.zmq import ZMQProgressReceiver
from pipelime.piper.progress.listener.callbacks.rich_table import (
    RichTableListenerCallback,
)


class ProgressReceiverFactory:
    """Factory for `ProgressReceiver`s"""

    DEFAULT_RECEIVER_TYPE = "ZMQ"

    CLASS_MAP = {
        "ZMQ": ZMQProgressReceiver,
    }

    @classmethod
    def get_receiver(
        cls, token: str, type_: str = DEFAULT_RECEIVER_TYPE
    ) -> ProgressReceiver:
        return cls.CLASS_MAP[type_](token)


class ListenerCallbackFactory:
    """Factory for `ListenerCallback`s"""

    DEFAULT_CALLBACK_TYPE = "RICH_TABLE"

    CLASS_MAP = {
        "RICH_TABLE": RichTableListenerCallback,
    }

    @classmethod
    def get_callback(
        cls, type_: str = DEFAULT_CALLBACK_TYPE, **kwargs
    ) -> ListenerCallback:
        return cls.CLASS_MAP[type_](**kwargs)
