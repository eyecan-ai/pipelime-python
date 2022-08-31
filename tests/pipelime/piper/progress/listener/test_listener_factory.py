from pipelime.piper.progress.listener.factory import (
    ProgressReceiverFactory,
    ListenerCallbackFactory,
)
from pipelime.piper.progress.listener.base import ProgressReceiver, ListenerCallback


class TestReceiverFactory:
    def test_get_receiver(self):
        receiver = ProgressReceiverFactory.get_receiver("token")
        assert isinstance(receiver, ProgressReceiver)


class TestListenerCallbackFactory:
    def test_get_callback(self):
        callback = ListenerCallbackFactory.get_callback()
        assert isinstance(callback, ListenerCallback)
