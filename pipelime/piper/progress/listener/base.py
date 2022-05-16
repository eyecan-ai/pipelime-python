from abc import ABC, abstractmethod
from threading import Thread
from typing import Optional

from loguru import logger

from pipelime.piper.progress.model import ProgressUpdate


class ListenerCallback:
    """A callback for the listener"""

    def on_start(self) -> None:
        """Called when the listener starts"""
        pass

    def on_update(self, prog: ProgressUpdate) -> None:
        """Called when a progress update is received"""
        pass

    def on_stop(self) -> None:
        """Called when the listener stops"""
        pass


class ProgressReceiver(ABC):
    """A receiver for progress updates"""

    def __init__(self, token: str) -> None:
        super().__init__()
        self._token = token

    @abstractmethod
    def receive(self) -> Optional[ProgressUpdate]:
        """Receive a progress update"""
        pass

    def __next__(self) -> Optional[ProgressUpdate]:
        """Wait for the next progress update"""
        try:
            res = self.receive()
        except Exception as e:  # pragma: no cover
            logger.exception(e)
            res = None
        return res


class Listener:
    """A listener for progress updates"""

    def __init__(
        self, receiver: ProgressReceiver, *callbacks: ListenerCallback
    ) -> None:
        """Initialize the listener

        Args:
            receiver (ProgressReceiver): The progress receiver to use
        """
        self._receiver = receiver
        self._callbacks = callbacks

        self._stop_flag = False
        self._listening_thread = None

    def _listen(self) -> None:
        while not self._stop_flag:
            prog = next(self._receiver)
            if prog is None:
                continue  # pragma: no cover

            for cb in self._callbacks:
                cb.on_update(prog)

    def start(self) -> None:
        """Start the listener in a thread"""
        self._listening_thread = Thread(target=self._listen)
        self._listening_thread.start()

        for cb in self._callbacks:
            cb.on_start()

    def stop(self) -> None:
        """Stop the listener"""
        self._stop_flag = True
        self._listening_thread.join(5.0)
        self._listening_thread = None

        for cb in self._callbacks:
            cb.on_stop()
