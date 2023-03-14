from abc import ABC, abstractmethod
from threading import Thread
import time
from typing import Optional, Tuple

from loguru import logger

from pipelime.piper.progress.model import ProgressUpdate


class ListenerCallback:
    """A callback for the listener"""

    def __init__(self, show_token: bool):
        self._show_token = show_token

    @property
    def show_token(self) -> bool:
        return self._show_token

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

    def __init__(self, token: Optional[str]) -> None:
        super().__init__()
        self._token = token

    @abstractmethod
    def receive(self) -> Tuple[str, Optional[ProgressUpdate]]:
        """Receive a progress update

        Returns:
            Tuple[str, Optional[ProgressUpdate]]: the token and the progress update
        """
        pass

    def __next__(self) -> Optional[ProgressUpdate]:
        """Wait for the next progress update"""
        try:
            tkn, res = self.receive()
        except Exception:  # pragma: no cover
            logger.exception("Progress receiver error")
            return None

        return res if self._token is None or tkn == self._token else None


class Listener:
    """A listener for progress updates"""

    def __init__(
        self,
        receiver: ProgressReceiver,
        *callbacks: ListenerCallback,
        min_poll_interval: float = 0.1
    ) -> None:
        """Initialize the listener

        Args:
            receiver (ProgressReceiver): The progress receiver to use
            callbacks (ListenerCallback): The callbacks to call
            min_poll_interval (float): The minimum interval, in seconds, between
                progress updates
        """
        self._receiver = receiver
        self._callbacks = callbacks
        self._min_poll_interval = min_poll_interval

        self._stop_flag = False
        self._listening_thread = None

    def _listen(self) -> None:
        while not self._stop_flag:
            last_update = time.time()
            prog = next(self._receiver)
            if prog is not None:
                for cb in self._callbacks:
                    cb.on_update(prog)

            time_diff = time.time() - last_update
            if time_diff < self._min_poll_interval:
                time.sleep(time_diff)

    def start(self) -> None:
        """Start the listener in a thread"""
        self._listening_thread = Thread(target=self._listen)
        self._listening_thread.start()

        for cb in self._callbacks:
            cb.on_start()

    def stop(self) -> None:
        """Stop the listener"""
        if self._listening_thread is not None:
            self._stop_flag = True
            self._listening_thread.join(5.0)
            self._listening_thread = None

            for cb in self._callbacks:
                cb.on_stop()
