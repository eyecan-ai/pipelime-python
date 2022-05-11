from typing import Optional

from loguru import logger

from pipelime.piper.progress.listener.base import ProgressReceiver
from pipelime.piper.progress.model import ProgressUpdate


class ZMQProgressReceiver(ProgressReceiver):
    """A receiver for progress updates over pubsub ZMQ socket"""

    def __init__(self, token: str) -> None:
        try:
            import zmq

            super().__init__(token)
            self._context = zmq.Context()
            self._socket = self._context.socket(zmq.SUB)
            self._socket.connect("tcp://localhost:5556")
            self._socket.subscribe(token.encode())
        except ModuleNotFoundError:  # pragma: no cover
            logger.error(f"{self.__class__.__name__} needs `pyzmq` python package.")
            self._socket = None

    def receive(self) -> Optional[ProgressUpdate]:
        if self._socket is not None:
            _, messagedata = self._socket.recv_multipart()
            return ProgressUpdate.parse_raw(messagedata.decode())
