from typing import Optional

import zmq

from pipelime.piper.progress.listener.base import ProgressReceiver
from pipelime.piper.progress.model import ProgressUpdate


class ZMQProgressReceiver(ProgressReceiver):
    """A receiver for progress updates over pubsub ZMQ socket"""

    def __init__(self, token: str, host: str = "localhost", port: int = 5555) -> None:
        self._token = token
        super().__init__(token)
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.SUB)
        self._socket.connect(f"tcp://{host}:{port}")
        self._socket.subscribe(token.encode())

    def receive(self) -> Optional[ProgressUpdate]:
        if self._socket is not None:
            while True:
                # Receive a message
                try:
                    token, messagedata = self._socket.recv_multipart(flags=zmq.NOBLOCK)
                except zmq.ZMQError:
                    return None  # no message

                # If token is wrong, wait for another message
                if token.decode() == self._token:
                    break

            # Parse message and return
            return ProgressUpdate.parse_raw(messagedata.decode())
