from typing import Optional, Tuple

import zmq
from loguru import logger

from pipelime.piper.progress.listener.base import ProgressReceiver
from pipelime.piper.progress.model import ProgressUpdate


class ZMQProgressReceiver(ProgressReceiver):
    """A receiver for progress updates over pubsub ZMQ socket"""

    DEFAULT_PORT_NUMBER = 5555

    def __init__(
        self,
        token: Optional[str],
        host: str = "localhost",
        port: int = DEFAULT_PORT_NUMBER,
    ) -> None:
        super().__init__(token)
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.SUB)
        self._socket.connect(f"tcp://{host}:{port}")
        self._socket.subscribe(token.encode() if token is not None else b"")

        logger.info(
            f"Listening on {host}:{port} for progress updates "
            f"with {'any token' if token is None else f'token `{token}`'}"
        )

    def receive(self) -> Tuple[str, Optional[ProgressUpdate]]:
        if self._socket is not None:
            # Receive a message
            try:
                token, messagedata = self._socket.recv_multipart(flags=zmq.NOBLOCK)
            except zmq.ZMQError:
                return "", None  # no message

            # Parse message and return
            return token.decode(), ProgressUpdate.parse_raw(messagedata.decode())
        return "", None  # no message
