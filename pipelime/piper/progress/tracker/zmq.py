import time

import zmq

from pipelime.piper.progress.model import ProgressUpdate
from pipelime.piper.progress.tracker.base import TrackCallback


class ZmqTrackCallback(TrackCallback):
    """ZMQ tracker callback"""

    def __init__(self, addr: str = "tcp://*:5556") -> None:
        super().__init__()
        self._addr = addr
        self._socket = None

    def _send(self, prog: ProgressUpdate) -> None:
        topic = prog.op_info.token
        if self._socket is not None:
            self._socket.send_multipart([topic.encode(), prog.json().encode()])

    def on_start(self, prog: ProgressUpdate) -> None:
        self._socket = zmq.Context().socket(zmq.PUB)
        self._socket.bind(self._addr)

        # Wait for the socket to be ready...
        # Apparently, this is the only way to do it. I don't know why.
        time.sleep(1)

        self._send(prog)

    def on_advance(self, prog: ProgressUpdate) -> None:
        self._send(prog)

    def on_finish(self, prog: ProgressUpdate) -> None:
        self._send(prog)

        if self._socket is not None:
            self._socket.close()
            self._socket = None  # free resources
