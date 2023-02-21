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

    def update(self, prog: ProgressUpdate):
        if self.socket:
            topic = prog.op_info.token
            self.socket.send_multipart([topic.encode(), prog.json().encode()])

            if prog.finished:
                self.socket.close()
                self._socket = None  # free resources

    @property
    def socket(self):
        if not self._socket:
            self._socket = zmq.Context().socket(zmq.PUB)
            self._socket.bind(self._addr)

            # Wait for the socket to be ready...
            # Apparently, this is the only way to do it. I don't know why.
            time.sleep(1)
        return self._socket
