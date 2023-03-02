import time
import zmq
from typing import Dict, Optional
from threading import Lock
import weakref

from pipelime.piper.progress.model import ProgressUpdate
from pipelime.piper.progress.tracker.base import TrackCallback


class ZmqTrackCallback(TrackCallback):
    """ZMQ tracker callback"""

    _addr: str
    _socket: zmq.Socket
    _finalizer: weakref.finalize

    PROTOTYPES: Dict[str, "weakref.ReferenceType[ZmqTrackCallback]"] = {}
    LOCK = Lock()

    def __new__(cls, addr: str = "tcp://*:5556"):
        with cls.LOCK:
            proto_ref = cls.PROTOTYPES.get(addr)
            proto: Optional["ZmqTrackCallback"] = proto_ref() if proto_ref else None

            if proto is None:
                proto = super().__new__(cls)

                # Create the socket
                proto._socket = zmq.Context().socket(zmq.PUB)
                proto._socket.bind(addr)
                proto._finalizer = weakref.finalize(
                    proto, ZmqTrackCallback.clean_up, proto._socket
                )

                # Wait for the socket to be ready...
                # Apparently, this is the only way to do it. I don't know why.
                time.sleep(1)

                # Save the prototype for future use
                cls.PROTOTYPES[addr] = weakref.ref(proto)
        return proto

    def update(self, prog: ProgressUpdate):
        topic = prog.op_info.token
        self._socket.send_multipart([topic.encode(), prog.json().encode()])

    @staticmethod
    def clean_up(socket: zmq.Socket):
        socket.close()
