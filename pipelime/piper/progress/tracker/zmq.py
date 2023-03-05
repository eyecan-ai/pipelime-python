import time
import zmq
from loguru import logger
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

    PROTOTYPES: Dict[int, "weakref.ReferenceType[ZmqTrackCallback]"] = {}
    MAX_PORT_NUMBER = 30000
    LOCK = Lock()

    def __new__(cls, port: int = 5555):
        with cls.LOCK:
            proto_ref = cls.PROTOTYPES.get(port)
            proto: Optional["ZmqTrackCallback"] = proto_ref() if proto_ref else None

            if proto is None:
                proto = super().__new__(cls)

                # Create the socket
                proto._socket = zmq.Context().socket(zmq.PUB)

                while port < cls.MAX_PORT_NUMBER:
                    if ZmqTrackCallback._try_bind(proto._socket, port):
                        if port == 5555:
                            logger.info(f"Piper tracking bound to the default port")
                            logger.info(
                                f"Run `pipelime watch +t YOUR_TOKEN` to see the progress"
                            )
                        else:
                            logger.info(f"Piper tracking bound to port {port}")
                            logger.info(
                                f"Run `pipelime watch +t YOUR_TOKEN +p {port}` "
                                "to see the progress"
                            )
                        break
                    port += 1
                if port >= cls.MAX_PORT_NUMBER:
                    raise RuntimeError(
                        "ZMQ piper tracker could not bind to any standard port."
                    )

                proto._finalizer = weakref.finalize(
                    proto, ZmqTrackCallback.clean_up, proto._socket
                )

                # Wait for the socket to be ready...
                # Apparently, this is the only way to do it. I don't know why.
                time.sleep(1)

                # Save the prototype for future use
                cls.PROTOTYPES[port] = weakref.ref(proto)
        return proto

    @staticmethod
    def _try_bind(socket, port: int) -> bool:
        try:
            socket.bind(f"tcp://*:{port}")
        except zmq.ZMQError:
            return False
        return True

    def update(self, prog: ProgressUpdate):
        topic = prog.op_info.token
        self._socket.send_multipart([topic.encode(), prog.json().encode()])

    @staticmethod
    def clean_up(socket: zmq.Socket):
        socket.close()
