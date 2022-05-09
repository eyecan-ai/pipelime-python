from itertools import count
from typing import Iterable

import zmq
from loguru import logger

from pipelime.piper.progress.model import OperationInfo, ProgressUpdate


class TrackCallback:
    """A custom callback on start, stop and advancement of a running operation"""

    def __init__(self):
        """Constructor for a generic `TrackCallback`"""
        self._op_info = None

    @property
    def _ready(self) -> bool:
        return self._op_info is not None

    def start(self, op_info: OperationInfo) -> None:
        """Set this callback in "ready" state and call the `on_start` callback

        Args:
            op_info (OperationInfo): The operation info to track

        Raises:
            RuntimeError: If the callback is already setup
        """
        if self._ready:
            raise RuntimeError("Callback already setup")
        self._op_info = op_info
        self.on_start()

    def advance(self, advance: int = 1) -> None:
        """Advance the progress of the tracked operation by a custom amount of steps

        Args:
            advance (int, optional): The number of steps to advance. Defaults to 1.

        Raises:
            RuntimeError: If the callback is not setup
        """
        if not self._ready:
            raise RuntimeError("Callback not setup")
        prog_update = ProgressUpdate(op_info=self._op_info, advance=advance)
        self.on_advance(prog_update)

    def finish(self) -> None:
        """Finish the trackeing of the operation and call the `on_finish` callback

        Raises:
            RuntimeError: If the callback is not setup
        """
        if not self._ready:
            raise RuntimeError("Callback not setup")
        self._op_info = None
        self.on_finish()

    def on_start(self) -> None:
        """What to do when the operation is started"""
        pass

    def on_advance(self, prog: ProgressUpdate) -> None:
        """What to do when the operation advances

        Args:
            prog (ProgressUpdate): The progress update object.
        """
        pass

    def on_finish(self) -> None:
        """What to do when the operation is finished"""
        pass


class ZmqTrackCallback(TrackCallback):
    """ZMQ tracker callback"""

    def __init__(self, addr: str = "tcp://*:5556") -> None:
        super().__init__()
        self._addr = addr

    def on_start(self) -> None:
        context = zmq.Context()
        self._socket = context.socket(zmq.PUB)
        self._socket.bind(self._addr)

    def on_advance(self, prog: ProgressUpdate):
        topic = prog.op_info.token
        self._socket.send_multipart([topic.encode(), prog.json().encode()])

    def on_finish(self) -> None:
        self._socket.close()


class LoguruTrackCallback(TrackCallback):
    """Loguru tracker callback"""

    def __init__(self, level: str = "INFO") -> None:
        super().__init__()
        self._level = level

    def on_start(self) -> None:
        logger.log(
            self._level,
            "Token: {} | Node: {} | Chunk: {} | {} | Started.",
            self._op_info.token,
            self._op_info.node,
            self._op_info.chunk,
            self._op_info.message,
        )

    def on_advance(self, prog: ProgressUpdate):
        logger.log(
            self._level,
            "Token: {} | Node: {} | Chunk: {} | {} | Advanced of {} steps.",
            self._op_info.token,
            self._op_info.node,
            self._op_info.chunk,
            self._op_info.message,
            prog.advance,
        )

    def on_finish(self) -> None:
        logger.log(
            self._level,
            "Token: {} | Node: {} | Chunk: {} | {} | Finished.",
            self._op_info.token,
            self._op_info.node,
            self._op_info.chunk,
            self._op_info.message,
        )


class TrackCallbackFactory:
    """Factory for `TrackCallback`s"""

    DEFAULT_CALLBACK_TYPE = "ZMQ"

    CLASS_MAP = {
        "ZMQ": ZmqTrackCallback,
        "LOGURU": LoguruTrackCallback,
    }

    @classmethod
    def get_callback(
        cls, type_: str = DEFAULT_CALLBACK_TYPE, **kwargs
    ) -> TrackCallback:
        return cls.CLASS_MAP[type_](**kwargs)


class Tracker:
    """Tracker for running operations"""

    def __init__(self, token: str, node: str, *callbacks: TrackCallback) -> None:
        """Constructor for a `Tracker`

        Args:
            token (str): The piper token
            node (str): The piper node name
            callbacks (TrackCallback, optional): The callbacks to use.
        """
        self._counter = count()
        self._callbacks = callbacks
        self._token = token
        self._node = node

    def track(self, seq: Iterable, message: str = "") -> Iterable:
        """Track a generic iterable sequence"""

        id_ = next(self._counter)
        op_info = OperationInfo(
            token=self._token,
            node=self._node,
            chunk=id_,
            total=len(seq),
            message=message,
        )

        for callback in self._callbacks:
            callback.start(op_info)

        for x in seq:
            for callback in self._callbacks:
                callback.advance()

            yield x

        for callback in self._callbacks:
            callback.finish()
