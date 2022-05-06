from itertools import count
from typing import Iterable

import zmq
from pydantic import BaseModel


class OperationInfo(BaseModel):
    # The piper token identifying the session
    token: str

    # The node currently being executed
    node: str

    # The current chunk number
    chunk: int

    # The progress of the current chunk
    total: int
    message: str = ""


class ProgressUpdate(BaseModel):
    # What operation is being executed
    op_info: OperationInfo

    # The progress of the current chunk
    advance: int = 1


class TrackCallback:
    def __init__(self):
        self._op_info = None

    @property
    def ready(self) -> bool:
        return self._op_info is not None

    def start(self, op_info: OperationInfo) -> None:
        if self.ready:
            raise RuntimeError("Callback already setup")
        self._op_info = op_info
        self.on_start()

    def _build_progress_update(self, advance: int) -> ProgressUpdate:
        return ProgressUpdate(op_info=self._op_info, advance=advance)

    def advance(self, advance: int = 1) -> None:
        if not self.ready:
            raise RuntimeError("Callback not setup")
        prog_update = self._build_progress_update(advance)
        self.on_advance(prog_update)

    def finish(self) -> None:
        if not self.ready:
            raise RuntimeError("Callback not setup")
        self._op_info = None
        self.on_finish()

    def on_start(self) -> None:
        pass

    def on_advance(self, prog: ProgressUpdate) -> None:
        pass

    def on_finish(self) -> None:
        pass


class Tracker:
    def __init__(self, token: str, node: str, *callbacks: TrackCallback) -> None:
        self._counter = count()
        self._callbacks = callbacks
        self._token = token
        self._node = node

    def track(self, seq: Iterable, message: str = "") -> Iterable:
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


class ZmqTrackCallback(TrackCallback):
    def __init__(self, addr: str = "tcp://*:5556"):
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


class TrackCallbackFactory:
    DEFAULT_CALLBACK_TYPE = "ZMQ"

    CLASS_MAP = {
        "ZMQ": ZmqTrackCallback,
    }

    @classmethod
    def get_callback(
        cls, type_: str = DEFAULT_CALLBACK_TYPE, **kwargs
    ) -> TrackCallback:
        return cls.CLASS_MAP[type_](**kwargs)
