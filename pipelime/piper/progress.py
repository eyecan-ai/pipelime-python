from itertools import count
from typing import Iterable

import zmq
from pydantic import BaseModel


class ProgressUpdate(BaseModel):
    id_: int
    total: int
    message: str = ""
    advance: int = 1


class TrackCallback:
    def __init__(self):
        self._is_setup = False

    def start(self, id_: int, total: int, message: str = "") -> None:
        self._id = id_
        self._total = total
        self._is_setup = True
        self._message = message
        self.on_start()

    def _build_progress_update(self, advance: int) -> ProgressUpdate:
        return ProgressUpdate(
            id_=self._id, total=self._total, message=self._message, advance=advance
        )

    def advance(self, advance: int = 1) -> None:
        if not self._is_setup:
            raise RuntimeError("Callback not setup")
        prog_update = self._build_progress_update(advance)
        self.on_advance(prog_update)

    def finish(self) -> None:
        if not self._is_setup:
            raise RuntimeError("Callback not setup")
        self.on_finish()

    def on_start(self) -> None:
        pass

    def on_advance(self, prog: ProgressUpdate) -> None:
        pass

    def on_finish(self) -> None:
        pass


class Tracker:
    def __init__(self, *callbacks: TrackCallback) -> None:
        self._counter = count()
        self._callbacks = callbacks

    def track(self, seq: Iterable, message: str = "") -> Iterable:
        id_ = next(self._counter)

        for callback in self._callbacks:
            callback.start(id_, len(seq), message=message)

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
        topic = "TOKEN"
        self._socket.send_multipart([topic.encode(), prog.json().encode()])

    def on_finish(self) -> None:
        self._socket.close()
