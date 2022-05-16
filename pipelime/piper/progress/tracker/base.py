from itertools import count
from typing import Iterable, Optional, Sequence, Union

from pipelime.piper.progress.model import OperationInfo, ProgressUpdate


class TrackCallback:
    """A custom callback on start, stop and advancement of a running operation"""

    def __init__(self):
        """Constructor for a generic `TrackCallback`"""
        self._op_info = None
        self._progress = 0

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
        self._progress = 0
        prog = ProgressUpdate(op_info=self._op_info, progress=self._progress)
        self.on_start(prog)

    def advance(self, advance: int = 1) -> None:
        """Advance the progress of the tracked operation by a custom amount of steps

        Args:
            advance (int, optional): The number of steps to advance. Defaults to 1.

        Raises:
            RuntimeError: If the callback is not setup
        """
        if not self._ready:
            raise RuntimeError("Callback not setup")
        self._progress += advance
        prog = ProgressUpdate(op_info=self._op_info, progress=self._progress)
        self.on_advance(prog)

    def finish(self) -> None:
        """Finish the trackeing of the operation and call the `on_finish` callback

        Raises:
            RuntimeError: If the callback is not setup
        """
        if not self._ready:
            raise RuntimeError("Callback not setup")
        op_info = self._op_info
        self._progress = op_info.total
        prog = ProgressUpdate(op_info=op_info, finished=True, progress=self._progress)
        self.on_finish(prog)
        self._op_info = None

    def on_start(self, prog: ProgressUpdate) -> None:
        """What to do when the operation is started

        Args:
            prog (ProgressUpdate): The progress update object.
        """
        pass

    def on_advance(self, prog: ProgressUpdate) -> None:
        """What to do when the operation advances

        Args:
            prog (ProgressUpdate): The progress update object.
        """
        pass

    def on_finish(self, prog: ProgressUpdate) -> None:
        """What to do when the operation is finished

        Args:
            prog (ProgressUpdate): The progress update object.
        """
        pass


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

    def track(
        self,
        seq: Union[Iterable, Sequence],
        size: Optional[int] = None,
        message: str = "",
    ) -> Iterable:
        """Track a generic iterable sequence"""

        id_ = next(self._counter)
        op_info = OperationInfo(
            token=self._token,
            node=self._node,
            chunk=id_,
            total=len(seq) if size is None else size,  # type: ignore
            message=message,
        )

        for callback in self._callbacks:
            callback.start(op_info)

        for x in seq:
            yield x

            for callback in self._callbacks:
                callback.advance()

        for callback in self._callbacks:
            callback.finish()
