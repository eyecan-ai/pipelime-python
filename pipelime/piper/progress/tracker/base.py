from abc import ABC, abstractmethod
from itertools import count
from typing import Iterable, Optional, Sequence, Union

from pipelime.piper.progress.model import OperationInfo, ProgressUpdate


class TrackCallback(ABC):
    """A custom callback to track the progress of a running task"""

    @abstractmethod
    def update(self, prog: ProgressUpdate) -> None:
        """What to do when the operation advances

        Args:
            prog (ProgressUpdate): The progress update object.
        """

class TrackedTask(ABC):
    """Context manager to track a single task."""

    def __init__(self):
        self._progress = 0

    @property
    def progress(self) -> int:
        return self._progress

    def advance(self, advance: int = 1):
        """Advance the task by a custom amount of steps."""
        self._do_update(self._progress + advance)

    def update(self, progress: int):
        """Set the progress of the task to a custom value."""
        self._do_update(progress)

    def finish(self):
        """Complete the task."""
        self._do_update(self._progress, finished=True)

    def restart(self):
        """Restart the task."""
        self._do_update(0)

    def track(self, iterable: Iterable):
        """Track a generic iterable sequence"""
        for x in iterable:
            yield x
            self.advance()

    def _do_update(self, progress: int, finished: bool = False):
        self._progress = progress
        self.on_update(finished)

    @abstractmethod
    def on_update(self, finished: bool = False):
        """What to do when the task is updated"""

    def __enter__(self):
        self.restart()
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        self.finish()


class PiperTask(TrackedTask):
    def __init__(self, op_info: OperationInfo, callbacks: Sequence[TrackCallback]):
        super().__init__()
        self._op_info = op_info
        self._callbacks = callbacks

    def on_update(self, finished: bool = False):
        prog = ProgressUpdate(
            op_info=self._op_info, progress=self.progress, finished=finished
        )
        for cb in self._callbacks:
            cb.update(prog)


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

        with self.create_task(
            total=len(seq) if size is None else size, message=message  # type: ignore
        ) as t:
            yield from t.track(seq)

    def create_task(self, total: int, message: str = ""):
        """Explicit task creation"""

        op_info = OperationInfo(
            token=self._token,
            node=self._node,
            chunk=next(self._counter),
            total=total,  # type: ignore
            message=message,
        )
        return PiperTask(op_info, self._callbacks)
