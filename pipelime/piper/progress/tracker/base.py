import shutil
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


class TqdmTask(TrackedTask):
    @staticmethod
    def _tty_length():
        return shutil.get_terminal_size((80, 20)).columns

    @staticmethod
    def _clip_message(msg: str, length: int) -> str:
        return msg if len(msg) <= length else msg[: length - 3] + "..."

    @staticmethod
    def default_bar(
        iterable=None,
        *,
        total=None,
        message=None,
        bar_width=None,
        ncols=None,
        position=None,
    ):
        from tqdm import tqdm

        tty_length = TqdmTask._tty_length()
        bar = tqdm(
            iterable,
            total=len(iterable) if total is None else total,  # type: ignore
            colour="#4CAE4F",
            position=position,
            ncols=tty_length if ncols is None else (None if ncols <= 0 else ncols),
            dynamic_ncols=ncols is None,
            bar_format=(
                None
                if bar_width is None
                else "{desc} {percentage:3.0f}%|{bar:" + str(bar_width) + "}{r_bar}"
            ),
        )
        TqdmTask._set_description(bar, message)
        return bar

    @staticmethod
    def _set_description(bar, message: Optional[str]):
        # Hardcoded factor: the message will not be longer than the 40% of the
        # TTY width. This should be appropriate for most cases.
        factor = 0.4
        maxlen = int(TqdmTask._tty_length() * factor)

        # Format the message, adding emoji and clipping it if necessary
        message = "🍋 " + message if message else "🍋"
        message = TqdmTask._clip_message(message, maxlen)

        # Set the description
        bar.set_description_str(message)

    def __init__(
        self,
        total: int,
        message: str,
        bar_width: Optional[int] = None,
        total_width: Optional[int] = None,
        position: Optional[int] = None,
    ):
        super().__init__()
        self._bar = None
        self._total = total
        self._message = message
        self._bar_width = bar_width
        self._total_width = total_width
        self._position = position

    def set_message(self, message: str):
        self._message = message
        if self._bar:
            self._set_description(self._bar, self._message)

    @property
    def bar(self):
        # create and show the bar upon first access
        if not self._bar:
            self._bar = self.default_bar(
                total=self._total,
                message=self._message,
                bar_width=self._bar_width,
                ncols=self._total_width,
                position=self._position,
            )
        return self._bar

    def on_update(self, finished: bool = False):
        bar = self.bar
        if finished:
            bar.close()
        elif bar.n < self.progress:
            bar.update(self.progress - bar.n)
        elif bar.n > self.progress:
            bar.reset()
            bar.update(self.progress)
        bar.refresh()


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
