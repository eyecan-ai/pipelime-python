import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from threading import Thread
from typing import Dict, Optional

import numpy as np
from loguru import logger
from rich import box
from rich.live import Live
from rich.table import Table

from pipelime.piper.progress.model import OperationInfo, ProgressUpdate


class Estimator(ABC):
    """An estimator for the speed and ETA of a running task"""

    @abstractmethod
    def tick(self, advance: int = 1) -> None:
        """Advance the estimator by a custom amount of steps

        Args:
            advance (int, optional): The number of steps to advance. Defaults to 1.
        """
        pass

    @abstractmethod
    def reset(self, total: int) -> None:
        """Reset the estimator to a new total number of steps

        Args:
            total (int): The total number of steps
        """
        pass

    @property
    @abstractmethod
    def eta(self) -> float:
        """The estimated time remaining in seconds"""
        pass

    @property
    @abstractmethod
    def speed(self) -> float:
        """The estimated speed in steps per second"""
        pass

    @property
    @abstractmethod
    def elapsed(self) -> float:
        """The elapsed time in seconds"""
        pass

    @property
    @abstractmethod
    def start_time(self) -> float:
        """The start time in seconds"""
        pass


class NaiveEstimator(Estimator):
    """A naive estimator that simply estimates the speed and ETA based on the current
    progress and the biased EWA time between steps"""

    def __init__(self, alpha: float = 0.9):
        super().__init__()
        self.alpha = alpha
        self._start_time = -1

    def reset(self, total: int) -> None:
        self._avg_dt = -1
        self._progress = 0
        self._total = total
        self._start_time = time.time()
        self._last = self._start_time

    def tick(self, advance: int = 1) -> None:
        now = time.time()
        if advance > 0:
            new_delta = (now - self._last) / advance
            if self._avg_dt < 0:
                self._avg_dt = new_delta
            else:
                self._avg_dt = self._avg_dt * self.alpha + new_delta * (1 - self.alpha)
        self._progress += advance
        self._last = now

    @property
    def start_time(self) -> float:
        return self._start_time

    @property
    def elapsed(self) -> float:
        return self._last - self._start_time

    @property
    def speed(self) -> float:
        return 1 / self._avg_dt

    @property
    def eta(self) -> float:
        if self._avg_dt < 0:
            return -1
        return self._avg_dt * (self._total - self._progress)


class ProgressReceiver(ABC):
    """A receiver for progress updates"""

    def __init__(self, token: str) -> None:
        super().__init__()
        self._token = token

    @abstractmethod
    def receive(self) -> Optional[ProgressUpdate]:
        """Receive a progress update"""
        pass

    def __next__(self) -> Optional[ProgressUpdate]:
        """Wait for the next progress update"""
        try:
            res = self.receive()
        except Exception as e:
            logger.exception(e)
            res = None
        return res


class ZMQProgressReceiver(ProgressReceiver):
    """A receiver for progress updates over pubsub ZMQ socket"""

    def __init__(self, token: str) -> None:
        try:
            import zmq

            super().__init__(token)
            self._context = zmq.Context()
            self._socket = self._context.socket(zmq.SUB)
            self._socket.connect("tcp://localhost:5556")
            self._socket.subscribe(token.encode())
        except ModuleNotFoundError:  # pragma: no cover
            logger.error(f"{self.__class__.__name__} needs `pyzmq` python package.")
            self._socket = None

    def receive(self) -> Optional[ProgressUpdate]:
        if self._socket is not None:
            _, messagedata = self._socket.recv_multipart()
            return ProgressUpdate.parse_raw(messagedata.decode())


class ListenerCallback:
    """A callback for the listener"""

    def on_start(self) -> None:
        """Called when the listener starts"""
        pass

    def on_update(self, prog: ProgressUpdate) -> None:
        """Called when a progress update is received"""
        pass

    def on_stop(self) -> None:
        """Called when the listener stops"""
        pass


class RichTableListenerCallback(ListenerCallback):
    """A callback for the listener that displays a rich table"""

    def __init__(self) -> None:
        super().__init__()
        self._printer_thread = None
        self.on_stop()

    def on_start(self) -> None:
        self._stop_flag = False
        self._printer_thread = Thread(target=self._print_loop)
        self._printer_thread.start()

    def on_update(self, prog: ProgressUpdate) -> None:
        op = prog.op_info
        if op not in self._estimators:
            estimator = NaiveEstimator()
            estimator.reset(op.total)
            self._estimators[op] = estimator
            self._progress_map[op] = prog

        advance = prog.progress - self._progress_map[op].progress
        if advance > 0:
            self._estimators[op].tick(advance)

        self._progress_map[op] = prog

    def _percentage_string(self, v: float):
        colors = ["red", "yellow", "green"]
        ths = np.linspace(0.0, 1.0, len(colors) + 1)[:-1]

        color = colors[0]
        for th, maybe_color in zip(ths, colors):
            if v > th:
                color = maybe_color

        return f"[{color}]{v*100:.1f}%[/{color}]"

    def _generate_table(self) -> Table:
        title = "Piper Watcher"
        table = Table(
            "Node",
            "Message",
            "Progress",
            "Start Time",
            "Elapsed",
            "Speed",
            "ETA",
            box=box.SIMPLE_HEAVY,
            title=f"[bold red]{title}[/]",
            title_style="on white",
        )
        for op in self._progress_map.keys():
            prog = self._progress_map[op]
            est = self._estimators[op]
            table.add_row(
                op.node,
                op.message,
                self._percentage_string(prog.progress / op.total),
                datetime.fromtimestamp(est.start_time).strftime("%Y-%m-%d %H:%M:%S"),
                str(timedelta(seconds=round(est.elapsed))),
                "N.A." if est.speed < 0 else str(round(est.speed, 2)),
                "N.A." if est.eta < 0 else str(timedelta(seconds=round(est.eta))),
            )

        return table

    def _print_loop(self) -> None:
        with Live(self._generate_table(), refresh_per_second=4) as live:
            while not self._stop_flag:
                time.sleep(0.1)
                live.update(self._generate_table())

    def on_stop(self) -> None:
        self._stop_flag = True
        if self._printer_thread is not None:
            self._printer_thread.join(5.0)
        self._printer_thread = None
        self._progress_map: Dict[OperationInfo, ProgressUpdate] = {}
        self._estimators: Dict[OperationInfo, Estimator] = {}


class Listener:
    """A listener for progress updates"""

    def __init__(
        self, receiver: ProgressReceiver, *callbacks: ListenerCallback
    ) -> None:
        """Initialize the listener

        Args:
            receiver (ProgressReceiver): The progress receiver to use
        """
        self._receiver = receiver
        self._callbacks = callbacks

        self._stop_flag = False
        self._listening_thread = None

    def _listen(self) -> None:
        while not self._stop_flag:
            prog = next(self._receiver)
            if prog is None:
                continue

            for cb in self._callbacks:
                cb.on_update(prog)

    def start(self) -> None:
        """Start the listener in a thread"""
        self._listening_thread = Thread(target=self._listen)
        self._listening_thread.start()

        for cb in self._callbacks:
            cb.on_start()

    def stop(self) -> None:
        """Stop the listener"""
        self._stop_flag = True
        self._listening_thread.join(5.0)
        self._listening_thread = None

        for cb in self._callbacks:
            cb.on_stop()
