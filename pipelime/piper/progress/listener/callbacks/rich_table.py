import time
from datetime import datetime, timedelta
from threading import Thread
from typing import Dict

import numpy as np
from rich import box
from rich.live import Live
from rich.table import Table

from pipelime.piper.progress.listener.base import ListenerCallback
from pipelime.piper.progress.model import OperationInfo, ProgressUpdate
from pipelime.piper.progress.estimator.base import Estimator
from pipelime.piper.progress.estimator.factory import EstimatorFactory


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
            estimator = EstimatorFactory.get_estimator()
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
