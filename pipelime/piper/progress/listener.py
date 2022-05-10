import json
import time
from abc import ABC, abstractmethod
from threading import Thread
from typing import Dict, Optional

import numpy as np
import zmq
from loguru import logger
from rich.live import Live
from rich.table import Table

from pipelime.piper.progress.model import OperationInfo, ProgressUpdate


class ProgressReceiver(ABC):
    def __init__(self, token: str) -> None:
        super().__init__()
        self._token = token

    @abstractmethod
    def receive(self) -> Optional[ProgressUpdate]:
        pass

    def __next__(self) -> Optional[ProgressUpdate]:
        try:
            res = self.receive()
        except Exception as e:
            logger.exception(e)
            res = None
        return res


class ZMQProgressReceiver(ProgressReceiver):
    def __init__(self, token: str) -> None:
        super().__init__(token)
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.SUB)
        self._socket.connect("tcp://localhost:5556")
        self._socket.subscribe(token.encode())

    def receive(self) -> Optional[ProgressUpdate]:
        _, messagedata = self._socket.recv_multipart()
        messagedata = json.loads(messagedata.decode())
        return ProgressUpdate.from_json(messagedata)


class ListenerCallback:
    def on_start(self) -> None:
        pass

    def on_advance(self, prog: ProgressUpdate) -> None:
        pass

    def on_stop(self) -> None:
        pass


class RichTableListenerCallback(ListenerCallback):
    def __init__(self) -> None:
        super().__init__()
        self.on_stop()

    def on_start(self) -> None:
        self._stop_flag = False
        self._printer_thread = Thread(target=self._print_loop)
        self._printer_thread.start()

    def on_advance(self, prog: ProgressUpdate) -> None:
        if prog not in self._progress_map:
            self._progress_map[prog] = 0
        self._progress_map[prog.op_info] += prog.advance

    def _percentage_string(self, v: float):
        colors = ["red", "yellow", "green"]
        ths = np.linspace(0.0, 1.0, len(colors) + 1)[:-1]

        color = colors[0]
        for th, maybe_color in zip(ths, colors):
            if v > th:
                color = maybe_color

        return f"[{color}]{v*100:.1f}%[/{color}]"

    def _generate_table(self) -> Table:
        table = Table()
        table.add_column("Node")
        table.add_column("Chunk")
        table.add_column("Message")
        table.add_column("Progress")

        for op in self._progress_map.keys():
            progress = self._progress_map[op]
            progress_perc = self._percentage_string(progress / op.total)
            table.add_row(
                f"{op.node}", f"{op.chunk}", f"{op.message}", f"{progress_perc}"
            )

        return table

    def _print_loop(self) -> None:
        with Live(self._generate_table(), refresh_per_second=4) as live:
            while not self._stop_flag:
                time.sleep(0.1)
                live.update(self._generate_table())

    def on_stop(self) -> None:
        self._stop_flag = True
        self._printer_thread.join(5.0)
        self._printer_thread = None
        self._progress_map: Dict[OperationInfo, int] = {}


class Listener:
    def __init__(
        self, receiver: ProgressReceiver, *callbacks: ListenerCallback
    ) -> None:
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
                cb.on_advance(prog)

    def start(self) -> None:
        self._listening_thread = Thread(target=self._listen)
        self._listening_thread.start()

        for cb in self._callbacks:
            cb.on_start()

    def stop(self) -> None:
        self._stop_flag = True
        self._listening_thread.join(5.0)
        self._listening_thread = None

        for cb in self._callbacks:
            cb.on_stop()
