import time
from threading import Thread

import numpy as np
import rich
from pydantic import BaseModel
from rich.live import Live
from rich.table import Table

from pipelime.pipes.communication import PiperCommunicationChannelFactory


class ChunkProgress(BaseModel):
    id_: str
    chunk_index: int
    progress: float

    @property
    def file(self):
        return self.id_.split(":")[0]

    @property
    def method(self):
        return self.id_.split(":")[1]

    @property
    def unique(self):
        return self.id_.split(":")[2]


class Watcher:
    def __init__(self, token: str) -> None:
        self._tasks_map = {}
        self._stop_flag = False

        self._channel = PiperCommunicationChannelFactory.create_channel(token)

        self._listener = None
        self._printer = None

    def _percentage_string(self, v: float):
        colors = ["red", "yellow", "green"]
        ths = np.linspace(0.0, 1.0, len(colors) + 1)[:-1]

        color = colors[0]
        for th, maybe_color in zip(ths, colors):
            if v > th:
                color = maybe_color

        return f"[{color}]{v*100:.1f}%[/{color}]"

    def _generate_table(self) -> Table:
        """Make a new table."""
        table = Table()
        table.add_column("File")
        table.add_column("Method")
        table.add_column("ID")
        table.add_column("Progress")

        for task_id in self._tasks_map.keys():
            progress = [
                self._percentage_string(v) for v in self._tasks_map[task_id].values()
            ]
            progress = " ‖ ".join(progress)

            filename, method, unique = task_id.split(":")
            table.add_row(f"{filename}", f"{method}", f"{unique}", f"{progress}")

        return table

    def _callback(self, data: dict):
        payload = data["payload"]
        if "_progress" in payload:
            progress = payload["_progress"]

            chunk_progress = ChunkProgress(
                id_=data["id"],
                chunk_index=progress["chunk_index"],
                progress=progress["progress_data"]["advance"]
                / progress["progress_data"]["total"],
            )

            if chunk_progress.id_ not in self._tasks_map:
                self._tasks_map[chunk_progress.id_] = {}
            if chunk_progress.chunk_index not in self._tasks_map[chunk_progress.id_]:
                self._tasks_map[chunk_progress.id_][chunk_progress.chunk_index] = 0.0

            self._tasks_map[chunk_progress.id_][
                chunk_progress.chunk_index
            ] += chunk_progress.progress

        elif "event" in payload:
            event = payload["event"]

            if event not in self._tasks_map:
                self._tasks_map[event] = {0: 0.0}

            self._tasks_map[event][0] += 1

    def _listener_thread(self):
        self._channel.register_callback(self._callback)
        self._channel.listen()

    def _printer_thread(self):
        with Live(self._generate_table(), refresh_per_second=4) as live:
            while not self._stop_flag:
                time.sleep(0.1)
                live.update(self._generate_table())

        self._channel.close()

    def stop(self) -> None:
        self._stop_flag = True
        self._listener.join()
        self._printer.join()

    def watch(self):
        self._stop_flag = False
        self._listener = Thread(target=self._listener_thread, daemon=True)
        self._printer = Thread(target=self._printer_thread, daemon=True)

        self._listener.start()
        self._printer.start()
