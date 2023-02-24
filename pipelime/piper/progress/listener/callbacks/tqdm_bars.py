from typing import Dict
from pipelime.piper.progress.listener.base import ListenerCallback
from pipelime.piper.progress.model import OperationInfo, ProgressUpdate
from pipelime.piper.progress.tracker.base import TqdmTask


class TqdmBarsListenerCallback(ListenerCallback):
    """Listener callback based on tqdm progress bars."""

    def __init__(self) -> None:
        super().__init__()
        self._bars: Dict[OperationInfo, TqdmTask] = {}

    def on_start(self) -> None:
        from rich import print as rp
        from rich.align import Align
        from rich.panel import Panel
        from rich.style import Style
        from rich import box
        from rich.rule import Rule

        rp(
            Panel(
                Align("[red bold][on white]Piper Watcher[/][/]", align="center"),
                box=box.HEAVY,
            )
        )

    def on_update(self, prog: ProgressUpdate):
        if prog.op_info not in self._bars:
            self._bars[prog.op_info] = TqdmTask(
                total=prog.op_info.total,
                message="",
                bar_width=20,
                total_width=-1,
            )

            # adapt the width of the description to the longest one
            mlen = max(self._bars, key=lambda x: len(x.node) + len(x.message))
            mlen = len(mlen.node) + len(mlen.message) + 3
            for k, v in self._bars.items():
                v.set_message((f"[{k.node}] {k.message}").ljust(mlen))

        bar = self._bars[prog.op_info]

        # do not close on "finish", otherwise we won't be able to resize the bars
        if not prog.finished:
            bar.update(prog.progress)

    def on_stop(self):
        pass
