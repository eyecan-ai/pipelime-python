from typing import Dict
from pipelime.piper.progress.listener.base import ListenerCallback
from pipelime.piper.progress.model import OperationInfo, ProgressUpdate
from pipelime.piper.progress.tracker.base import TqdmTask


class TqdmBarsListenerCallback(ListenerCallback):
    """Listener callback based on tqdm progress bars."""

    def __init__(self, show_token: bool = False) -> None:
        super().__init__(show_token)
        self._bars: Dict[OperationInfo, TqdmTask] = {}

    def on_start(self) -> None:
        from rich import print as rp
        from rich.align import Align
        from rich.panel import Panel
        from rich import box

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
            mlen = max(self._bars, key=lambda x: len(self._build_message(x)))
            mlen = len(self._build_message(mlen))
            for k, v in self._bars.items():
                v.set_message(self._build_message(k).ljust(mlen))

        bar = self._bars[prog.op_info]

        # do not close on "finish", otherwise we won't be able to resize the bars
        if not prog.finished:
            bar.update(prog.progress)

    def on_stop(self):
        for bar in self._bars.values():
            bar.finish()

    def _build_message(self, x: OperationInfo):
        chunk = "" if x.chunk <= 0 else f"${x.chunk}"
        return (
            "["
            + (f"{x.token}/" if self.show_token else "")
            + f"{x.node}{chunk}] {x.message}"
        )
