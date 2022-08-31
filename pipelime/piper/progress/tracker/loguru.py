from loguru import logger

from pipelime.piper.progress.model import ProgressUpdate
from pipelime.piper.progress.tracker.base import TrackCallback


class LoguruTrackCallback(TrackCallback):
    """Loguru tracker callback"""

    def __init__(self, level: str = "INFO") -> None:
        super().__init__()
        self._level = level

    def on_start(self, prog: ProgressUpdate) -> None:
        logger.log(
            self._level,
            "Token: {} | Node: {} | Chunk: {} | {} | Started.",
            prog.op_info.token,
            prog.op_info.node,
            prog.op_info.chunk,
            prog.op_info.message,
        )

    def on_advance(self, prog: ProgressUpdate):
        logger.log(
            self._level,
            "Token: {} | Node: {} | Chunk: {} | {} | Advanced of {} steps.",
            prog.op_info.token,
            prog.op_info.node,
            prog.op_info.chunk,
            prog.op_info.message,
            prog.progress,
        )

    def on_finish(self, prog: ProgressUpdate) -> None:
        logger.log(
            self._level,
            "Token: {} | Node: {} | Chunk: {} | {} | Finished.",
            prog.op_info.token,
            prog.op_info.node,
            prog.op_info.chunk,
            prog.op_info.message,
        )
