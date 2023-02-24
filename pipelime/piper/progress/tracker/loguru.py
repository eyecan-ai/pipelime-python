from loguru import logger

from pipelime.piper.progress.model import ProgressUpdate
from pipelime.piper.progress.tracker.base import TrackCallback


class LoguruTrackCallback(TrackCallback):
    """Loguru tracker callback"""

    def __init__(self, level: str = "INFO") -> None:
        super().__init__()
        self._level = level

    def update(self, prog: ProgressUpdate):
        logger.log(
            self._level,
            "Token: {} | Node: {} | Chunk: {} | {} | It: {}/{} [{}]",
            prog.op_info.token,
            prog.op_info.node,
            prog.op_info.chunk,
            prog.op_info.message,
            prog.progress,
            prog.op_info.total,
            "COMPLETED" if prog.finished else "RUNNING",
        )
