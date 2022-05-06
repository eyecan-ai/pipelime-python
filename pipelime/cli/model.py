from typing import Iterable

import rich.progress
from pydantic import BaseModel

from pipelime.piper.model import PiperModel
from pipelime.piper.progress import Tracker, TrackCallbackFactory


class CliModel(BaseModel):
    piper: PiperModel

    def __call__(self) -> None:
        self.run()

    def run(self) -> None:
        pass

    def track(self, seq: Iterable, message: str = "") -> Iterable:
        if self.piper.active:
            cb = TrackCallbackFactory.get_callback()
            tracker = Tracker(self.piper.token, self.piper.node, cb)
            return tracker.track(seq, message=message)
        else:
            return rich.progress.track(seq, description=message)
