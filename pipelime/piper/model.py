import subprocess
from typing import Dict, Iterable

import rich.progress
from pydantic import BaseModel

from pipelime.piper.model import PiperInfo
from pipelime.piper.progress import TrackCallbackFactory, Tracker


class PiperInfo(BaseModel):
    token: str = ""
    node: str = ""

    @property
    def active(self) -> bool:
        return len(self.token) > 0


class PipelimeCommand(BaseModel):
    piper: PiperInfo

    def run(self) -> None:
        pass

    def get_inputs(self) -> Iterable[str]:
        # TODO: implement
        pass

    def get_outputs(self) -> Iterable[str]:
        # TODO: implement
        pass

    def track(self, seq: Iterable, message: str = "") -> Iterable:
        if self.piper.active:
            cb = TrackCallbackFactory.get_callback()
            tracker = Tracker(self.piper.token, self.piper.node, cb)
            return tracker.track(seq, message=message)
        else:
            return rich.progress.track(seq, description=message)

    def __call__(self) -> None:
        self.run()


class ShellCommand(PipelimeCommand):
    command: str
    inputs: Iterable[str]
    outputs: Iterable[str]

    def get_inputs(self) -> Iterable[str]:
        return self.inputs

    def get_outputs(self) -> Iterable[str]:
        return self.outputs

    def run(self) -> None:
        subprocess.run(self.command)


class DAGModel(BaseModel):
    nodes: Dict[str, PipelimeCommand]

    def purged_dict(self):
        return self.dict(exclude_unset=True, exclude_none=True)
