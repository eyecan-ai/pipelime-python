import subprocess
from string import Formatter
from typing import Any, Dict, Iterable, Sequence

import rich.progress
from pydantic import BaseModel, Field

from pipelime.choixe import XConfig
from pipelime.piper.progress.tracker import TrackCallbackFactory, Tracker


class PiperInfo(BaseModel):
    token: str = ""
    node: str = ""

    @property
    def active(self) -> bool:
        return len(self.token) > 0


class PipelimeCommand(BaseModel):
    piper: PiperInfo = PiperInfo()

    def run(self) -> None:
        pass

    def _filter_fields_by_flag(self, flag: str) -> Iterable[str]:
        for k, v in self.__fields__.items():
            if v.field_info.extra.get(flag, False):
                yield k

    def _get_fields_by_flag(self, flag: str) -> Dict[str, Any]:
        return {k: getattr(self, k) for k in self._filter_fields_by_flag(flag)}

    def get_inputs(self) -> Dict[str, Any]:
        return self._get_fields_by_flag("piper_input")

    def get_outputs(self) -> Dict[str, Any]:
        return self._get_fields_by_flag("piper_output")

    def command_name(self) -> str:
        return self.__class__.__name__

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
    inputs: Dict[str, Any] = Field(default_factory=dict)
    outputs: Dict[str, Any] = Field(default_factory=dict)

    def get_inputs(self) -> Dict[str, Any]:
        return self.inputs

    def get_outputs(self) -> Dict[str, Any]:
        return self.outputs

    def command_name(self) -> str:
        return self.command

    def _to_command_chunk(self, key: str, value: Any) -> str:
        if isinstance(value, Sequence):
            cmd += f" --{key} {value[0]} {self._to_command_chunk(key, value[1:])}"

        elif isinstance(value, Dict):
            raise NotImplementedError("Dict values are not supported")

        elif isinstance(value, bool):
            cmd += f" --{key}"

        else:
            cmd += f" --{key} {value}"

    def run(self) -> None:
        cmd = self.command
        fields = [fname for _, fname, _, _ in Formatter().parse(cmd) if fname]
        args = {**self.inputs, **self.outputs}
        cmd = cmd.format(**args)

        for key in set(args.keys()).difference(fields):
            value = args[key]
            cmd += self._to_command_chunk(key, value)

        subprocess.run(cmd, shell=True)


class DAGModel(BaseModel):
    nodes: Dict[str, PipelimeCommand]

    def purged_dict(self):
        return XConfig(data={"nodes": self.nodes}).to_dict()
