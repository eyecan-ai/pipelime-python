import subprocess
from string import Formatter
from typing import Any, Dict, Mapping, Sequence

from pydantic import Field

from pipelime.piper import PipelimeCommand, PiperPortType


class ShellCommand(PipelimeCommand):
    command: str
    inputs: Dict[str, Any] = Field(default_factory=dict, piper_port=PiperPortType.INPUT)
    outputs: Dict[str, Any] = Field(
        default_factory=dict, piper_port=PiperPortType.OUTPUT
    )

    def get_inputs(self) -> Dict[str, Any]:
        return self.inputs

    def get_outputs(self) -> Dict[str, Any]:
        return self.outputs

    def command_name(self) -> str:
        return self.command

    def _to_command_chunk(self, key: str, value: Any) -> str:
        cmd = ""
        if isinstance(value, Sequence):
            cmd += f" --{key} {value[0]} {self._to_command_chunk(key, value[1:])}"
        elif isinstance(value, Mapping):
            raise NotImplementedError("Mapping values are not supported")
        elif isinstance(value, bool):
            if value:
                cmd += f" --{key}"
        else:
            cmd += f" --{key} {value}"
        return cmd

    def run(self) -> None:
        cmd = self.command
        fields = [fname for _, fname, _, _ in Formatter().parse(cmd) if fname]
        args = {**self.inputs, **self.outputs}
        cmd = cmd.format(**args)

        for key in set(args.keys()).difference(fields):
            value = args[key]
            cmd += self._to_command_chunk(key, value)

        subprocess.run(cmd, shell=True)
