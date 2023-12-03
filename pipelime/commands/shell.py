import subprocess
from string import Formatter
from typing import Any, Dict, Mapping, Sequence

from pydantic import Field

from pipelime.piper import PipelimeCommand, PiperPortType


class ShellCommand(PipelimeCommand, title="shell"):
    """A generic pipelime command wrapping a shell command."""

    command: str = Field(
        ...,
        alias="c",
        description=(
            "The shell command to execute. Use the `{name}` syntax to refer "
            "to specific inputs and outputs, then remaining inputs/outputs keys "
            "are appended as `--key value` arguments."
        ),
    )
    inputs: Dict[str, Any] = Field(
        default_factory=dict,
        alias="i",
        description=(
            "The input options. They will be matched with outputs "
            "of other commands when building a piper graph."
        ),
        piper_port=PiperPortType.INPUT,
    )
    outputs: Dict[str, Any] = Field(
        default_factory=dict,
        alias="o",
        description=(
            "The output options. They will be matched with inputs "
            "of other commands when building a piper graph."
        ),
        piper_port=PiperPortType.OUTPUT,
    )

    def get_inputs(self) -> Dict[str, Any]:
        return self.inputs

    def get_outputs(self) -> Dict[str, Any]:
        return self.outputs

    @PipelimeCommand.command_name.getter
    def command_name(self) -> str:
        return self.command_title() + ":" + self.command.partition(" ")[0]

    def _to_command_chunk(self, key: str, value: Any) -> str:
        cmd = ""
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            if value:
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
            cmd += self._to_command_chunk(key, args[key])

        with self.create_task(1, "Executing") as t:
            subprocess.run(cmd, shell=True)
            t.advance()
