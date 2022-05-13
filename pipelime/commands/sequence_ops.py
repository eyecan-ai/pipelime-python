import typing as t
from pathlib import Path

from pydantic import Field, PrivateAttr

import pipelime.commands.interfaces as pl_interfaces
from pipelime.piper import PipelimeCommand, PiperPortType


class PipeCommand(PipelimeCommand, title="pipe"):
    """A general purpose command to build up linear pipelines."""

    input: pl_interfaces.InputDatasetInterface = Field(
        ..., description="Input dataset.", piper_port=PiperPortType.INPUT
    )
    output: pl_interfaces.OutputDatasetInterface = Field(
        ..., description="Output dataset.", piper_port=PiperPortType.OUTPUT
    )
    grabber: pl_interfaces.GrabberInterface = Field(
        default_factory=pl_interfaces.GrabberInterface,  # type: ignore
        description="Grabber options.",
    )
    operations: str = Field(
        ...,
        description="The pipeline to run as a one-line YAML/JSON string "
        "or a path to a YAML/JSON file (use <filepath>:<key-path> to load a "
        "from a pydash-like path).\n"
        "Can be an ordered mapping or a sequence of ordered mappings. "
        "Each key is the name of the operation to run, while the value "
        "gathers the arguments to pass to the operation, which might be "
        "a single value, a sequence of values or a mapping.",
    )

    _pipe_list: t.Union[
        str, t.Mapping[str, t.Any], t.Sequence[t.Union[str, t.Mapping[str, t.Any]]]
    ] = PrivateAttr()

    def __init__(self, **data):
        import yaml
        import pydash as py_

        super().__init__(**data)

        filepath, _, root_key = (
            self.operations.rpartition(":")
            if ":" in self.operations
            else (self.operations, None, None)
        )
        filepath = Path(filepath)
        if filepath.exists():
            with filepath.open() as f:
                self._pipe_list = yaml.safe_load(f)
                if root_key is not None:
                    self._pipe_list = py_.get(  # type: ignore
                        self._pipe_list, root_key, default=None
                    )
        else:
            self._pipe_list = yaml.safe_load(str(self.operations))

        if not self._pipe_list:
            raise ValueError(f"Invalid pipeline: {self.operations}")

    def run(self):
        from pipelime.sequences import build_pipe

        seq = self.input.create_reader()
        seq = build_pipe(self._pipe_list, seq)
        seq = self.output.append_writer(seq)

        with self.output.serialization_cm():
            self.grabber.grab_all(
                seq,
                keep_order=False,
                parent_cmd=self,
                track_message="Writing results...",
            )
