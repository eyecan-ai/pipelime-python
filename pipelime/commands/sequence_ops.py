import typing as t

from pydantic import Field

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
    operations: t.Union[
        t.Mapping[str, t.Any], t.Sequence[t.Union[str, t.Mapping[str, t.Any]]]
    ] = Field(
        ...,
        description="The pipeline to run. "
        "Can be an ordered mapping or a sequence of ordered mappings. "
        "Each key is the name of the operation to run, while the value "
        "gathers the arguments to pass to the operation, which might be "
        "a single value, a sequence of values or a mapping.",
    )

    def run(self):
        pass
