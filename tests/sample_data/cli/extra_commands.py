from pydantic import Field

from pipelime.commands.interfaces import InputDatasetInterface, OutputDatasetInterface
from pipelime.piper import PipelimeCommand, PiperPortType


class RandomSlice(PipelimeCommand, title="randrange"):
    """Extracts a random range from the input dataset."""

    input: InputDatasetInterface = Field(
        ..., description="Input dataset.", piper_port=PiperPortType.INPUT
    )
    output: OutputDatasetInterface = Field(
        ..., description="Output dataset.", piper_port=PiperPortType.OUTPUT
    )
    seed: int = Field(42, description="Random seed.")

    def run(self):
        import random

        seq = self.input.create_reader()

        random.seed(self.seed)
        seq = seq[0 : random.randrange(0, len(seq))]  # noqa: E203
        seq = self.output.append_writer(seq)

        for _ in seq:
            pass
