from pydantic.v1 import Field

from pipelime.sequences import Sample, piped_sequence
from pipelime.sequences.pipes import PipedSequenceBase


@piped_sequence
class ReverseSequence(PipedSequenceBase, title="reversed"):
    """Reverses the order of the first `num` samples."""

    num: int = Field(..., description="The number of samples to reverse.")

    def get_sample(self, idx: int) -> Sample:
        if idx < self.num:
            return self.source[self.num - idx - 1]
        return self.source[idx]
