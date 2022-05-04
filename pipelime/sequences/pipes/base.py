from pipelime.sequences import SamplesSequence, Sample
import pydantic as pyd


class PipedSequenceBase(SamplesSequence):
    """Reasonable base implementation of `size` and `get_sample`."""

    # subclasses may override and give a proper description
    source: SamplesSequence = pyd.Field(
        ..., description="The source sample sequence.", pipe_source=True
    )

    def size(self) -> int:
        return len(self.source)

    def get_sample(self, idx: int) -> Sample:
        return self.source[idx]
