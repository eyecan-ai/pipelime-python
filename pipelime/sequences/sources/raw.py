import typing as t
import pydantic as pyd

import pipelime.sequences as pls


@pls.source_sequence("from_list")
class SamplesList(pls.SamplesSequence):
    """A SamplesSequence from a list of Samples. Usage::

    sseq = SamplesSequence.from_list([...])
    """

    samples: t.Sequence[pls.Sample] = pyd.Field(
        ..., description="The source sequence of samples."
    )

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, samples: t.Sequence[pls.Sample], **data):
        super().__init__(samples=samples, **data)  # type: ignore

    def size(self) -> int:
        return len(self.samples)

    def get_sample(self, idx: int) -> pls.Sample:
        return self.samples[idx]
