import typing as t

import pydantic as pyd

import pipelime.sequences as pls


@pls.source_sequence
class SamplesList(pls.SamplesSequence, title="from_list", arbitrary_types_allowed=True):
    """A SamplesSequence from a list of Samples."""

    samples: t.Sequence[pls.Sample] = pyd.Field(
        ..., description="The source sequence of samples."
    )

    def __init__(self, samples: t.Sequence[pls.Sample], **data):
        super().__init__(samples=samples, **data)  # type: ignore

    def size(self) -> int:
        return len(self.samples)

    def get_sample(self, idx: int) -> pls.Sample:
        return self.samples[idx]
