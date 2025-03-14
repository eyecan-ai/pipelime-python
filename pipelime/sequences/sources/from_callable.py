import typing as t

import pydantic.v1 as pyd

import pipelime.sequences as pls


@pls.source_sequence
class SequenceFromCallable(pls.SamplesSequence, title="from_callable"):
    """A SamplesSequence calling a user-defined generator to get the samples."""

    generator_fn: t.Callable[[int], pls.Sample] = pyd.Field(
        ..., description="A callable returning a sample for a given index."
    )
    length: t.Union[int, t.Callable[[], int]] = pyd.Field(
        ...,
        description=(
            "The length of the sequence, either an integer "
            "or a callable returning an integer."
        ),
    )

    def size(self) -> int:
        return self.length if isinstance(self.length, int) else self.length()

    def get_sample(self, idx: int) -> pls.Sample:
        return self.generator_fn(idx)
