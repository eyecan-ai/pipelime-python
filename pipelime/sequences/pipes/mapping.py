import typing as t
import pydantic as pyd

import pipelime.sequences as pls
from pipelime.sequences.pipes import PipedSequenceBase
from pipelime.stages import StageInput


@pls.piped_sequence
class MappedSequence(PipedSequenceBase, title="map"):
    """Applies a stage on all samples."""

    stage: StageInput = pyd.Field(...)

    def __init__(self, stage: StageInput, **data):
        super().__init__(stage=stage, **data)  # type: ignore

    def get_sample(self, idx: int) -> pls.Sample:
        return self.stage(self.source[idx])


any_condition_t = t.Union[
    t.Callable[[], bool],
    t.Callable[[int], bool],
    t.Callable[[int, pls.Sample], bool],
    t.Callable[[int, pls.Sample, pls.SamplesSequence], bool],
]


@pls.piped_sequence
class ConditionallyMappedSequence(PipedSequenceBase, title="map_if"):
    """Applies a stage on all samples if a condition returns True."""

    stage: StageInput = pyd.Field(...)
    condition: any_condition_t = pyd.Field(
        ...,
        description=(
            "A callable that returns True if the sample should be mapped through "
            "the stage. Accepted signatures `() -> bool`, `(index: int) -> bool`, "
            "`(index: int, sample: Sample) -> bool`, "
            "`(index: int, sample: Sample, source: SamplesSequence) -> bool`."
        ),
    )

    # This convoluted implementation is needed to support pickling and multiprocessing
    _call_condition = pyd.PrivateAttr()

    @staticmethod
    def _call0(
        fn: t.Callable[[], bool], idx: int, x: pls.Sample, s: pls.SamplesSequence
    ) -> bool:
        return fn()

    @staticmethod
    def _call1(
        fn: t.Callable[[int], bool], idx: int, x: pls.Sample, s: pls.SamplesSequence
    ) -> bool:
        return fn(idx)

    @staticmethod
    def _call2(
        fn: t.Callable[[int, pls.Sample], bool],
        idx: int,
        x: pls.Sample,
        s: pls.SamplesSequence,
    ) -> bool:
        return fn(idx, x)

    @staticmethod
    def _call3(
        fn: t.Callable[[int, pls.Sample, pls.SamplesSequence], bool],
        idx: int,
        x: pls.Sample,
        s: pls.SamplesSequence,
    ) -> bool:
        return fn(idx, x, s)

    def __init__(self, **data):
        from inspect import signature, Parameter

        super().__init__(**data)

        prms = signature(self.condition).parameters
        has_var_pos = any(p.kind == Parameter.VAR_POSITIONAL for p in prms.values())
        if len(prms) > 4 or (len(prms) == 4 and not has_var_pos):
            raise ValueError(
                f"Invalid signature for `condition`: {signature(self.condition)}"
            )

        if len(prms) == 3 or has_var_pos:
            self._call_condition = self._call3
        elif len(prms) == 2:
            self._call_condition = self._call2
        elif len(prms) == 1:
            self._call_condition = self._call1
        else:
            self._call_condition = self._call0

    def get_sample(self, idx: int) -> pls.Sample:
        x = self.source[idx]
        if self._call_condition(self.condition, idx, x, self.source):  # type: ignore
            return self.stage(x)
        return x


class MappingConditionProbability(pyd.BaseModel):
    """A condition that returns True with a given probability."""

    probability: float = pyd.Field(
        ...,
        alias="p",
        ge=0.0,
        le=1.0,
        description="Probability that the sample will be mapped through the stage.",
    )
    seed: t.Optional[int] = pyd.Field(None, description="Random seed.")

    _generator = pyd.PrivateAttr(None)

    def __call__(self) -> bool:
        # create the random generator here so that every process has its own
        if self._generator is None:
            import random

            self._generator = random.Random(self.seed)
        return self._generator.random() < self.probability


class MappingConditionIndexRange(pyd.BaseModel):
    """A condition that returns True if the sample index is in a given range.
    Negative indices are supported.
    """

    start: int = pyd.Field(0, description="First index (inclusive).")
    stop: t.Optional[int] = pyd.Field(None, description="Last index (exclusive).")
    step: int = pyd.Field(1, description="Step.")

    def __call__(self, idx: int, _: pls.Sample, seq: pls.SamplesSequence) -> bool:
        start = (len(seq) + self.start) if self.start < 0 else self.start
        stop = (
            len(seq)
            if self.stop is None
            else ((len(seq) + self.stop) if self.stop < 0 else self.stop)
        )
        return idx in range(start, stop, self.step)
