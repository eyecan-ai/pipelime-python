from abc import ABC, abstractmethod
from pipelime.sequences import Sample

import pydantic as pyd
import typing as t


class SampleStage(pyd.BaseModel, ABC, extra="forbid"):
    """Base class for all sample stages."""

    @abstractmethod
    def __call__(self, x: Sample) -> Sample:
        pass


class StageIdentity(SampleStage):
    """Returns the input sample."""

    def __call__(self, x: Sample) -> Sample:
        return x


class StageLambda(SampleStage):
    """Applies a callable to the sample."""

    func: t.Callable[[Sample], Sample] = pyd.Field(
        ..., description="The callable to apply."
    )

    def __init__(self, func, **data):
        super().__init__(func=func, **data)  # type: ignore

    def __call__(self, x: Sample) -> Sample:
        return self.func(x)


class StageCompose(SampleStage):
    """Applies a sequence of stages."""

    stages: t.Sequence[SampleStage] = pyd.Field(..., description="The stages to apply.")

    def __init__(self, *stages: SampleStage, **data):
        super().__init__(stages=stages, **data)  # type: ignore

    def __call__(self, x: Sample) -> Sample:
        for s in self.stages:
            x = s(x)
        return x
