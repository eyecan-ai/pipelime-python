from abc import ABC, abstractmethod

import pydantic as pyd
import typing as t


class SampleStage(pyd.BaseModel, ABC, extra="forbid", copy_on_model_validation=False):
    """Base class for all sample stages."""

    @abstractmethod
    def __call__(self, x: "Sample") -> "Sample":  # type: ignore # noqa: 0602
        pass


class StageIdentity(SampleStage):
    """Returns the input sample."""

    def __call__(self, x: "Sample") -> "Sample":  # type: ignore # noqa: 0602
        return x


class StageLambda(SampleStage):
    """Applies a callable to the sample."""

    func: t.Callable[["Sample"], "Sample"] = pyd.Field(  # type: ignore # noqa: 0602
        ..., description="The callable to apply."
    )

    def __init__(self, func, **data):
        super().__init__(func=func, **data)  # type: ignore

    def __call__(self, x: "Sample") -> "Sample":  # type: ignore # noqa: 0602
        return self.func(x)


class StageCompose(SampleStage):
    """Applies a sequence of stages."""

    stages: t.Sequence[SampleStage] = pyd.Field(..., description="The stages to apply.")

    def __init__(self, *stages: SampleStage, **data):
        super().__init__(stages=stages, **data)  # type: ignore

    def __call__(self, x: "Sample") -> "Sample":  # type: ignore # noqa: 0602
        for s in self.stages:
            x = s(x)
        return x
