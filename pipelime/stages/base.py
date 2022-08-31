from abc import ABC, abstractmethod

import pydantic as pyd
import typing as t


class SampleStage(pyd.BaseModel, ABC, extra="forbid", copy_on_model_validation=False):
    """Base class for all sample stages."""

    @abstractmethod
    def __call__(self, x: "Sample") -> "Sample":  # type: ignore # noqa: 0602
        pass


class StageIdentity(SampleStage, title="identity"):
    """Returns the input sample."""

    def __call__(self, x: "Sample") -> "Sample":  # type: ignore # noqa: 0602
        return x


class StageLambda(SampleStage, title="lambda"):
    """Applies a callable to the sample."""

    func: t.Callable[["Sample"], "Sample"] = pyd.Field(  # type: ignore # noqa: 0602
        ..., description="The callable to apply."
    )

    def __init__(self, func, **data):
        super().__init__(func=func, **data)  # type: ignore

    def __call__(self, x: "Sample") -> "Sample":  # type: ignore # noqa: 0602
        return self.func(x)


class StageCompose(SampleStage, title="compose"):
    """Applies a sequence of stages."""

    stages: t.Sequence[
        t.Union[SampleStage, t.Mapping[str, t.Optional[t.Mapping[str, t.Any]]]]
    ] = pyd.Field(
        ...,
        description=(
            "The stages to apply. Each stage can be a `<name>: <args>` mapping, "
            "where `<name>` is `compose`, `remap`, `albumentations` etc, "
            "while `<args>` is a mapping of its arguments."
        ),
    )

    @pyd.validator("stages", always=True, each_item=True)
    def _validate_stage(cls, v):
        from pipelime.cli.utils import create_stage_from_config

        return (
            v
            if isinstance(v, SampleStage)
            else create_stage_from_config(*next(iter(v.items())))
        )

    def __init__(self, stages: t.Sequence[SampleStage], **data):
        super().__init__(stages=stages, **data)  # type: ignore

    def __call__(self, x: "Sample") -> "Sample":  # type: ignore # noqa: 0602
        for s in self.stages:
            x = s(x)  # type: ignore
        return x
