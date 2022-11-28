from __future__ import annotations
from abc import ABC, abstractmethod

import pydantic as pyd
import typing as t
import inspect

if t.TYPE_CHECKING:
    from pipelime.sequences import Sample


class SampleStage(pyd.BaseModel, ABC, extra="forbid", copy_on_model_validation="none"):
    """Base class for all sample stages."""

    @abstractmethod
    def __call__(self, x: "Sample") -> "Sample":
        pass

    def __rshift__(self, other: SampleStage) -> SampleStage:
        """`>>` composes two stages: `other` after `self`."""
        return StageCompose([self, other])

    def __lshift__(self, other: SampleStage) -> SampleStage:
        """`<<` composes two stages: `other` before `self`."""
        return StageCompose([other, self])


class StageIdentity(SampleStage, title="identity"):
    """Returns the input sample."""

    def __call__(self, x: "Sample") -> "Sample":
        return x


class StageLambda(SampleStage, title="lambda"):
    """Applies a callable to the sample."""

    func: t.Callable = pyd.Field(
        ...,
        description="The callable to apply, accepting a Sample and returning a Sample.",
    )

    def __init__(self, func, **data):
        super().__init__(func=func, **data)  # type: ignore

    def __call__(self, x: "Sample") -> "Sample":
        return self.func(x)


class StageInput(pyd.BaseModel, extra="forbid", copy_on_model_validation="none"):
    """A stage is SampleStage object, `<name>` or `<name>: <args>` mapping,
    where `<name>` is `compose`, `remap`, `albumentations` etc,
    while `<args>` is a mapping of its arguments."""

    __root__: SampleStage

    def __call__(self, x: "Sample") -> "Sample":
        return self.__root__(x)

    def __str__(self) -> str:
        return str(self.__root__)

    def __repr__(self) -> str:
        return repr(self.__root__)

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value):
        from pipelime.cli.utils import create_stage_from_config

        if isinstance(value, StageInput):
            return value
        if isinstance(value, SampleStage):
            return StageInput(__root__=value)
        if isinstance(value, (str, bytes)):
            return StageInput(__root__=create_stage_from_config(str(value), None))
        if isinstance(value, t.Mapping):
            return StageInput(
                __root__=create_stage_from_config(*next(iter(value.items())))
            )
        raise ValueError(f"Invalid stage definition: {value}")


class StageCompose(SampleStage, title="compose"):
    """Applies a sequence of stages."""

    stages: t.Sequence[StageInput] = pyd.Field(
        ...,
        description="The stages to apply. " + str(inspect.getdoc(StageInput)),
    )

    def __init__(
        self,
        stages: t.Sequence[
            t.Union[SampleStage, str, t.Mapping[str, t.Mapping[str, t.Any]]]
        ],
        **data,
    ):
        super().__init__(stages=stages, **data)  # type: ignore

    def __call__(self, x: "Sample") -> "Sample":
        for s in self.stages:
            x = s(x)  # type: ignore
        return x
