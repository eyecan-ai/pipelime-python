from __future__ import annotations

import inspect
import typing as t
from abc import ABC, abstractmethod

import pydantic as pyd

import pipelime.utils.pydantic_types as pl_types

if t.TYPE_CHECKING:
    from pipelime.sequences import Sample


class SampleStage(
    pyd.BaseModel,
    ABC,
    extra="forbid",
    copy_on_model_validation="none",
    allow_population_by_field_name=True,
):
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

    func: pl_types.CallableDef = pyd.Field(
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

    def dict(self, *args, **kwargs) -> t.Mapping:
        return {self.__root__.__config__.title: self.__root__.dict(*args, **kwargs)}


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


class StageTimer(SampleStage, title="timer"):
    """Times the stage execution and writes the nanoseconds to the sample metadata."""

    stage: StageInput = pyd.Field(
        ..., description="The stage to time. " + str(inspect.getdoc(StageInput))
    )
    skip_first: pyd.NonNegativeInt = pyd.Field(
        1, description="Skip the first n samples, then start the timer."
    )
    time_key_path: str = pyd.Field(
        "timings.*",
        description=(
            "The item metadata key path where the time will be written to. "
            "Any `*` will be replaced with the name of the stage."
        ),
    )
    process: bool = pyd.Field(
        False,
        description=(
            "Measure process time instead of using a performance counter clock."
        ),
    )

    _skipped: int = pyd.PrivateAttr(0)

    def __init__(
        self,
        stage: t.Union[SampleStage, str, t.Mapping[str, t.Mapping[str, t.Any]]],
        **data,
    ):
        super().__init__(stage=stage, **data)  # type: ignore

    def __call__(self, x: "Sample") -> "Sample":
        import time

        stg = self.stage.__root__

        if self._skipped < self.skip_first:
            self._skipped += 1
            return stg(x)

        clock_fn = time.process_time_ns if self.process else time.perf_counter_ns

        start_time = clock_fn()
        x = stg(x)
        end_time = clock_fn()

        stage_cls = self.stage.__root__.__class__
        stage_name = (
            stage_cls.__config__.title
            if stage_cls.__config__.title
            else stage_cls.__name__
        )

        x = x.deep_set(
            self.time_key_path.replace("*", stage_name), end_time - start_time
        )
        return x
