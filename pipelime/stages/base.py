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
    populate_by_name=True,
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


class StageInput(pyd.RootModel[SampleStage]):
    """A stage is SampleStage object, `<name>` or `<name>: <args>` mapping,
    where `<name>` is `compose`, `remap`, `albumentations` etc,
    while `<args>` is a mapping of its arguments."""

    def __call__(self, x: "Sample") -> "Sample":
        return self.root(x)

    def __str__(self) -> str:
        return str(self.root)

    def __repr__(self) -> str:
        return repr(self.root)

    @pyd.model_validator(mode="before")
    @classmethod
    def _coerce(cls, value):
        from pipelime.cli.utils import create_stage_from_config

        if isinstance(value, StageInput):
            return value.root
        if isinstance(value, SampleStage):
            return value
        if isinstance(value, (str, bytes)):
            return create_stage_from_config(str(value), None)
        if isinstance(value, t.Mapping):
            return create_stage_from_config(*next(iter(value.items())))
        raise ValueError(f"Invalid stage definition: {value}")

    @pyd.model_serializer(mode="plain")
    def _serialize(self, info: pyd.SerializationInfo) -> t.Mapping:
        return {
            self.root.model_config.get("title"): self.root.model_dump(
                by_alias=bool(info.by_alias),
                exclude_none=bool(info.exclude_none),
                exclude_defaults=bool(info.exclude_defaults),
            )
        }


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

        stg = self.stage.root

        if self._skipped < self.skip_first:
            self._skipped += 1
            return stg(x)

        clock_fn = time.process_time_ns if self.process else time.perf_counter_ns

        start_time = clock_fn()
        x = stg(x)
        end_time = clock_fn()

        stage_cls = self.stage.root.__class__
        stage_name = (
            stage_cls.model_config.get("title")
            if stage_cls.model_config.get("title")
            else stage_cls.__name__
        )

        x = x.deep_set(
            self.time_key_path.replace("*", stage_name), end_time - start_time
        )
        return x
