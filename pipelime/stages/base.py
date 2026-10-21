from __future__ import annotations

import inspect
import typing as t
from abc import ABC, abstractmethod

import pydantic

import pipelime.utils.pydantic_types as pl_types
from pipelime.utils.pydantic_compat import (
    Field,
    PipelimeModel,
    PipelimeRootModel,
    model_title,
)

if t.TYPE_CHECKING:
    from pipelime.sequences import Sample


class SampleStage(PipelimeModel, ABC, extra="forbid", populate_by_name=True):
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

    func: pl_types.CallableDef = Field(
        ...,
        description="The callable to apply, accepting a Sample and returning a Sample.",
    )

    def __init__(self, func, **data):
        super().__init__(func=func, **data)  # type: ignore

    def __call__(self, x: "Sample") -> "Sample":
        return self.func(x)


class StageInput(PipelimeRootModel[SampleStage]):
    """A stage is SampleStage object, `<name>` or `<name>: <args>` mapping,
    where `<name>` is `compose`, `remap`, `albumentations` etc,
    while `<args>` is a mapping of its arguments."""

    def __call__(self, x: "Sample") -> "Sample":
        return self.root(x)

    def __str__(self) -> str:
        return str(self.root)

    def __repr__(self) -> str:
        return repr(self.root)

    @classmethod
    def _coerce(cls, value):
        from pipelime.cli.utils import create_stage_from_config

        if isinstance(value, SampleStage):
            return value
        if isinstance(value, (str, bytes)):
            return create_stage_from_config(str(value), None)
        if isinstance(value, t.Mapping):
            return create_stage_from_config(*next(iter(value.items())))
        raise ValueError(f"Invalid stage definition: {value}")

    @pydantic.model_serializer(mode="wrap")
    def _serialize(self, handler) -> t.Dict[str, t.Any]:
        # `{<stage title>: <stage args>}`, the shape accepted back by `_coerce`
        return {model_title(type(self.root)): handler(self)}

    def dict(self, *args, **kwargs) -> t.Mapping:  # type: ignore[override]
        # v1 overrode `dict()` with the `{title: args}` shape (no `__root__` envelope)
        return self.model_dump(*args, **kwargs)


class StageCompose(SampleStage, title="compose"):
    """Applies a sequence of stages."""

    stages: t.Sequence[StageInput] = Field(
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

    stage: StageInput = Field(
        ..., description="The stage to time. " + str(inspect.getdoc(StageInput))
    )
    skip_first: pydantic.NonNegativeInt = Field(
        1, description="Skip the first n samples, then start the timer."
    )
    time_key_path: str = Field(
        "timings.*",
        description=(
            "The item metadata key path where the time will be written to. "
            "Any `*` will be replaced with the name of the stage."
        ),
    )
    process: bool = Field(
        False,
        description=(
            "Measure process time instead of using a performance counter clock."
        ),
    )

    _skipped: int = pydantic.PrivateAttr(0)

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

        stage_name = model_title(type(stg))

        x = x.deep_set(
            self.time_key_path.replace("*", stage_name), end_time - start_time
        )
        return x
