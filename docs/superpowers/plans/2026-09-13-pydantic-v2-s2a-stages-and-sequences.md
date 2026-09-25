# S2a — Stages and sequences

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert `pipelime/stages/*` and `pipelime/sequences/*` to native pydantic v2 on top of the toolkit (S1), preserving every v1 behaviour pinned by the contract tests.

**Architecture:** `SampleStage`, `SamplesSequence`, `BaseEntity`, `ParsedItem`, `EntityAction`, `Grabber`, `DataStream`, `ItemInfo` become `PipelimeModel`s; `StageInput` and `Transformation` become `PipelimeRootModel`s with a `_coerce` hook and a serializer; `StageEntity` becomes a regular stage with a compat `__init__`. `to_pipe()`/`piped_sequence()` move to `iter_fields`. The stage/command registry (`pipelime.cli.utils.PipelimeSymbolsHelper`) still imports `pipelime.commands` (v1 until S2b), so **the only gates in S2a are import smoke checks and tests that construct stages/sequences directly**; the real S2 gate is at the end of S2b.

**Tech Stack:** pydantic 2.x, pydantic-extra-types (`Color`), albumentations.

**Spec:** `docs/superpowers/specs/2026-09-12-pydantic-v2-migration-design.md` §4.3, §4.4.

## Global Constraints

- MRO of `SamplesSequence(SamplesSequenceBase, PipelimeModel, ...)` and `DataStream(t.Sequence[Sample], PipelimeModel, ...)` stays non-pydantic-base-first.
- Remove every `copy_on_model_validation=`, `underscore_attrs_are_private=`, `allow_population_by_field_name=` (→ `populate_by_name=`) class kwarg; v2 forwards unknown class kwargs to `__init_subclass__` and would raise.
- Inside validators/`_coerce` hooks raise `ValueError` (never `TypeError`): pydantic v2 turns only `ValueError`/`AssertionError` into `ValidationError`, whereas v1 also converted `TypeError`.
- Mechanical rules applied to every file in this subtask: `import pydantic.v1 as pyd` → `import pydantic as pyd`; `from pydantic.v1 import X` → `from pydantic import X`; `from pydantic.v1.generics import GenericModel` → removed (use `PipelimeModel, t.Generic[...]`); `@pyd.validator("f")` → `@pyd.field_validator("f")` + `@classmethod`; `@pyd.validator("f", always=True)` → `@pyd.field_validator("f")` + `@classmethod` **and** `validate_default=True` added to that field's `Field(...)`; validator signature `(cls, v, values)` → `(cls, v, info: pyd.ValidationInfo)` with `values` → `info.data`; `@pyd.root_validator` → `@pyd.model_validator(mode="after")` on an instance method returning `self`; `.dict(` → `.model_dump(`; `.json(` → `.model_dump_json(`; `X.__config__.title` → `model_title(X)`; `X.__fields__` → `iter_fields(X)`/`X.model_fields`; `pyd.Field(..., <pipelime flag>=...)` → `Field(...)` from `pipelime.utils.pydantic_compat`.
- Expected while S2a is in progress: `import pipelime.stages` fails until both stages and sequences are converted (they import each other); any test that resolves a stage by *name* fails with an `ImportError` from `pipelime.commands`/`pipelime.piper` until S2b — those failures are expected; any other failure must be fixed before moving on.
- Install `pydantic-extra-types` into the venv before T3: `.venv/bin/python -m pip install pydantic-extra-types` (it is added to `pyproject.toml` in S5).

---

### Task S2a-T1: `stages/base.py`

**Files:**
- Modify: `pipelime/stages/base.py`

**Interfaces:**
- Produces: `SampleStage(PipelimeModel, ABC, extra="forbid", populate_by_name=True)`, `StageInput(PipelimeRootModel[SampleStage])` with `_coerce`, `.root`/`.__root__`, `dict()` == `model_dump()` (v1 shape `{title: args}`).

- [ ] **Step 1: Rewrite the pydantic parts of the module**

```python
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
```

- [ ] **Step 2: Smoke check the module in isolation**

Run: `.venv/bin/python -c "import pipelime.stages.base as b; s = b.StageInput(b.StageCompose([b.StageIdentity()])); print(s.model_dump(), s.dict(), b.StageInput.validate(s) is s)"`
Expected: `{'compose': {'stages': [{'identity': {}}]}} {'compose': {'stages': [{'identity': {}}]}} True` (if `import pipelime.stages.base` fails because `pipelime/stages/__init__.py` imports still-v1 modules, proceed with T2–T4 and re-run this check afterwards).

- [ ] **Step 3: Commit**

```bash
git add pipelime/stages/base.py
git commit -m "refactor(stages): SampleStage/StageInput on native pydantic v2"
```

---

### Task S2a-T2: `stages/entities.py`

**Files:**
- Modify: `pipelime/stages/entities.py`

**Interfaces:**
- Produces: `ParsedItem`, `ParsedData`, `DynamicKey`, `BaseEntity`, `ActionDef`, `BaseEntityType`, `EntityAction`, `StageEntity` (field `entity_action`, compat `__init__`, `__root__` property).

- [ ] **Step 1: Rewrite the pydantic parts** (`register_action` and the module header docstrings stay as they are)

```python
import inspect
import typing as t

import pydantic
from pydantic import ConfigDict

from pipelime.items import Item
from pipelime.stages import SampleStage
from pipelime.utils.pydantic_compat import Field, PipelimeModel, get_field
from pipelime.utils.pydantic_types import CallableDef, TypeDef

if t.TYPE_CHECKING:
    from pipelime.sequences import Sample

# ... register_action unchanged ...

ItTp = t.TypeVar("ItTp", bound=Item)
ValTp = t.TypeVar("ValTp")


class ParsedItem(
    PipelimeModel,
    t.Generic[ItTp, ValTp],
    extra="forbid",
    arbitrary_types_allowed=True,
):
    raw_item: ItTp
    parsed_value: ValTp

    def __call__(self) -> ValTp:
        return self.parsed_value

    @property
    def value(self) -> ValTp:
        return self.parsed_value

    @classmethod
    def raw_item_type(cls) -> t.Type[ItTp]:
        return cls.model_fields["raw_item"].annotation  # type: ignore[return-value]

    @classmethod
    def parsed_value_type(cls) -> t.Type[ValTp]:
        return cls.model_fields["parsed_value"].annotation  # type: ignore[return-value]

    @classmethod
    def value_to_item_data(cls, value) -> t.Any:
        if hasattr(value, "__to_item_data__"):
            return value.__to_item_data__()
        elif isinstance(value, pydantic.BaseModel):
            return value.model_dump()
        return value

    @classmethod
    def make_raw_item(cls, value) -> ItTp:
        return cls.raw_item_type().make_new(cls.value_to_item_data(value))

    @classmethod
    def make_parsed_value(cls, value) -> ValTp:
        pvtp = cls.parsed_value_type()
        if inspect.isclass(pvtp) and issubclass(pvtp, pydantic.BaseModel):
            return pydantic.TypeAdapter(pvtp).validate_python(value)  # type: ignore
        return pvtp(value)  # type: ignore

    @classmethod
    def make_new(cls, value) -> ItTp:
        return cls.make_raw_item(value)

    @classmethod
    def _coerce(cls, value) -> t.Dict[str, t.Any]:
        """Any accepted input -> the `{"raw_item", "parsed_value"}` field mapping."""
        if isinstance(value, Item):
            if isinstance(value, cls.raw_item_type()):
                return {"raw_item": value, "parsed_value": cls.make_parsed_value(value())}
        elif isinstance(value, cls.parsed_value_type()):
            return {"raw_item": cls.make_raw_item(value), "parsed_value": value}
        else:
            try:
                value = cls.make_parsed_value(value)
            except Exception:
                pass
            else:
                return {"raw_item": cls.make_raw_item(value), "parsed_value": value}
        # NB: v1 turned TypeError into a ValidationError, v2 only converts ValueError
        raise ValueError(
            f"{value} is neither `{cls.raw_item_type()}` nor `{cls.parsed_value_type()}`"
        )

    @pydantic.model_validator(mode="wrap")
    @classmethod
    def _validate_input(cls, value, handler):
        if isinstance(value, cls):
            return value
        # direct construction `cls(raw_item=..., parsed_value=...)` passes the field mapping
        if isinstance(value, t.Mapping) and set(value.keys()) == {"raw_item", "parsed_value"}:
            return handler(value)
        return handler(cls._coerce(value))

    @classmethod
    def validate(cls, value):  # v1 name kept for downstream code
        return cls.model_validate(value)


class ParsedData(ParsedItem[Item, ValTp], t.Generic[ValTp]):
    pass


class ModelDynamicKey:
    __slots__ = ("owner", "item_tp", "default", "default_factory", "extra_kwargs")

    def __init__(
        self,
        item_tp: t.Union[t.Type[Item], t.Type[ParsedItem]],
        default: t.Any = ...,
        default_factory: t.Optional[t.Callable[[], t.Any]] = None,
        extra_kwargs: t.Mapping[str, t.Any] = {},
    ):
        self.item_tp = item_tp
        self.default = default
        self.default_factory = default_factory
        self.extra_kwargs = extra_kwargs

    def validate(self, key) -> t.Union[t.Type[Item], t.Type[ParsedItem]]:
        parser_model = pydantic.create_model(
            "DynamicKeyParser",
            __config__=ConfigDict(arbitrary_types_allowed=True),
            **{
                key: (
                    self.item_tp,
                    Field(
                        self.default,
                        default_factory=self.default_factory,
                        **self.extra_kwargs,
                    ),
                )
            },
        )
        parsed_values = parser_model.model_validate(
            {key: getattr(self.owner, key)} if hasattr(self.owner, key) else {}
        )
        return getattr(parsed_values, key)


def DynamicKey(
    item_tp: t.Union[t.Type[Item], t.Type[ParsedItem]],
    default: t.Any = ...,
    *,
    default_factory: t.Optional[t.Callable[[], t.Any]] = None,
    **field_kwargs,
) -> t.Any:
    if default is not ... and default_factory is not None:
        raise ValueError("cannot specify both default and default_factory")

    return pydantic.PrivateAttr(
        ModelDynamicKey(item_tp, default, default_factory, field_kwargs)
    )


DerivedEntityTp = t.TypeVar("DerivedEntityTp", bound="BaseEntity")


class BaseEntity(PipelimeModel, extra="allow", arbitrary_types_allowed=True):
    """The base class for all input/output entity models."""

    def __init__(self, **data):
        # create an item field from raw values
        # NB: if the field is optional, it can be None
        cls = type(self)
        for k, v in data.items():
            if k in cls.model_fields and not isinstance(v, Item):
                k_field = get_field(cls, k)
                item_cls = k_field.inner_type
                if (
                    inspect.isclass(item_cls)
                    and issubclass(item_cls, Item)
                    and (k_field.required or v is not None)
                ):
                    data[k] = item_cls.make_new(v)
        super().__init__(**data)

        # assign self as the owner of dynamic key fields
        for k in self.__private_attributes__:
            v = getattr(self, k)
            if isinstance(v, ModelDynamicKey):
                v.owner = self

    @pydantic.model_serializer(mode="wrap")
    def _serialize(self, handler) -> t.Dict[str, t.Any]:
        # skip None fields and bypass ParsedItem (v1 `_iter` override)
        out = {}
        for k, v in handler(self).items():
            if v is not None:
                if isinstance(v, t.Mapping) and "raw_item" in v and "parsed_value" in v:
                    if v["raw_item"] is not None:
                        out[k] = v["raw_item"]
                else:
                    out[k] = v
        return out

    @classmethod
    def merge(
        cls: t.Type[DerivedEntityTp], __other__: "BaseEntity", /, **kwargs
    ) -> DerivedEntityTp:
        """Creates a new entity by merging `other` entity with extra `kwargs` fields."""
        other_dict = __other__.model_dump()
        for k, v in kwargs.items():
            if not isinstance(v, Item):
                # NB: None is a valid value for optional fields
                my_k_field = get_field(cls, k) if k in cls.model_fields else None
                if not my_k_field or my_k_field.required or v is not None:
                    # user has no preference on the actual Item class,
                    # so keep the original item type if it is a subclass
                    # of the declared output item type
                    if k in other_dict:
                        other_item_obj = other_dict[k]
                        if other_item_obj is not None:
                            my_item_cls = Item
                            if my_k_field:
                                my_item_cls = my_k_field.inner_type
                                if inspect.isclass(my_item_cls) and issubclass(
                                    my_item_cls, ParsedItem
                                ):
                                    my_item_cls = my_item_cls.raw_item_type()
                            if isinstance(other_item_obj, my_item_cls):
                                kwargs[k] = other_item_obj.make_new(
                                    ParsedItem.value_to_item_data(v)
                                )
        return cls(**{**other_dict, **kwargs})


class ActionDef(CallableDef):
    """Action definition, can be a registered action title, a callable
    (cfr. Choixe `$call`/`$symbol`), a `class.path.to.a.function`,
    a `path/to/a/file.py:function`, a `function:::<code>`
    or a mapping where the key is like above, while the value is
    the list of `__init__` arguments (mapping, sequence or single value).
    """

    @classmethod
    def _coerce(cls, value):
        from pipelime.cli.utils import PipelimeSymbolsHelper

        if isinstance(value, str):
            # action is function
            act = PipelimeSymbolsHelper.get_action(value)
            if act is not None:
                value = act.action
        elif isinstance(value, t.Mapping):
            # action is a callable instance
            name, args = next(iter(value.items()))
            if isinstance(name, str):
                act = PipelimeSymbolsHelper.get_actions().get(name, None)
                if act is not None:
                    value = {act.action: args}
        return super()._coerce(value)

    @classmethod
    def validate_action(cls, value) -> "ActionDef":  # v1 name kept for downstream code
        return cls.model_validate(value)


class BaseEntityType(TypeDef[BaseEntity]):
    """An entity type. It accepts both type names and string."""


class EntityAction(PipelimeModel, extra="forbid"):
    """An action and its associated input entity model."""

    action: ActionDef = Field(
        ...,
        description=(
            "The action callable to run (can be a class path). The expected annotation "
            "is `(BaseEntitySubClass) -> BaseEntityLike`. Return type annotation "
            "is not mandatory."
        ),
    )
    input_type: BaseEntityType = Field(
        BaseEntity,
        validate_default=True,
        description=(
            "The input type of the action (can be a string). If None, "
            "it is inferred from the callable's annotations or set to BaseEntity."
        ),
    )

    @pydantic.field_validator("action")
    @classmethod
    def validate_action(cls, v):
        params = v.full_signature.parameters
        if not params:
            raise ValueError(
                "The action must have at least one argument, ie, the input model entity."
            )
        first_param = next(iter(params.values()))
        if (
            first_param.kind == inspect.Parameter.KEYWORD_ONLY
            or first_param.kind == inspect.Parameter.VAR_KEYWORD
        ):
            raise ValueError(
                "The action must have at least one positional argument, "
                "ie, the input model entity."
            )
        return v

    @pydantic.field_validator("input_type")
    @classmethod
    def validate_input_type(cls, v, info: pydantic.ValidationInfo):
        if "action" in info.data:  # if not True, an error should have been raised yet
            action_t = info.data["action"].args_type[0]
            if v is None:
                v = BaseEntityType.create(action_t or BaseEntity)
            else:
                if action_t is not None and not issubclass(v.value, action_t):
                    if not issubclass(action_t, v.value):
                        raise ValueError(
                            "Action annotation is incompatible with the input type."
                        )
                    v = BaseEntityType.create(action_t)  # use the more specific type

        return v

    @pydantic.model_validator(mode="wrap")
    @classmethod
    def _validate_input(cls, value, handler):
        if isinstance(value, cls):
            return value
        if not isinstance(value, t.Mapping):
            value = {"action": value}
        if "action" not in value:
            value = {"action": value}
        return handler(value)

    @classmethod
    def validate(cls, value):  # v1 name kept for downstream code
        return cls.model_validate(value)


_MISSING = object()


class StageEntity(SampleStage, title="entity"):
    """Runs an entity action, ie, `(BaseEntitySubClass) -> BaseEntityLike`,
    on each sample."""

    entity_action: EntityAction = Field(..., description="The entity action to run.")

    @staticmethod
    def _normalize(value: t.Any) -> t.Any:
        """Any pipelime 2.x input shape -> the EntityAction spec.

        Shapes: a bare spec (callable / str / `{action: ..., input_type: ...}`),
        the `{"__root__": spec}` envelope written by 2.x `to_pipe()`, and the
        `{"entity_action": spec}` field mapping.
        """
        if isinstance(value, t.Mapping):
            keys = set(value.keys())
            if keys == {"__root__"}:
                return value["__root__"]
            if keys == {"entity_action"}:
                return value["entity_action"]
        return value

    def __init__(self, __root__: t.Any = _MISSING, /, **data):
        # positional spec, `__root__=` keyword, the 2.x envelope, or the bare
        # `{action: ..., input_type: ...}` kwargs form
        if __root__ is not _MISSING:
            if data:
                raise TypeError(
                    "StageEntity takes a single entity action, "
                    f"got extra arguments {sorted(data)}"
                )
            spec = __root__
        else:
            spec = self._normalize(data)
        super().__init__(entity_action=spec)  # type: ignore

    @pydantic.model_validator(mode="wrap")
    @classmethod
    def _validate_input(cls, value, handler):
        # `model_validate(spec)` / `TypeAdapter(StageEntity)` / nested validation
        # receive the raw spec (v1 accepted it through `_enforce_dict_if_root`)
        if isinstance(value, cls):
            return value
        return handler({"entity_action": cls._normalize(value)})

    @pydantic.model_serializer(mode="wrap")
    def _serialize(self, handler) -> t.Dict[str, t.Any]:
        return handler(self)["entity_action"]

    def __call__(self, x: "Sample") -> "Sample":
        from pipelime.sequences import Sample

        ea = self.entity_action
        return Sample(ea.action(ea.input_type.value(**x)).model_dump())


# v1 exposed the action as `__root__`; pydantic rejects the name inside a class body
StageEntity.__root__ = property(lambda self: self.entity_action)  # type: ignore[attr-defined]
```

- [ ] **Step 2: Smoke check** (may need T3/T4 first for `import pipelime.stages` to succeed)

Run:
```bash
.venv/bin/python - <<'EOF'
import numpy as np, pipelime.items as pli
from pipelime.stages.entities import BaseEntity, EntityAction, ParsedData, StageEntity
from pipelime.sequences import Sample
import pydantic
class Meta(pydantic.BaseModel):
    name: str
class In(BaseEntity):
    image: pli.ImageItem
    meta: ParsedData[Meta]
def act(x: In):
    return In.merge(x, meta=Meta(name=x.meta().name + "!"))
x = Sample({"image": pli.PngImageItem(np.zeros((2, 2, 3), np.uint8)), "meta": pli.JsonMetadataItem({"name": "n"})})
for st in (StageEntity(EntityAction(action=act)), StageEntity(__root__={"action": act}), StageEntity(action=act), StageEntity({"__root__": {"action": act}}), pydantic.TypeAdapter(StageEntity).validate_python(act), StageEntity.model_validate({"action": act})):
    print(st(x)["meta"](), st.model_dump()["action"], st.__root__.input_type.value is In)
EOF
```
Expected: six lines `{'name': 'n!'} <module>.act True`.

- [ ] **Step 3: Commit**

```bash
git add pipelime/stages/entities.py
git commit -m "refactor(stages): entities on native pydantic v2 (ParsedItem, BaseEntity, StageEntity)"
```

---

### Task S2a-T3: `stages/augmentations.py`, `item_replacement.py`, `key_transformations.py`, `item_info.py`, `item_sources.py`

**Files:**
- Modify: the five modules

- [ ] **Step 1: `augmentations.py`** — imports and `Transformation`:

```python
import typing as t
from pathlib import Path

import albumentations as A
import numpy as np
import pydantic as pyd
from pydantic_extra_types.color import Color

from pipelime.stages import SampleStage
from pipelime.utils.pydantic_compat import Field, PipelimeRootModel

if t.TYPE_CHECKING:
    from pipelime.sequences import Sample


class Transformation(PipelimeRootModel[t.Dict[str, t.Any]]):
    """The albumentations transformation defined as python object,
    serialized dict or yaml/json file.
    """

    _value: t.Union[A.BaseCompose, A.BasicTransform] = pyd.PrivateAttr(None)

    def model_post_init(self, _context: t.Any) -> None:
        self._value = A.from_dict(self.root)  # type: ignore

    @property
    def value(self):  # NB: overrides PipelimeRootModel.value on purpose (v1 API)
        return self._value

    def __str__(self) -> str:
        return str(self.root)

    def __repr__(self) -> str:
        return repr(self.root)

    @classmethod
    def _coerce(cls, value):
        if isinstance(value, (A.BaseCompose, A.BasicTransform)):
            return A.to_dict(value)
        if isinstance(value, (str, Path)):
            import yaml

            with open(str(value)) as f:
                value = yaml.safe_load(f)
        if isinstance(value, t.Mapping):
            return value
        raise ValueError(f"{value} is not a valid transformation")
```

Then in the rest of the file: `@pyd.validator("output_key_format")` → `@pyd.field_validator("output_key_format")` + `@classmethod` (both occurrences); every `pyd.Field(` → `Field(`; class kwargs `copy_on_model_validation="none"` removed; `Color` usages unchanged (`Color("black")`, `.as_rgb_tuple(...)`).

- [ ] **Step 2: `item_replacement.py`**

`import pydantic.v1 as pyd` → `import pydantic as pyd`; `@pyd.validator("algorithm")` → `@pyd.field_validator("algorithm")` + `@classmethod`; the root validator becomes:

```python
    # We need to check that the keys are not present in both lists.
    @pyd.model_validator(mode="after")
    def _validate_keys(self) -> "StageShareItems":
        if set(self.share) & set(self.unshare):
            raise ValueError(
                "The keys in the `share` and `unshare` lists must be disjoint."
            )
        return self
```
(use the actual class name in the return annotation).

- [ ] **Step 3: `key_transformations.py`, `item_info.py`, `item_sources.py`**

- `key_transformations.py`: import swap; `@pyd.validator("key_format")` → `@pyd.field_validator("key_format")` + `@classmethod`.
- `item_info.py`: import swap; `class ItemInfo(pyd.BaseModel)` → `from pipelime.utils.pydantic_compat import PipelimeModel` and `class ItemInfo(PipelimeModel)`.
- `item_sources.py`: import swap only.

- [ ] **Step 4: Grep the package for leftovers**

Run: `grep -n "pydantic.v1\|copy_on_model_validation\|underscore_attrs_are_private\|allow_population_by_field_name\|__root__\|__config__\|__fields__\|\.dict(\|\.json(\|pyd.validator\|root_validator" pipelime/stages/*.py`
Expected: only the `StageEntity.__root__ = property(...)` line and the `StageEntity.__init__` handling of `"__root__"` in `entities.py`.

- [ ] **Step 5: Commit**

```bash
git add pipelime/stages
git commit -m "refactor(stages): augmentations, item replacement/info/sources on pydantic v2"
```

---

### Task S2a-T4: `sequences/samples_sequence.py`

**Files:**
- Modify: `pipelime/sequences/samples_sequence.py`

**Interfaces:**
- Produces: `SamplesSequence(SamplesSequenceBase, PipelimeModel, extra="forbid")`, `name()`, `to_pipe()` (str-safe), `piped_sequence()` reading `pipe_source` via `iter_fields`.

- [ ] **Step 1: Imports and class header**

```python
import pydantic as pyd
from loguru import logger

from pipelime.sequences.sample import Sample
from pipelime.utils.pydantic_compat import PipelimeModel, field_extra, iter_fields, model_title
```

```python
class SamplesSequence(SamplesSequenceBase, PipelimeModel, extra="forbid"):
```

```python
    @classmethod
    def name(cls) -> str:
        return model_title(cls)
```

- [ ] **Step 2: Rewrite `to_pipe()`** (bug fix: `str`/`bytes` are not recursed into)

```python
        def _maybe_go_deeper(field_value):
            if isinstance(field_value, SamplesSequence):
                if recursive:
                    field_value = field_value.to_pipe(
                        recursive=recursive, objs_to_str=objs_to_str
                    )
            elif isinstance(field_value, pyd.BaseModel):
                if recursive:
                    # NB: do not unfold sub-pydantic models, since it may not be
                    # straightforward to de-serialize them when subclasses are used
                    field_value = field_value.model_dump()
            elif isinstance(field_value, (str, bytes)):
                pass  # NB: strings are sequences of strings: never recurse into them
            elif isinstance(field_value, t.Sequence):
                field_value = [_maybe_go_deeper(x) for x in field_value]
            elif isinstance(field_value, t.Mapping):
                field_value = {k: _maybe_go_deeper(v) for k, v in field_value.items()}

            if (
                not objs_to_str
                or isinstance(
                    field_value,
                    (str, bytes, int, float, bool, t.Mapping, t.Sequence),
                )
                or field_value is None
            ):
                return field_value
            return str(field_value)

        source_list = []
        arg_dict = {}
        for fview in iter_fields(type(self)):
            field_value = getattr(self, fview.name)
            field_alias = fview.effective_alias
            if fview.extra.get("pipe_source", False):
                if not isinstance(field_value, SamplesSequence):
                    raise ValueError(
                        f"{field_alias} is tagged as `pipe_source`, "
                        "but it is not a SamplesSequence instance."
                    )
                source_list = field_value.to_pipe(
                    recursive=recursive, objs_to_str=objs_to_str
                )
            else:
                arg_dict[field_alias] = _maybe_go_deeper(field_value)

        return source_list + [{self._operator_path: arg_dict}]
```

- [ ] **Step 3: `piped_sequence()`**

```python
    prms_source_name = None
    for fview in iter_fields(cls):
        if fview.extra.get("pipe_source", False):
            if prms_source_name is not None:
                raise ValueError(
                    f"More than one field has `pipe_source=True` in {cls.__name__}."
                )
            prms_source_name = fview.effective_alias
```

- [ ] **Step 4: Commit** (after T5 if the package does not import yet)

```bash
git add pipelime/sequences/samples_sequence.py
git commit -m "refactor(sequences): SamplesSequence on pydantic v2; fix to_pipe str recursion"
```

---

### Task S2a-T5: `sequences/pipes/*`, `sources/*`, `utils.py`, `grabber.py`

**Files:**
- Modify: `pipelime/sequences/pipes/base.py`, `mapping.py`, `operations.py`, `validation.py`, `writers.py`; `pipelime/sequences/sources/from_callable.py`, `raw.py`, `readers.py`, `toy_dataset.py`; `pipelime/sequences/utils.py`; `pipelime/sequences/grabber.py`

- [ ] **Step 1: `pipes/base.py`**

```python
from pipelime.sequences.samples_sequence import SamplesSequence
from pipelime.utils.pydantic_compat import Field


class PipedSequenceBase(SamplesSequence):
    """Base class for all piped sequences."""

    source: SamplesSequence = Field(
        ..., description="The source sample sequence.", exclude=True, pipe_source=True
    )
```
(keep the rest of the file: `size`/`get_sample` defaults if present.)

- [ ] **Step 2: `pipes/validation.py`** — `_check_sample`:

```python
    def _check_sample(self, sample: pls.Sample):
        from pydantic import ValidationError

        try:
            _ = self.sample_schema.schema_model(**sample)
        except ValidationError as e:
            raise ValueError(
                f"Sample schema validation failed for:\n{str(sample)}\n\n"
                f"Errors:\n{e}"
            ) from e
```
plus `import pydantic.v1 as pyd` → `import pydantic as pyd`; drop `arbitrary_types_allowed=True` only if the class has no arbitrary-typed field (it has `SampleValidationInterface`, a model → keep the kwarg, it is harmless).

- [ ] **Step 3: `pipes/operations.py`, `pipes/mapping.py`, `pipes/writers.py`, `sources/*.py`, `grabber.py`** — mechanical rules only:

- import swaps (`readers.py`: `from pydantic import Field, PrivateAttr, field_validator`).
- `@pyd.validator("key_format")` → `@pyd.field_validator("key_format")` + `@classmethod` (`operations.py::ZippedSequences`, `sources/toy_dataset.py`).
- `@pyd.validator("exists_ok", always=True)` in `writers.py` → `@pyd.field_validator("exists_ok")` + `@classmethod`, signature `(cls, v: bool, info: pyd.ValidationInfo)`, `values` → `info.data`, and `exists_ok: bool = pyd.Field(False, validate_default=True, description=...)`.
- `@validator("must_exist", always=True)` in `readers.py` (three classes) → `@field_validator("must_exist")` + `@classmethod`, `(cls, v, info: ValidationInfo)`, `values["folder"]`/`values["video"]` → `info.data["folder"]`/`info.data["video"]`, and `must_exist: bool = Field(True, validate_default=True, ...)` in each.
- `writers.py` class kwarg `underscore_attrs_are_private=True` removed.
- `grabber.py`: `class Grabber(PipelimeModel, extra="forbid")` with `from pipelime.utils.pydantic_compat import PipelimeModel`.
- `sources/raw.py`, `sources/from_callable.py`, `pipes/mapping.py`: import swap only (`MappingConditionProbability`/`MappingConditionIndexRange` become `PipelimeModel` subclasses).

- [ ] **Step 4: `utils.py`** — `DataStream`:

```python
from pydantic import Field, PrivateAttr

from pipelime.utils.pydantic_compat import PipelimeModel
...
class DataStream(t.Sequence[Sample], PipelimeModel, extra="forbid"):
```
(everything else unchanged; the `_output_sequence: SamplesSequence` annotation-only private attribute keeps working in v2.)

- [ ] **Step 5: Import smoke and direct-construction tests**

Run:
```bash
.venv/bin/python -c "import pipelime.stages, pipelime.sequences; print('ok')"
.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/sequences/test_sample.py tests/pipelime/sequences/test_samples_sequences.py tests/pipelime/stages/test_entities.py tests/pipelime/stages/test_base_stages.py 2>&1 | tail -15
```
Expected: `ok`; test failures are acceptable **only** if their traceback ends in an `ImportError`/`ModuleNotFoundError` raised while importing `pipelime.commands`/`pipelime.piper`/`pipelime.cli` (registry), or in `pydantic.v1` code of those packages. Record the list of such tests in the ledger; fix anything else now.

- [ ] **Step 6: Leftover grep and commit**

Run: `grep -rn "pydantic.v1\|copy_on_model_validation\|underscore_attrs_are_private\|allow_population_by_field_name\|__config__\|__fields__\|field_info\|\.dict(\|\.json(\|pyd.validator\|@validator" pipelime/sequences`
Expected: no matches.

```bash
git add pipelime/sequences
git commit -m "refactor(sequences): pipes, sources, DataStream, Grabber on pydantic v2"
```

Update the ledger (S2a done; note the expected registry-related failures). Then open `2026-09-13-pydantic-v2-s2b-piper-and-commands.md`.
