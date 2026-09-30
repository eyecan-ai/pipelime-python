# S2b — Piper, commands, and the minimal CLI/choixe compat (S2 gate)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the command framework (`pipelime/piper/*`), the interfaces and commands (`pipelime/commands/*`), plus the few `cli/utils.py` and `choixe` lines the registry/DAG code needs, and reach the **S2 gate**: `tests/pipelime/{stages,sequences,piper,commands}` and the non-CLI contract tests green.

**Architecture:** `PipelimeCommand`, `PiperDAG`, `LazyCommand`, `PiperInfo`, `DAGModel` become `PipelimeModel`s; `NodesDefinition` a `PipelimeRootModel`; every compact-form interface derives from a new `CompactFormModel(PipelimeModel)` whose `wrap` validator calls the class's `_compact_to_data()` hook; `@command` synthesizes annotations and fixes `**kwargs`; `cli/utils.py` gets `resolve_pipelime_command()` and `format_validation_error()`; `choixe` recognises v2 models in `$model`/decode (dataclasses are left for S4).

**Tech Stack:** pydantic 2.x.

**Spec:** `docs/superpowers/specs/2026-09-12-pydantic-v2-migration-design.md` §4.2, §4.5, §4.6 (error formatting), §4.7 (decoder/`$model` lines only).

## Global Constraints

- Same mechanical rules as S2a (import swaps, `field_validator` + `@classmethod`, `validate_default=True` for `always=True`, `info.data` for `values`, `.dict(`→`.model_dump(`, `.json(`→`.model_dump_json(`, `parse_obj`→`model_validate`, `parse_raw`→`model_validate_json`, `.copy(update=`→`.model_copy(update=`, `X.__config__.title`→`model_title(X)`, `__fields__`→`iter_fields`/`model_fields`), and: raise `ValueError` (never `TypeError`) inside validators/hooks.
- Every `Field(...)` that carries a pipelime flag (`piper_port`, `pipe_source`, `is_required`, `expand_help`) uses `pipelime.utils.pydantic_compat.Field` (re-exported as `pipelime.piper.Field`); pipelime source must not trigger pydantic's "extra keyword arguments on `Field`" warning.
- `pipelime/cli/pretty_print.py`, `cli/tui/*`, `cli/main.py` are **not** touched here (S3); `choixe/ast/nodes.py` is **not** touched here (S4).

---

### Task S2b-T1: Progress models and the ZMQ wire

**Files:**
- Modify: `pipelime/piper/progress/model.py`, `pipelime/piper/progress/tracker/zmq.py:77`, `pipelime/piper/progress/listener/receiver/zmq.py:44`

- [ ] **Step 1: Edit**

`model.py`:
```python
from pipelime.utils.pydantic_compat import PipelimeModel


class OperationInfo(PipelimeModel, frozen=True):
    ...  # fields unchanged


class ProgressUpdate(PipelimeModel):
    ...  # fields unchanged
```
`tracker/zmq.py`: `prog.json().encode()` → `prog.model_dump_json().encode()`.
`listener/receiver/zmq.py`: `ProgressUpdate.parse_raw(messagedata.decode())` → `ProgressUpdate.model_validate_json(messagedata.decode())`.

- [ ] **Step 2: Test and commit**

Run: `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/piper/progress`
Expected: passed (the ZMQ tests take ~11 s).

```bash
git add pipelime/piper/progress
git commit -m "refactor(piper): progress models on pydantic v2"
```

---

### Task S2b-T2: `piper/checkpoint.py`

**Files:**
- Modify: `pipelime/piper/checkpoint.py`

- [ ] **Step 1: Edit**

```python
import pydantic as pyd
from pipelime.utils.pydantic_compat import PipelimeModel
from pipelime.utils.pydantic_types import NewPath
...
class LocalCheckpoint(Checkpoint, PipelimeModel):
    folder: t.Union[pyd.DirectoryPath, NewPath] = pyd.Field(
        ..., description="The folder where checkpoints are read/written"
    )
    try_link: bool = pyd.Field(True, description="Try to link assets instead of copying")

    _temp_folder: Path = pyd.PrivateAttr()

    _assets_dir: t.ClassVar[str] = "__assets"
    _data_dir: t.ClassVar[str] = "__data"

    @pyd.field_validator("folder")
    @classmethod
    def _validate_folder(cls, v):
        return v.resolve().absolute()
```
(rest unchanged.)

- [ ] **Step 2: Commit**

```bash
git add pipelime/piper/checkpoint.py
git commit -m "refactor(piper): LocalCheckpoint on pydantic v2"
```

---

### Task S2b-T3: `piper/model.py` and `pipelime.piper.Field`

**Files:**
- Modify: `pipelime/piper/model.py`
- Modify: `pipelime/piper/__init__.py`

**Interfaces:**
- Produces: `pipelime.piper.Field`; `PipelimeCommand(PipelimeModel, ABC, populate_by_name=True, extra="forbid")`; `command()` with annotation synthesis and the `**kwargs` fix; `LazyCommand(PipelimeModel, Generic[CmdTp])`; `NodesDefinition(PipelimeRootModel[Mapping[str, PipelimeCommand]])` with `_build_nodes()`; `DAGModel`.
- Consumes: `pipelime.cli.utils.resolve_pipelime_command`, `format_validation_error` (T7 — write T7 before running anything that builds DAG nodes).

- [ ] **Step 1: `pipelime/piper/__init__.py`** — add the re-export next to the existing ones:

```python
from pipelime.utils.pydantic_compat import Field  # noqa: F401  (pydantic.Field + piper flags)
```

- [ ] **Step 2: Rewrite the pydantic parts of `piper/model.py`**

Imports:
```python
import typing as t
from abc import ABC, abstractmethod
from enum import Enum

import typing_extensions as te
from loguru import logger
from pydantic import PrivateAttr, model_serializer

from pipelime.piper.checkpoint import CheckpointNamespace
from pipelime.piper.progress.tracker.base import TrackCallback, TrackedTask, Tracker
from pipelime.utils.pydantic_compat import (
    Field,
    PipelimeModel,
    PipelimeRootModel,
    field_extra,
    iter_fields,
    model_title,
)
```

Inside `command()`'s `_make_cmd`:
```python
    def _make_cmd(func):
        import inspect

        from pydantic.fields import FieldInfo
        from pydantic_core import PydanticUndefined

        def _make_field(p: inspect.Parameter):
            """Returns a tuple of (annotation, default) for a given parameter.
            NB: *args translates to a tuple and **kwargs translates to a dict.
            """
            value = Field(...) if p.default is inspect.Parameter.empty else p.default
            no_ann = p.annotation is inspect.Signature.empty

            if p.kind is p.VAR_POSITIONAL:
                ann = tuple if no_ann else tuple[p.annotation, ...]
            elif p.kind is p.VAR_KEYWORD:
                ann = dict if no_ann else dict[str, p.annotation]
            elif no_ann:
                # pydantic v2 needs an annotation: infer it from the default as v1 did
                raw = value
                if isinstance(value, FieldInfo):
                    raw = (
                        value.default_factory()
                        if value.default_factory is not None
                        else value.default
                    )
                ann = (
                    t.Any
                    if raw is None or raw is PydanticUndefined or raw is Ellipsis
                    else type(raw)
                )
            else:
                ann = p.annotation

            return (ann, value)
```
The `fields`/`posonly_names`/... gathering is unchanged. `_FnModel.__init__` unchanged. `_FnModel.run` becomes:
```python
            def run(self):
                # get all arguments in the right order
                # NB: do not use self.model_dump(), it returns sub-models as dict!
                self_arg = (self,) if is_bound else tuple()
                pos_args = [getattr(self, n) for n in posonly_names + poskw_names]
                var_args = getattr(self, varpos_name) if varpos_name else tuple()
                kw_args = {n: getattr(self, n) for n in kwonly_names if hasattr(self, n)}
                if varkw_name and hasattr(self, varkw_name):
                    kw_args.update(getattr(self, varkw_name))  # expand **kwargs (bug fix)
                func(*self_arg, *pos_args, *var_args, **kw_args)
```
`fmodel = type(...)` unchanged (the `__annotations__` dict now always has an entry for every field). `_unwrap_default`:
```python
        def _unwrap_default(value):
            if isinstance(value, FieldInfo):
                value = (
                    value.default
                    if value.default_factory is None
                    else value.default_factory()
                )
                if value is Ellipsis or value is PydanticUndefined:
                    return inspect.Parameter.empty
            return value
```

`PiperInfo`:
```python
class PiperInfo(PipelimeModel, extra="forbid"):
    token: str = Field("", description="The piper execution token.")
    node: str = Field("", description="The piper dag's node name.")
    ...
```

`PipelimeCommand`:
```python
class PipelimeCommand(PipelimeModel, ABC, populate_by_name=True, extra="forbid"):
    """(docstring unchanged)"""

    _force_gc: t.ClassVar[bool] = False
    _no_default_checkpoint: t.ClassVar[bool] = False
    _track_callback: t.ClassVar[t.Optional[TrackCallback]] = None

    _piper: PiperInfo = PrivateAttr(default_factory=PiperInfo)  # type: ignore
    _tracker: t.Optional["Tracker"] = PrivateAttr(None)
    _checkpoint: CheckpointNamespace = PrivateAttr(default_factory=CheckpointNamespace)

    # lazy(), __init_subclass__, save_to_default_checkpoint, init_from_checkpoint,
    # command_checkpoint, run, _get_fields_by_flag, get_inputs, get_outputs,
    # _get_piper_tracker, command_name, set_piper_info, track, create_task, __call__:
    # unchanged

    @classmethod
    def _filter_fields_by_flag(cls, flag: str, value: t.Any) -> t.Iterable[str]:
        for fview in iter_fields(cls):
            if fview.extra.get(flag, object()) == value:
                yield fview.name

    @classmethod
    def command_title(cls) -> str:
        return model_title(cls)
```

`LazyCommand`:
```python
class LazyCommand(PipelimeModel, t.Generic[CmdTp], extra="forbid"):
    command_class: t.Type[CmdTp]
    data: t.Dict[str, t.Any] = Field(default_factory=dict)

    def __call__(self) -> CmdTp:
        return self.command_class(**self.data)

    def init_from_checkpoint(self, checkpoint: CheckpointNamespace):
        return self.command_class.init_from_checkpoint(checkpoint, **self.data)

    def __setattr__(self, name, value):
        self.data[name] = value

    def __getattr__(self, name):
        if name.startswith("__") or name == "data":  # never recurse into ourselves
            raise AttributeError(name)
        if name in self.data:
            return self.data[name]
        if name in self.command_class.model_fields:
            return self.command_class.model_fields[name].get_default(
                call_default_factory=True
            )
        if hasattr(self.command_class, name):
            return getattr(self.command_class, name)
        raise AttributeError(f"{self.command_class.__name__} has no attribute '{name}'")
```

`NodesDefinition` and `DAGModel`:
```python
class NodesDefinition(PipelimeRootModel[t.Mapping[str, PipelimeCommand]]):
    """A simple interface to parse a DAG node configuration."""

    @classmethod
    def _build_nodes(
        cls,
        value: T_NODES,
        *,
        checkpoint: t.Optional[CheckpointNamespace] = None,
        skip_on_error: bool = False,
    ) -> t.Dict[str, PipelimeCommand]:
        from pydantic import ValidationError

        from pipelime.cli.utils import format_validation_error, resolve_pipelime_command

        plnodes = {}
        for name, cmd in value.items():
            if checkpoint:
                ckpt = checkpoint.get_namespace(
                    name.replace(".", "_").replace("[", "_").replace("]", "_")
                )
            else:
                ckpt = None

            cmd_cls, build = resolve_pipelime_command(cmd, ckpt)
            try:
                plcmd = build()
            except ValidationError as e:
                if skip_on_error:
                    logger.warning(f"Skipping node `{name}` due to validation error.")
                else:
                    raise ValueError(
                        f"Invalid node definition `{name}`:\n"
                        f"{format_validation_error(e, cmd_cls)}"
                    ) from e
            else:
                plnodes[name] = plcmd
        return plnodes

    @classmethod
    def create(
        cls,
        value: t.Union["NodesDefinition", T_NODES],
        *,
        checkpoint: t.Optional[CheckpointNamespace] = None,
        skip_on_error: bool = False,
    ):
        if isinstance(value, NodesDefinition):
            return value
        return cls(cls._build_nodes(value, checkpoint=checkpoint, skip_on_error=skip_on_error))

    @classmethod
    def _coerce(cls, value):
        if isinstance(value, t.Mapping) and all(
            isinstance(v, PipelimeCommand) for v in value.values()
        ):
            return value
        return cls._build_nodes(value)

    @model_serializer(mode="wrap")
    def _serialize(self, handler) -> t.Dict[str, t.Any]:
        # NB: `handler` gives `{node_name: <command args>}` with the caller's flags applied
        return {
            node_name: {self.root[node_name].command_title(): cmd_args}
            for node_name, cmd_args in handler(self).items()
        }


T_DAG_NODE = t.Union[
    PipelimeCommand, LazyCommand, t.Mapping[str, t.Optional[t.Mapping[str, t.Any]]]
]
T_NODES = t.Mapping[str, T_DAG_NODE]


class DAGModel(PipelimeModel, extra="forbid"):
    """A Piper DAG as a `<node>: <command>` mapping."""

    nodes: NodesDefinition

    def purged_dict(self):
        from pipelime.choixe import XConfig

        return XConfig(data={"nodes": self.nodes.value}).decode()
```
`T_DAG_NODE`/`T_NODES` must stay defined before `NodesDefinition` (they are used in its annotations) — keep their current position in the file.

- [ ] **Step 3: Commit** (tests after T7)

```bash
git add pipelime/piper/model.py pipelime/piper/__init__.py
git commit -m "refactor(piper): command framework on pydantic v2; fix @command **kwargs"
```

---

### Task S2b-T4: `commands/interfaces.py`

**Files:**
- Modify: `pipelime/commands/interfaces.py`

**Interfaces:**
- Produces: `CompactFormModel(PipelimeModel)` with hook `_compact_to_data(cls, value) -> Mapping` and public `validate(cls, value)`; all interface classes converted.

- [ ] **Step 1: Imports and the new base**

```python
from __future__ import annotations

import json
import typing as t
import uuid
from pathlib import Path

import pydantic as pyd

from pipelime.piper import PiperPortType
from pipelime.utils.pydantic_compat import Field, PipelimeModel
from pipelime.utils.pydantic_types import ItemType, SampleValidationInterface, YamlInput


class CompactFormModel(PipelimeModel):
    """A model that also accepts a *compact form* (a string, a number, a list…)
    when used as a field type or validated with `model_validate`.
    Subclasses implement `_compact_to_data`, returning the field mapping."""

    @classmethod
    def _compact_to_data(cls, value: t.Any) -> t.Mapping[str, t.Any]:
        if isinstance(value, t.Mapping):
            return value
        raise ValueError(f"Invalid {cls.__name__} definition: {value!r}")

    @pyd.model_validator(mode="wrap")
    @classmethod
    def _validate_compact(cls, value, handler):
        if isinstance(value, cls):
            return value
        return handler(cls._compact_to_data(value))

    @classmethod
    def validate(cls, value):  # v1 name kept for downstream code
        return cls.model_validate(value)
```

`PydanticFieldMixinBase` unchanged. `pyd_field()` in both mixins now uses the pipelime `Field` (import above) with the same arguments (`default_factory=cls`, `piper_port=...`, `**kwargs`).

- [ ] **Step 2: Convert each interface** (remove `copy_on_model_validation`, `underscore_attrs_are_private`; `allow_population_by_field_name=True` → `populate_by_name=True`)

`GrabberInterface(PydanticFieldWithDefaultMixin, CompactFormModel, extra="forbid")`:
```python
    @classmethod
    def _compact_to_data(cls, value):
        if isinstance(value, (str, bytes, int)):
            data = {}
            if isinstance(value, int):
                data["num_workers"] = value
            else:
                raw_data = str(value).split(",")
                try:
                    if raw_data[0]:
                        data["num_workers"] = int(raw_data[0])
                    if len(raw_data) > 1 and raw_data[1]:
                        data["prefetch"] = int(raw_data[1])
                    if len(raw_data) > 2 and raw_data[2]:
                        data["allow_nested_mp"] = raw_data[2].lower() == "true"
                except ValueError:
                    raise ValueError("Invalid grabber definition.")
            value = data
        if isinstance(value, t.Mapping):
            return value
        raise ValueError("Invalid grabber definition.")
```
(`__get_validators__`/`validate` removed; `grab_all`/`grab_all_wrk_init` unchanged.)

`InputDatasetInterface(PydanticFieldNoDefaultMixin, CompactFormModel, extra="forbid", populate_by_name=True)`:
- `pipe: t.Optional[YamlInput] = Field(None, validate_default=True, description=...)`
- `@pyd.field_validator("folder") @classmethod def resolve_folder(cls, v)` (same body)
- `@pyd.field_validator("pipe") @classmethod def check_pipe_and_folder(cls, v, info: pyd.ValidationInfo)` with `values.get("folder", None)` → `info.data.get("folder", None)`
- `_compact_to_data`: the body of the old `validate` from `if isinstance(value, (str, bytes, Path))` to the end, returning `value` when it is a Mapping and raising `ValueError("Invalid input dataset definition.")` otherwise.

`SerializationModeInterface(PipelimeModel, extra="forbid")` — `__init__` and everything else unchanged.

`OutputDatasetInterface(PydanticFieldNoDefaultMixin, CompactFormModel, extra="forbid", populate_by_name=True)`:
- `exists_ok: bool = Field(False, validate_default=True, description=...)`, `pipe: ... = Field(None, validate_default=True, ...)`
- `resolve_folder`, `_check_folder_exists(cls, v, info)`, `check_pipe_and_folder(cls, v, info)` as field validators reading `info.data`
- `_compact_to_data` from the old `validate` body (raise `ValueError("Invalid output dataset definition.")`).

`ToyDatasetInterface(PipelimeModel, extra="forbid")`: `@pyd.field_validator("key_format") @classmethod`.

`Interval(PydanticFieldWithDefaultMixin, CompactFormModel, extra="forbid")` and `ExtendedInterval(Interval)`: `_compact_to_data` = the old `validate` body returning `data` (a mapping) instead of `cls(**data)`; the `isinstance(value, cls)` check is dropped (handled by the base).

`OutputValueInterface(PydanticFieldNoDefaultMixin, CompactFormModel, t.Generic[ValueType], extra="forbid")`:
- `_data: ValueType` stays an annotation-only private attribute
- `exists_ok: bool = Field(False, validate_default=True, ...)`
- `@pyd.field_validator("file") @classmethod def resolve_file`, `@pyd.field_validator("exists_ok") @classmethod def _check_file_exists(cls, v, info)` reading `info.data`
- `_compact_to_data` from the old `validate` body (raise `ValueError("Invalid OutputValueInterface definition.")`).

- [ ] **Step 3: Grep for leftovers**

Run: `grep -n "__get_validators__\|pydantic.v1\|GenericModel\|copy_on_model\|underscore_attrs\|allow_population\|always=True\|values\[" pipelime/commands/interfaces.py`
Expected: no matches.

- [ ] **Step 4: Commit**

```bash
git add pipelime/commands/interfaces.py
git commit -m "refactor(commands): interfaces on pydantic v2 with CompactFormModel"
```

---

### Task S2b-T5: `commands/split_ops.py`

**Files:**
- Modify: `pipelime/commands/split_ops.py`

- [ ] **Step 1: Edit**

```python
import typing as t

import pydantic as pyd

import pipelime.commands.interfaces as pl_interfaces
from pipelime.piper import Field, PipelimeCommand, PiperPortType


class SplitBase(
    pl_interfaces.PydanticFieldNoDefaultMixin,
    pl_interfaces.CompactFormModel,
    populate_by_name=True,
    extra="forbid",
):
    ...  # `output` field and __repr__/__piper_repr__ unchanged
```
`PercSplit._compact_to_data(cls, value)`: old `validate` body with `return PercSplit(**value)` → `return value` (when Mapping) and the `isinstance(value, PercSplit)` line removed; same for `AbsoluteSplit`. `self.output.copy(update={...})` → `self.output.model_copy(update={...})`. Every `pyd.Field(..., piper_port=...)` → `Field(...)`. `Splits` and the commands otherwise unchanged.

- [ ] **Step 2: Commit**

```bash
git add pipelime/commands/split_ops.py
git commit -m "refactor(commands): split ops on pydantic v2"
```

---

### Task S2b-T6: `commands/general.py`, `piper.py`, `resume.py`, `shell.py`, `tempman.py`, `toy_dataset.py`

**Files:**
- Modify: the six modules

- [ ] **Step 1: `general.py`**

- `import pydantic.v1 as pyd` → `import pydantic as pyd`; add `from pipelime.piper import Field` and `from pipelime.utils.pydantic_compat import PipelimeModel, model_title`.
- `class OutputTime(pyd.BaseModel)`, `OutputStageTime`, the nested `OutputSchemaDefinition`, `OutputCmdLineSchema` → `(PipelimeModel)`.
- every `pyd.Field(` → `Field(` (the file has many `piper_port=` flags).
- `StageTimingCommand.run`: `stage_cls = st.__root__.__class__` → `stage_cls = type(st.root)`; `stage_name = stage_cls.__config__.title if ... else stage_cls.__name__` → `stage_name = model_title(stage_cls)`.
- `@pyd.validator("operations")` → `@pyd.field_validator("operations")` + `@classmethod`.
- `FilterCommand`: `filter_fn: ... = Field(None, validate_default=True, ...)` (find the field declaration above line 805) and `@pyd.validator("filter_fn", always=True)` → `@pyd.field_validator("filter_fn")` + `@classmethod`, `(cls, v, info: pyd.ValidationInfo)`, `values.get("filter_query", None)` → `info.data.get("filter_query", None)`.
- `ValidateCommand.run`: `.dict(by_alias=True)` → `.model_dump(by_alias=True)`.

- [ ] **Step 2: `piper.py`**

- `from pydantic.v1 import (...)` → `from pydantic import PrivateAttr, create_model, field_validator` (+ whatever else the block imported except `Field`/`validator`), and `from pipelime.piper import Field`; `from pipelime.utils.pydantic_compat import PipelimeModel, model_title`.
- `@validator("folder_debug", always=True)` → `@field_validator("folder_debug")` + `@classmethod`; the `folder_debug` field gets `validate_default=True`.
- `PiperDAG(PipelimeModel, ABC, populate_by_name=True, extra="forbid")`.
- `__cls_kwargs__={"title": cls.schema()["title"]}` → `__cls_kwargs__={"title": model_title(cls)}`.
- every `Field(..., piper_port=...)` uses the pipelime `Field`.

- [ ] **Step 3: `resume.py`**

```python
from pydantic import DirectoryPath, ValidationError, conint, field_validator
from pipelime.piper import Field, PipelimeCommand
...
    ckpt: t.Optional[
        t.Union[DirectoryPath, conint(ge=1, le=PipelimeUserAppDir.MAX_CHECKPOINTS)]  # type: ignore
    ] = Field(None, alias="c", validate_default=True, description=(...unchanged...))

    @field_validator("ckpt")
    @classmethod
    def _validate_ckpt(cls, v):
        ...  # unchanged body
```
`cli.PlCliOptions.parse_obj(...)` → `cli.PlCliOptions.model_validate(...)`; `self.dict(exclude={"ckpt"})` → `self.model_dump(exclude={"ckpt"})`.

- [ ] **Step 4: `shell.py`, `tempman.py`, `toy_dataset.py`**

`from pydantic.v1 import Field` → `from pipelime.piper import Field` (`toy_dataset.py`, `shell.py`); `from pydantic.v1 import ByteSize, Field` → `from pydantic import ByteSize` + `from pipelime.piper import Field` (`tempman.py`).

- [ ] **Step 5: Leftover grep and commit**

Run: `grep -rn "pydantic.v1\|__config__\|__fields__\|__root__\|\.dict(\|\.json(\|parse_obj\|\.copy(\|\.schema()\|pyd.validator\|@validator\|root_validator\|always=True" pipelime/commands pipelime/piper`
Expected: no matches.

```bash
git add pipelime/commands pipelime/piper
git commit -m "refactor(commands): general/piper/resume/shell/tempman/toy on pydantic v2"
```

---

### Task S2b-T7: Minimal `cli/utils.py` compat — registry names, command resolution, error formatting

**Files:**
- Modify: `pipelime/cli/utils.py`

**Interfaces:**
- Produces: `resolve_pipelime_command(cmd, checkpoint=None) -> tuple[type[PipelimeCommand] | None, Callable[[], PipelimeCommand]]`, `format_validation_error(e, model_cls) -> str`; `get_pipelime_command` keeps its signature.

- [ ] **Step 1: Imports and `_symbol_name`**

Line 12: `from pydantic.v1 import BaseModel, ValidationError` → `from pydantic import BaseModel, ValidationError`.

```python
    @classmethod
    def _symbol_name(cls, symbol):
        import inspect

        from pydantic import BaseModel

        from pipelime.utils.pydantic_compat import model_title

        if inspect.isclass(symbol) and issubclass(symbol, BaseModel):
            return model_title(symbol)
        return symbol.__name__
```
In `print_info` (line ~634) the local `from pydantic.v1 import BaseModel` → `from pydantic import BaseModel`.

- [ ] **Step 2: Command resolution**

Replace `get_pipelime_command` with:

```python
def resolve_pipelime_command(
    cmd: "T_DAG_NODE", checkpoint: t.Optional["CheckpointNamespace"] = None
):
    """Splits a DAG node definition into the command class (None when `cmd` is
    already an instance) and a zero-arg builder returning the command."""
    from pipelime.piper.model import LazyCommand, PipelimeCommand

    cmd_cls, cmd_args = None, {}
    if isinstance(cmd, t.Mapping):
        cmd_name, _cmd_args = next(iter(cmd.items()))
        cmd_cls = get_pipelime_command_cls(cmd_name)
        if _cmd_args and isinstance(_cmd_args, t.Mapping):
            cmd_args = _cmd_args
    elif isinstance(cmd, LazyCommand):
        cmd_cls = cmd

    def _build() -> "PipelimeCommand":
        obj = cmd
        if cmd_cls is not None:
            obj = (
                cmd_cls(**cmd_args)
                if checkpoint is None
                else cmd_cls.init_from_checkpoint(checkpoint, **cmd_args)
            )
        if not isinstance(obj, PipelimeCommand):
            raise ValueError(f"{obj} is not a pipelime command.")
        if checkpoint is not None:
            obj._checkpoint = checkpoint
        return obj

    real_cls = cmd_cls.command_class if isinstance(cmd_cls, LazyCommand) else cmd_cls
    return real_cls, _build


def get_pipelime_command(
    cmd: "T_DAG_NODE", checkpoint: t.Optional["CheckpointNamespace"] = None
) -> "PipelimeCommand":
    return resolve_pipelime_command(cmd, checkpoint)[1]()
```

- [ ] **Step 3: Error formatting** (replaces `show_field_alias_valerr`; keep the old name as an alias so `cli/main.py` still imports until S3)

```python
def format_validation_error(e: ValidationError, model_cls=None) -> str:
    """The error text with `name / alias` locations, as pipelime 2.x printed them.
    `model_cls` is the class that raised (v2 errors do not carry it)."""
    from pipelime.utils.pydantic_compat import iter_fields

    alias_to_name = {}
    if model_cls is not None and model_cls.model_config.get("populate_by_name"):
        alias_to_name = {
            f.alias: f"{f.name} / {f.alias}" for f in iter_fields(model_cls) if f.has_alias
        }

    lines = [f"{e.error_count()} validation error(s) for {e.title}"]
    for err in e.errors():
        loc = " -> ".join(str(alias_to_name.get(p, p)) for p in err.get("loc", ()))
        lines.append(f"{loc or '(root)'}\n  {err['msg']} [type={err['type']}]")
    return "\n".join(lines)


def show_field_alias_valerr(e: ValidationError, model_cls=None) -> str:  # pipelime 2.x name
    return format_validation_error(e, model_cls)
```

- [ ] **Step 4: Commit**

```bash
git add pipelime/cli/utils.py
git commit -m "refactor(cli): registry titles, command resolution and error formatting for v2"
```

---

### Task S2b-T8: Minimal choixe compat — v2 models in `decode` and `$model`

**Files:**
- Modify: `pipelime/choixe/visitors/decoder.py:6,22-23`, `pipelime/choixe/visitors/processor.py:217`
- Modify (tests, v1 forms): `tests/pipelime/choixe/visitors/test_decoder.py:7`, `tests/pipelime/choixe/visitors/test_processor.py:8`

- [ ] **Step 1: Edit**

`decoder.py`: `from pydantic.v1 import BaseModel` → `from pydantic import BaseModel`; `json.loads(data.json(by_alias=True))` → `json.loads(data.model_dump_json(by_alias=True))`.

`processor.py::visit_model`:
```python
    def visit_model(self, node: ast.ModelNode) -> Any:
        symbol_branches = node.symbol.accept(self)
        args_branches = node.args.accept(self)
        branches = self._branches(symbol_branches, args_branches)
        models = []
        for s, a in branches:
            model_cls = import_symbol(s, cwd=self._cwd)
            if not hasattr(model_cls, "model_validate"):
                raise TypeError(
                    f"`$model` needs a pydantic v2 model, got {model_cls!r}: "
                    "pipelime 3 no longer supports pydantic.v1 models "
                    "(see docs/migration/pydantic_v2.md)"
                )
            models.append(model_cls.model_validate(a))
        return models
```

Tests: in both test files `from pydantic.v1 import BaseModel` → `from pydantic import BaseModel` (record in `tests/TEST_CHANGES.md`).

- [ ] **Step 2: Run choixe tests and commit**

Run: `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/choixe`
Expected: passed.

```bash
git add pipelime/choixe/visitors/decoder.py pipelime/choixe/visitors/processor.py tests/pipelime/choixe/visitors/test_decoder.py tests/pipelime/choixe/visitors/test_processor.py tests/TEST_CHANGES.md
git commit -m "refactor(choixe): decode/\$model accept pydantic v2 models"
```

---

### Task S2b-T9: Test edits in scope (v1 API forms only) and the S2 gate

**Files:**
- Modify: `tests/sample_data/cli/extra_commands.py`, `extra_operators.py`, `ckpt_dag.py`; `tests/pipelime/sequences/test_validation.py`, `test_sample.py`, `test_operations.py`, `test_samples_sequences.py`; `tests/pipelime/stages/test_base_stages.py`, `test_entities.py`, `test_item_info.py`; `tests/pipelime/piper/test_command_decorator.py`, `test_commands.py`, `test_dag.py`, `test_dag_parsers.py`; `tests/pipelime/commands/test_pipe.py`, `test_split.py`, `test_interfaces.py`
- Modify: `tests/TEST_CHANGES.md`

- [ ] **Step 1: Downstream-like sample modules (exactly the migration-guide edit)**

- `extra_commands.py`, `extra_operators.py`: `from pydantic.v1 import Field` → `from pipelime.piper import Field` (they pass `piper_port=`; `extra_operators.py` may use plain `pydantic.Field` if it passes no flag — check).
- `ckpt_dag.py`: `import pydantic.v1 as pyd` → `import pydantic as pyd`.

- [ ] **Step 2: Test-local v1 forms**

| File | Edit |
|---|---|
| `sequences/test_validation.py` | `import pydantic.v1 as pyd` → `import pydantic as pyd` (module + 2 local imports); `@pyd.validator("cfg")` → `@pyd.field_validator("cfg")` + `@classmethod`; `extra=pyd.Extra.ignore` → `extra="ignore"`, `pyd.Extra.forbid` → `"forbid"` (lines 30, 33, 149) |
| `sequences/test_sample.py` | `from pydantic.v1 import BaseConfig, Extra, create_model` → `from pydantic import ConfigDict, create_model`; `class SampleConfig(BaseConfig): ...` → `SampleConfig = ConfigDict(arbitrary_types_allowed=True, extra="forbid")`; `class SampleConfig2(BaseConfig): ...` → `SampleConfig2 = ConfigDict(arbitrary_types_allowed=True)` |
| `sequences/test_operations.py:351` | `from pydantic.v1 import ValidationError` → `from pydantic import ValidationError` |
| `sequences/test_samples_sequences.py:12-16` | `X.__config__.title` → `X.model_config.get("title")` |
| `stages/test_base_stages.py:69` | `from pydantic.v1 import BaseModel` → `from pydantic import BaseModel` |
| `stages/test_entities.py` | `from pydantic.v1 import BaseModel, ValidationError, parse_obj_as` → `from pydantic import BaseModel, TypeAdapter, ValidationError`; `parse_obj_as(StageEntity, x)` → `TypeAdapter(StageEntity).validate_python(x)`; `X.__config__.extra` → `X.model_config.get("extra")` (3 places) |
| `stages/test_item_info.py` | `from pydantic.v1 import parse_obj_as` → `from pydantic import TypeAdapter`; `parse_obj_as(T, v)` → `TypeAdapter(T).validate_python(v)` |
| `piper/test_command_decorator.py:4`, `piper/test_dag.py:5` | `from pydantic.v1 import Field(, ValidationError)` → `from pipelime.piper import Field` (+ `from pydantic import ValidationError`) |
| `piper/test_commands.py` | `from pydantic.v1 import BaseModel` → `from pydantic import BaseModel, RootModel`; `class _DotsTestData(BaseModel): __root__: t.Sequence[_DotOpts]` → `class _DotsTestData(RootModel[t.Sequence[_DotOpts]]): pass`; `.parse_obj(` → `.model_validate(`; `dots_test_data.__root__` → `dots_test_data.root` |
| `piper/test_dag_parsers.py` | `from pydantic.v1 import BaseModel` → `from pydantic import BaseModel`; `DAGModel.parse_obj(` → `DAGModel.model_validate(` |
| `commands/test_pipe.py` | `from pydantic.v1 import ValidationError` → `from pydantic import ValidationError`; `PipeCommand.parse_obj(` → `PipeCommand.model_validate(` |
| `commands/test_split.py` | `from pydantic.v1 import ValidationError, parse_obj_as` → `from pydantic import TypeAdapter, ValidationError`; `parse_obj_as(T, v)` → `TypeAdapter(T).validate_python(v)` (6 places); `X.parse_obj(` → `X.model_validate(` (3 places) |
| `commands/test_interfaces.py` | `from pydantic.v1 import ValidationError, create_model` → `from pydantic import ValidationError, create_model`; `model_cls.__fields__[f].field_info.description` → `model_cls.model_fields[f].description`; `.field_info.extra` → `(model_cls.model_fields[f].json_schema_extra or {})`; `model_cls.parse_obj(` → `model_cls.model_validate(`; `for k in model_cls.__fields__` → `for k in model_cls.model_fields`; `for k, v in interf_cls.__fields__.items(): default_values[k] = v.get_default()` → `for k, v in interf_cls.model_fields.items(): default_values[k] = v.get_default(call_default_factory=True)` |

Record every row in `tests/TEST_CHANGES.md`.

- [ ] **Step 3: The S2 gate**

Run:
```bash
.venv/bin/python -m pytest -q -o addopts="" -p no:cacheprovider tests/pipelime/stages tests/pipelime/sequences tests/pipelime/piper tests/pipelime/utils tests/pipelime/choixe tests/pipelime/items
.venv/bin/python -m pytest -q -o addopts="" -p no:cacheprovider -n auto --dist loadgroup tests/pipelime/commands
.venv/bin/python -m pytest -q -o addopts="" -p no:cacheprovider tests/pipelime/test_pydantic_contract.py -k "not HelpRendering and not test_help and not test_tui"
```
Expected: all green (contract xfails now pass: `test_command_decorator_var_keyword_expansion`, `test_to_pipe_roundtrip` — the `strict=True` xfail markers fire *only* while `V1` is true, so they simply pass now). Any failure is analysed with the rule "tests are the oracle": fix the source, never the expectation, unless the test uses a v1 API form (then add a row to `TEST_CHANGES.md`).

- [ ] **Step 4: Commit and ledger**

```bash
git add tests
git commit -m "test: v1 API forms → v2 in stages/sequences/piper/commands tests"
```

Update the ledger (S2 gate command + results). Then open `2026-09-13-pydantic-v2-s3-cli.md`.
