# Pydantic `v1` → native v2 API migration — Design

**Date:** 2026-09-12
**Branch:** `pydantic_v2`
**Status:** approved section by section in the design session; implementation plans in `docs/superpowers/plans/2026-09-13-pydantic-v2-*.md`
**Companion:** `2026-09-12-pydantic-v2-migration-analysis.md` (factual inventory, empirical
pydantic facts, risk map — read it first)

---

## 1. Goal and non-negotiables

Move every pipelime model from the `pydantic.v1` compatibility shim to the native
pydantic v2 API, **without breaking downstream projects** (every project in the
company depends on pipelime) and **without relying on the existing test suite as
the only safety net**.

Decisions taken with the maintainer (all locked):

| Topic | Decision |
|---|---|
| Compatibility policy | **Option A — "imports-only migration"**: downstream projects change `import pydantic.v1` → `import pydantic` (and `@validator` → `@field_validator` where they use `values`/`always`); pipelime's base classes restore every v1 *semantic* that v2 changes (see §3). Deliberate, documented "magic" inside pipelime is preferred over user-facing breaks or silent behaviour changes. |
| Downstream import style today | `pydantic.v1`, like pipelime. pipelime must fail **loudly** when a subclass still uses v1 objects. |
| `Field(piper_port=...)` idiom | pipelime ships its own `Field` wrapper (`pipelime.piper.Field`) accepting `piper_port=`, `pipe_source=`, `expand_help=`, `is_required=`; the raw `pydantic.Field(piper_port=...)` form stays accepted (pydantic warns). |
| Dependencies | `pydantic>=2.10,<3`; add `pydantic-extra-types` (for `Color`); add `pytest-xdist` to the `tests` extra. |
| Version | **3.0.0** (downstream must change imports → breaking release). |
| Prior attempt | `origin/feature/pydanticant` is **not** consulted, diffed, or reused. Everything is designed and implemented from scratch. |
| Modern type hints | `list[int]`, `dict[str, X]`, `tuple[...]`, `X \| None`, `X \| Y` must be first-class for user-defined fields everywhere (validation, help, TUI, `to_pipe`, DAG). Legacy `typing.List`/`Optional` keep working (free). |
| Pre-existing bugs | Fixed in scope: `SamplesSequence.to_pipe()` infinite recursion on `str` fields; `@command` mis-passing `**kwargs` parameters. |
| Execution | Approach 1: bottom-up along the import graph, "expanding green frontier", multi-session with a progress ledger. |
| Tests | Tests are the oracle; contract tests written first against v1; three tiers; `TEST_CHANGES.md` ledger. |

---

## 2. Why the migration is not mechanical (summary of the analysis)

- v1 and v2 models **cannot nest each other**: once a leaf type is converted, every
  model that uses it as a field type must be converted in the same step.
- Several v1 features pipelime relies on are **silently ignored** by v2:
  `__get_validators__` on `BaseModel` subclasses (all compact forms and value
  wrappers), `_iter()`/`dict()` overrides for nested serialization, and nested
  serialization by *declared* type (`{}` for a subclass held in a base-typed field).
- v1 semantics that downstream code depends on and v2 drops: `Optional[X]` without
  default being optional; `{"__root__": ...}` envelopes and `cls(__root__=...)`;
  `Field(**extra_flags)`; `ValidationError.model`; v1 `ModelField` introspection
  (`outer_type_` strips `Optional`).

---

## 3. Architecture: the compat toolkit — `pipelime/utils/pydantic_compat.py` (new)

A single, small, heavily documented module. Every pipelime model builds on it.
Nothing in it depends on `pydantic.v1` at runtime except the guard in §3.1.3,
which is `try`-guarded so the module works under pydantic 3.

### 3.1 `PipelimeModel(BaseModel, metaclass=PipelimeModelMeta)`

Base of `PipelimeCommand`, `SampleStage`, `SamplesSequence`, `BaseEntity`,
`PiperDAG`, the CLI interfaces, `Grabber`, `LocalCheckpoint`, `DataStream`,
`PiperInfo`, `DAGModel`, `ItemInfo`, `ItemValidationModel`,
`SampleValidationInterface`, `EntityAction`, `ParsedItem`, `LazyCommand`, the
progress models — i.e. every non-root pipelime model.

1. **Polymorphic nested serialization (v1 behaviour).** `__get_pydantic_core_schema__`
   takes the schema from the handler and replaces its `serialization` with a
   `wrap_serializer_function_ser_schema(info_arg=True)` whose function does:
   `type(v) is cls` → `handler(v)`; otherwise
   `type(v).__pydantic_serializer__.to_python(v, mode=info.mode, include=info.include,
   exclude=info.exclude, by_alias=info.by_alias, exclude_unset=..., exclude_defaults=...,
   exclude_none=..., round_trip=..., context=..., serialize_as_any=...)`.
   Verified (analysis §5-4): python and json modes, aliases, `exclude_*` flags,
   nested `list`/`dict`/`Optional`, JSON schema generation, no warnings, no
   recursion. It applies to *any* field annotated with a pipelime base class,
   including downstream ones. Top-level `model_dump()` is unaffected.
2. **v1 `Optional`/`None` semantics (metaclass).** In `PipelimeModelMeta.__new__`,
   before delegating to `ModelMetaclass`: (a) every annotation in the class
   namespace that is `Optional[X]` / `X | None` / `Union[..., None]` and has
   **no default in the namespace** gets `= None`; (b) every field whose default
   is `None` (plain or `Field(None)`) and whose annotation is not already
   optional gets its annotation wrapped in `Optional[...]` — v1 set
   `allow_none=True` in that case, so `x: int = None` accepted an explicit
   `None` (verified). Skips `ClassVar` and names starting with `_`. String
   annotations (from `from __future__ import annotations`) are evaluated with
   `eval(ann, module.__dict__, namespace)`; if evaluation fails, a textual check
   (`Optional[`, `| None`, `None |`, `Union[...None...]`) is the fallback.
   (c) `model_config` sets `coerce_numbers_to_str=True`: v1 coerced numbers into
   `str` fields and CLI values are parsed (`+key 0` → `0`) before validation.
3. **v1-leftover guard (metaclass).** If the namespace contains a
   `pydantic.v1.fields.FieldInfo`, a `pydantic.v1.fields.ModelPrivateAttr`, or a
   classmethod/function carrying `__validator_config__`/`__root_validator_config__`
   (v1 validators), raise
   `TypeError("<Class>.<attr> uses pydantic.v1 objects; pipelime 3 requires native
   pydantic v2: use `import pydantic` and `from pipelime.piper import Field`
   (see docs/migration/pydantic_v2.md)")`. Under pydantic 3 (no `pydantic.v1`)
   the guard is a no-op.

Config keys used by pipelime models after migration: `extra`, `populate_by_name`,
`arbitrary_types_allowed`, `frozen`, `title`, `coerce_numbers_to_str`,
`validate_default` (per field).
`copy_on_model_validation` and `underscore_attrs_are_private` disappear (v2
defaults match).

### 3.2 `PipelimeRootModel[T](RootModel[T])`

Base for the value wrappers (`NumpyType`, `YamlInput`, `TypeDef`, `CallableDef`,
`StageInput`, `NodesDefinition`, `Transformation`).

- `__init__(self, root=PydanticUndefined, /, **data)`: accepts the positional
  root, `__root__=<value>` (v1 spelling), and nothing else.
- `__root__` read-only property returning `self.root` — attached **after** class
  creation (`PipelimeRootModel.__root__ = property(...)`) because pydantic rejects
  the name inside a class body.
- `.value` property (existing API), `create(value)` classmethod →
  `model_validate(value)`, `validate(value)` classmethod kept as a public alias.
- `dict(**kw)` returns the v1 envelope `{"__root__": self.model_dump(**kw)}`;
  `model_dump()` / `model_dump_json()` follow v2 (bare value). Nested dumps unwrap
  in both versions, so only the top-level `.dict()` needed the override.
- Coercion pattern: a `model_validator(mode="wrap")` classmethod
  `_validate_root(value, handler)` that returns `value` unchanged when
  `isinstance(value, cls)` (identity pass-through, as v1) and otherwise calls
  `handler(cls._coerce(value))`, where `_coerce` is the per-type classmethod
  hook (subclasses override `_coerce`, never the validator).
- Output pattern: a `model_serializer` (`plain` or `wrap`) implementing what v1's
  `_iter()`/`dict()` override produced.
- Polymorphism of the root value (e.g. `StageInput.root: SampleStage`) comes from
  `PipelimeModel` (§3.1.1); no extra work.

### 3.3 `Field(...)`

`def Field(default=PydanticUndefined, **kwargs)`: every keyword that
`pydantic.Field` does not declare (`piper_port`, `pipe_source`, `expand_help`,
`is_required`, and any other custom flag — `test_interfaces.py` passes arbitrary
ones through `pyd_field(**kwargs)`) is merged into `json_schema_extra` (a dict;
a callable `json_schema_extra` is wrapped) and the rest is forwarded to
`pydantic.Field(...)`.
Re-exported as `pipelime.piper.Field`; `PydanticField*Mixin.pyd_field()` uses it.
`field_extra(field_info, key, default)` reads a flag back from
`json_schema_extra` (dict only; callables yield `default`).

### 3.4 Introspection helpers

- `FieldView` (frozen dataclass): `owner` (the model class), `name`, `alias` (may be `None`),
  `effective_alias` (`alias or name` — v1's `ModelField.alias`), `has_alias`,
  `annotation`, `inner_type` (annotation with `None` stripped from
  `Optional`/`Union`/`X | None`, exactly v1's `outer_type_`), `required`
  (`FieldInfo.is_required()`), `default` (`get_default(call_default_factory=True)`;
  `PydanticUndefined` → `Ellipsis` for display), `description`, `exclude`,
  `extra` (dict from `json_schema_extra`), `is_model` (inner type is a
  `BaseModel` subclass), `root_type` (inner type's root annotation
  when it is a `RootModel`, else `None`), `populate_by_name` (from the owner's
  config), `field_info`.
- `iter_fields(model_cls) -> Iterator[FieldView]` in declaration order;
  `get_field(model_cls, name)`.
- `model_title(cls)` → `model_config.get("title") or cls.__name__`.
- `type_info(tp)` → `(origin, args, is_union, is_optional)` treating
  `types.UnionType` and `typing.Union` identically; used by help/TUI rendering.

These replace every `__fields__`, `outer_type_`, `field_info.extra`,
`__config__.title`, `.required`, `.get_default()` access in `cli/`, `piper/`,
`sequences/`, `stages/`.

---

## 4. Per-layer design (bottom-up = subtask order)

### 4.1 Value wrappers — `utils/pydantic_types.py`

- `NumpyType`, `YamlInput`, `CallableDef`, `TypeDef[T]` → `PipelimeRootModel[...]`
  (`TypeDef(PipelimeRootModel[Type[TRoot]], Generic[TRoot], frozen=True)`;
  `CallableDef` `frozen=True`; `arbitrary_types_allowed=True` where needed).
  Coercion code moves verbatim into `_coerce`; `ItemType`, `BaseEntityType`,
  `ActionDef` override `_coerce`/`default_class_path` only.
- Serializers: `NumpyType` → `{"object": tolist(), "dtype": name, ["order": "F"]}`;
  `TypeDef`/`CallableDef` → symbol string via the existing `_type_to_string` /
  `_callable_to_string`; `YamlInput` → the value.
- `TypeDef.wrapped_type()` → `get_args(cls.model_fields["root"].annotation)[0]`.
  `__hash__`, `__call__`, `__str__`, `__repr__`, `__piper_repr__`, signature
  helpers unchanged.
- `NewPath(Path)`: `__get_pydantic_core_schema__` = path schema +
  `no_info_after_validator_function(cls.validate)`; `__get_pydantic_json_schema__`
  adds `exists=False` / `extension`. `new_file_path()` unchanged. Validation
  returns a `Path` (as v1's `path_validator` did).
- `ItemValidationModel(PipelimeModel, extra="forbid")`: `class_path: ItemType`,
  alias `validator` kept; `make_field()` → `(type, Field(...))`;
  `make_validator_method()` → `field_validator(field_name)(fn)` on the
  `exec`-generated uniquely named function (v2 also rejects duplicate validator
  names in `create_model`).
- `SampleValidationInterface(PipelimeModel, extra="forbid")`: `_make_schema` →
  `create_model("SampleSchema", __config__=ConfigDict(arbitrary_types_allowed=True,
  extra="ignore"|"forbid"), __validators__=..., **fields)`; `as_pipe()` →
  `model_dump(by_alias=True)`. Union `sample_schema: Type[BaseModel] | str |
  Mapping[str, ItemValidationModel]` verified to resolve correctly in v2 smart mode.

### 4.2 Command framework — `piper/model.py`, `piper/checkpoint.py`, `piper/progress/model.py`, `commands/piper.py`

- `PipelimeCommand(PipelimeModel, ABC, populate_by_name=True, extra="forbid")`;
  `force_gc` / `no_default_checkpoint` class kwargs unchanged.
- `@command`: keeps `type()`-based construction, custom `__init__` for positional
  arguments, and the forced `__signature__`. Changes: unannotated parameters get a
  synthesized annotation as v1 inferred it — `type(default)` when a plain
  (non-`FieldInfo`, non-`None`) default exists, else `Any`; `*args`/`**kwargs`
  become `tuple[X, ...]` / `dict[str, X]`; **bug fix:** `run()` expands the
  var-keyword field with `**` instead of passing it as one keyword.
  `FieldInfo`/`PydanticUndefined` from v2.
- `get_inputs()/get_outputs()` via `field_extra(..., "piper_port")`;
  `command_title()` via `model_title`.
- `LazyCommand(PipelimeModel, Generic[CmdTp], extra="forbid")`: `__getattr__` /
  `__setattr__` overrides and `_LazyCommand.__new__` unchanged (verified);
  defaults via `model_fields[name].get_default(call_default_factory=True)`.
- `NodesDefinition(PipelimeRootModel[Mapping[str, PipelimeCommand]])`: the node
  building loop becomes `_build_nodes(value, *, checkpoint=None, skip_on_error=False)`;
  `create(value, *, checkpoint, skip_on_error)` calls it and instantiates;
  `_coerce` (used when a raw mapping is validated as a field, e.g. `DAGModel.nodes`)
  calls it with the defaults; `wrap` serializer turns the handler output into
  `{node: {title: <cmd dump>}}` so `by_alias`/`exclude_*` propagate as v1's `_iter`
  did. `DAGModel(PipelimeModel, extra="forbid")`, `PiperInfo` likewise.
- `piper_dag`: `cls.schema()["title"]` → `model_title(cls)`;
  `PiperDAG(PipelimeModel, ABC, populate_by_name=True, extra="forbid")`.
- `LocalCheckpoint(Checkpoint, PipelimeModel)` — MRO order kept;
  `folder: DirectoryPath | NewPath` verified. `OperationInfo(frozen=True)` /
  `ProgressUpdate` → `model_dump_json()` / `model_validate_json()` on the ZMQ wire.
- `commands/resume.py`: `model_validate`, `model_dump(exclude=...)`, `conint`
  unchanged; `tempman.py` `ByteSize` unchanged.

### 4.3 Stages & entities — `stages/*`

- `SampleStage(PipelimeModel, ABC, extra="forbid", populate_by_name=True)`;
  `title=` kwargs unchanged; `StageTimer` reads the stage title via `model_title`.
- `StageInput(PipelimeRootModel[SampleStage])`: `_coerce` = existing
  `create_stage_from_config` dispatch; serializer emits `{title: <stage dump>}`
  (v1's `dict()` override applied to nested dumps as well — behaviour preserved).
- `StageEntity(SampleStage, title="entity")` — cannot be a `RootModel` (`extra`
  clash). Field `entity_action: EntityAction`; custom
  `__init__(self, __root__=<missing>, /, **data)` accepting all v1 call shapes:
  positional spec, `__root__=` keyword, the `{"__root__": ...}` envelope (configs
  written by pipelime 2.x `to_pipe()`), and the bare `{action: ..., input_type: ...}`
  kwargs form (v1 reached it through a `TypeError` fallback). `wrap` serializer
  emits the bare `EntityAction` dump. `__call__` unchanged
  (`Sample(action(input_type(**x)).model_dump())`).
- `ParsedItem(PipelimeModel, Generic[ItTp, ValTp], extra="forbid",
  arbitrary_types_allowed=True)`: the three-way coercion in a `wrap` model
  validator with instance pass-through; `raw_item_type()` /
  `parsed_value_type()` via `model_fields[...].annotation`;
  `make_parsed_value` uses `TypeAdapter(pvtp).validate_python` for models.
  `ParsedData` unchanged.
- `DynamicKey` / `ModelDynamicKey`: unchanged design; `create_model(...,
  __config__=ConfigDict(arbitrary_types_allowed=True))`, `model_validate`.
- `BaseEntity(PipelimeModel, extra="allow", arbitrary_types_allowed=True)`:
  `__init__` conversion uses `FieldView.inner_type` / `.required`; `wrap`
  serializer skips `None` values and replaces `ParsedItem` fields by their
  `raw_item` (detected on the instance attribute, not by dict sniffing); `merge()`
  on `model_dump()` + `iter_fields`. Docs' `debug: Optional[ImageItem]` (no
  default) keeps working through the metaclass.
- `EntityAction(PipelimeModel, extra="forbid")`: `field_validator("action")`;
  `field_validator("input_type")` + `validate_default=True` reading
  `info.data["action"]`; `wrap` validator wrapping a bare action into
  `{"action": value}`.
- `Transformation(PipelimeRootModel[dict[str, Any]])`: `_coerce` from
  albumentations objects / dicts; `_value` private attr built in `model_post_init`.
  `Color` → `pydantic_extra_types.color.Color`.
- Other stages: `validator` → `field_validator` (+`validate_default=True` where
  `always=True`), `root_validator` → `model_validator(mode="after")`.

### 4.4 Sequences — `sequences/*`

- `SamplesSequence(SamplesSequenceBase, PipelimeModel, extra="forbid")` and
  `DataStream(...)` keep their MRO order (non-pydantic base first: its `__iter__`
  must win over `BaseModel.__iter__`). `name()` via `model_title`.
- `to_pipe()` / `piped_sequence()` iterate `iter_fields`: `effective_alias`,
  `pipe_source` from `extra`, nested models via `model_dump()`.
  **Bug fix:** `_maybe_go_deeper` excludes `str`/`bytes` from the `Sequence`
  branch (today any `str` field recurses forever). A `to_pipe()` ⇄ `build_pipe()`
  round-trip test is added.
- `pipes/base.py` `source` → pipelime `Field(..., exclude=True, pipe_source=True)`;
  custom `__init__`s in `mapping.py`, `validation.py`, `utils.py` unchanged;
  `display_errors(e.errors())` → `str(e)`; `from_callable` `Callable` fields
  verified; `Grabber(PipelimeModel, extra="forbid")`.

### 4.5 Interfaces & commands — `commands/*`

- Compact forms (`GrabberInterface`, `InputDatasetInterface`,
  `OutputDatasetInterface`, `Interval`, `ToyDatasetInterface`, `PercSplit`,
  `AbsoluteSplit`, …): one `model_validator(mode="wrap")` per class with instance
  pass-through (identity kept), compact parsing moved into a `_compact_to_data()`
  classmethod hook, then `handler(data)`. `extra="forbid"` errors keep their
  `loc` (verified).
- `PydanticFieldMixinBase` and friends stay first in the MRO; `pyd_field()` →
  pipelime `Field`.
- `validator(..., always=True)` → `field_validator` + `validate_default=True` on
  the field; `values` → `info.data`. Field declaration order is preserved so the
  cross-field checks (`pipe`←`folder`, `exists_ok`←`folder`,
  `filter_fn`←`filter_query`, `input_type`←`action`) keep seeing their inputs;
  contract tests assert it.
- `SerializationModeInterface` custom `__init__` unchanged; `.copy(update=)` →
  `model_copy(update=)`; `.dict(by_alias=True)` → `model_dump(by_alias=True)`;
  `parse_obj` → `model_validate`; all pipelime-internal `pyd.Field(..., piper_port=...)`
  → pipelime `Field`. Command classes are otherwise untouched.

### 4.6 CLI help, TUI, error display — `cli/*`

- `pretty_print._field_row`, `_iterate_model_fields`, `_get_signature`,
  `print_model_field_values`; `tui/utils.init_tui_field`,
  `init_stageinput_tui_field`, `get_field_type`, `is_tui_needed`; `tui/tui.py`
  field loop — all consume `FieldView`. Root-type detection via
  `FieldView.root_type`. `_human_readable_type` rewritten on `type_info`:
  `list[int]`/`List[int]` → `[int, ...]`, `dict`→`{k: v}`, `tuple`→`(a, b)`,
  unions (both spellings) → `a | b`, `Optional` stripped upstream by
  `inner_type`, enums as today, never `.__name__` on non-classes.
- `inspect.signature(model_cls)` still drives `_get_signature`.
- `show_field_alias_valerr(e)` → `format_validation_error(e, model_cls) -> str`
  rebuilding the message with `name / alias` locations (v2 errors cannot be
  mutated and carry no model); call sites (`cli/main.py` run path,
  `NodesDefinition._coerce`) pass the class and print the text before re-raising.
- `print_model_info` / `print_info` work for any v2 `BaseModel`;
  `PlCliOptions.model_dump()`; `ValidationError` from `pydantic`.
- A contract test pins the rendered help rows (names, aliases, types, ports,
  defaults) of a representative command before migration.

### 4.7 Choixe — `choixe/ast/nodes.py`, `visitors/decoder.py`, `visitors/processor.py`

- AST nodes → stdlib `dataclasses.dataclass` with identical `init=`/`eq=`/
  `unsafe_hash=` arguments (v2 pydantic dataclasses break `init=False` +
  `__init__(*nodes)`; no node validation is relied upon — the choixe suite is
  the gate).
- `Decoder`: `isinstance(data, pydantic.BaseModel)` + `model_dump_json(by_alias=True)`.
- `$model` directive: `model_validate(args)`; if the imported symbol is a
  `pydantic.v1` model, raise a clear error pointing to the migration guide.

### 4.8 Modern type hints (cross-cutting)

All introspection goes through `FieldView`/`type_info` (§3.4), which treat
`types.UnionType` and `typing.Union`, builtin generics and `typing` aliases
identically. Pipelime's own field declarations, docs and examples switch to
modern hints. New-feature contract tests: a command / stage / piped sequence /
entity declared with `list[int]`, `dict[str, float]`, `tuple[int, str]`,
`int | None`, `int | str`, `list[SampleStage] | None` validates, renders help,
opens the TUI (`get_field_type`, `init_tui_field`, `is_tui_needed`), round-trips
through `to_pipe`, and serves as a DAG node. (Only the TUI cases fail on v1
today; recorded as such in the ledger.)

---

## 5. Testing strategy

### 5.1 Principles

- **Tests are the oracle.** The suite encodes v1 behaviour, which is what option A
  preserves. A test may be edited **only** when it uses a v1 API *form* in
  test-local code (`from pydantic.v1 import ...`, `pyd.validator` on a test-defined
  model, `BaseConfig`/`Extra`, `NumpyType(__root__=...)`, `parse_obj_as`, v1
  `ValidationError`), never to accommodate a behaviour change. Every edit is
  listed in `tests/TEST_CHANGES.md` with a one-line justification.
- `tests/sample_data/cli/*.py` are treated as **downstream code**: they receive
  exactly the migration-guide edit and nothing else.

### 5.2 Contract tests — `tests/pipelime/test_pydantic_contract.py` (subtask 0)

Written **before any source change**, against v1, and kept green through every
subtask. One test (or small class) per pinned behaviour:

compact forms for every interface and split type · polymorphic nested dumps
(stage inside `StageInput`, `Sequence[StageInput]`, DAG `NodesDefinition`,
`to_pipe`) · `Optional`-without-default on command / stage / entity / sequence
subclasses · root wrappers (`cls(__root__=...)`, positional, `.__root__`,
`.value`, `.dict()` envelope, nested unwrap, instance identity pass-through,
`create()`/`validate()`) · `piper_port`/`pipe_source` discovery through
`pipelime.piper.Field` **and** raw `pydantic.Field(piper_port=...)` ·
`@command` with unannotated / positional-only / `*args` / `**kwargs` parameters
and its `__signature__` · `LazyCommand` attribute access and `lazy()` ·
`StageEntity` call shapes incl. the 2.x `{"__root__": ...}` envelope ·
entity `merge` / `DynamicKey` / `ParsedItem` conversions · `EntityAction`
`input_type` inference · validator ordering (`pipe`/`folder`, `exists_ok`,
`filter_fn`) · pickling a piped sequence with stages/callables (grabber path) ·
rendered `help` rows of a representative command · `ValidationError` alias
formatting · `SampleValidationInterface` dynamic schema incl. shared/optional
checks and `ignore_extra_keys` · `Transformation` / `Color` · MRO: iterating a
`SamplesSequence`/`DataStream` yields samples · modern type hints (§4.8) ·
the two bug fixes (§4.2, §4.4) as *expected-failure-on-v1* tests.

### 5.3 Tiers

| Tier | What | When | Time |
|---|---|---|---|
| 0 | `tests/pipelime/{utils,stages,piper,sequences,choixe}` minus grabber/TUI/ZMQ + contract tests | after every change | ~15 s |
| 1 | the subtask's module tests + Tier 0 + a curated `commands` slice (one `nproc`) | subtask gate | 1–3 min |
| 2 | full suite with `pytest-xdist` (`-n auto --dist loadgroup`; the pipelime user dir is isolated per worker, the two ZMQ tests share one group), tox on 3.10–3.13 | end of subtasks 3/4/5, pre-merge | ~2–3 min |

The final Tier 0 run adds `-W error::pydantic.PydanticDeprecatedSince20` to prove
pipelime itself is warning-free.

---

## 6. Downstream & release

- `docs/migration/pydantic_v2.md`: (1) `import pydantic.v1` → `import pydantic`;
  (2) `@validator` → `@field_validator` (+`validate_default=True` for
  `always=True`; `info.data` for `values`), `@root_validator` → `@model_validator`;
  (3) optional: `pydantic.Field(piper_port=...)` → `pipelime.piper.Field` to
  silence pydantic's deprecation warning; (4) what stays identical (`Optional`
  semantics, `.dict()`/`__root__`, compact forms, polymorphic dumps, `.dict()`/
  `.json()`/`parse_obj` aliases); (5) loud errors you may meet (v1-leftover guard,
  `$model` with a v1 model) and their fixes; (6) modern type hints now supported.
- Docs, `examples/`, README and docstrings switch to the new imports and to
  modern type hints; `pyproject.toml`: `pydantic>=2.10,<3`, `pydantic-extra-types`,
  `pytest-xdist` (tests extra), version `3.0.0`; the "enables `import pydantic.v1`"
  comment goes.
- Pre-merge, maintainer-only task: run one real company project's test suite
  against the branch after applying the guide (checklist in the plan).

---

## 7. Execution protocol (multi-session)

- Work stays on `pydantic_v2`; one commit per subtask (or per file group inside a
  large subtask); no squash/rebase mid-way, so `git log` is the resume point.
- `docs/superpowers/plans/2026-09-12-pydantic-v2-migration-progress.md` is the
  **ledger**: per subtask → status, gate command, commit hash, surprises/notes.
  A session starts by reading analysis → spec → plan → ledger and **re-running
  the last completed gate** before touching code; it ends by updating the ledger,
  even mid-subtask.
- Subtask order = import graph:

| # | Subtask | Gate |
|---|---|---|
| 0 | test infra (`pytest-xdist`, tiers, `TEST_CHANGES.md`) + contract tests on v1 | contract tests green on v1 (except the documented expected failures) |
| 1 | compat toolkit + `utils/pydantic_types.py` | `tests/pipelime/utils` + toolkit unit tests |
| 2 | the model graph: `stages` + `sequences` (2a), then `piper` + `commands` + the minimal `cli/utils.py` registry/error compat (2b) — one unit, because the stage/command registry (`PipelimeSymbolsHelper.import_everything`) imports all of them | `tests/pipelime/{stages,sequences,piper,commands}` + contract tests minus the CLI-rendering ones |
| 3 | `cli` (help, TUI, errors, main) | **full suite green (Tier 2)** |
| 4 | `choixe` | choixe tests + Tier 2 |
| 5 | docs, examples, migration guide, deps, version, downstream smoke test + release checklist | Tier 2 + warning-free Tier 0 + tox + maintainer sign-off |

Between subtask 1 and the end of subtask 2 the package is intentionally not
fully importable; subtask 2a is checked with import smoke tests and
direct-construction tests only, and the real gate sits at the end of 2b.

---

## 8. Known residual risks

- pydantic minor releases keep changing introspection details (`FieldInfo`,
  signature generation, `json_schema_extra`); the toolkit isolates them and the
  contract tests detect drift. Pin `<3`.
- The `Optional` metaclass relies on evaluating string annotations; the textual
  fallback may misclassify exotic aliases (e.g. a user alias `MaybeInt = int | None`
  used as a string). Such a field becomes required — loud (`missing`), not silent.
- v2 union resolution is "smart" rather than left-to-right; the unions pipelime
  declares were checked, but downstream unions with overlapping members may pick
  a different branch. Called out in the migration guide.
- Multiprocessing pickling of v2 models is verified for the shapes pipelime
  uses; exotic downstream models may differ (`Callable` fields with lambdas were
  never picklable).
