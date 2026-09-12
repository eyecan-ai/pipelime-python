# Pydantic `v1` → native v2 API — Usage Analysis

**Date:** 2026-09-12
**Branch:** `pydantic_v2` (== `main` @ `57ccd28`, v2.3.0)
**Installed:** pydantic 2.12.5 / pydantic-core 2.41.5 / Python 3.11.14
**Status:** analysis only — no design decisions are taken here (see the design spec).

This document is the factual inventory of *where and how* pipelime uses pydantic,
which v1 constructs are involved, what pydantic v2 does with each of them
(verified empirically against the installed version, see §5), and where the
migration risk is concentrated. It is written so that it can be read cold in a
future session.

---

## 1. Starting point

- pipelime already depends on the **pydantic 2.x package** (`pydantic>=1.10.17,<3`)
  but every model is written against the **`pydantic.v1` compatibility shim**
  (`import pydantic.v1 as pyd`, `from pydantic.v1 import ...`). That shim is
  scheduled for removal in pydantic v3.
- The migration is therefore **`pydantic.v1` API → native pydantic v2 API**,
  not a package upgrade.
- **39 of 114** source files under `pipelime/` import pydantic (~20k LOC total);
  **21** test files and **3** `tests/sample_data` "downstream-like" modules do too.
- Docs and `examples/` still show `from pydantic import Field` (pre-shim era);
  `examples/cli/my_command.py` uses `from pydantic.v1 import Field`.

### 1.1 Out of scope by decision

A remote branch `origin/feature/pydanticant` contains an earlier attempt. By
explicit decision (2026-09-12) it is **not** to be consulted, diffed, or reused:
this migration is designed and implemented from scratch.

---

## 2. Inventory of v1 constructs (measured on `pipelime/`)

| Construct | Files | Refs | v2 target | Notes |
|---|---:|---:|---|---|
| `__root__` custom-root models | 7 | 66 | `RootModel[T]` | see §3.1 |
| `__get_validators__` overrides (on **BaseModel subclasses** and on `NewPath`) | 7 | 19 | `model_validator(mode="wrap")` / `__get_pydantic_core_schema__` | **silently ignored** by v2 (§5-2) |
| `__modify_schema__` | 1 | 1 | `__get_pydantic_json_schema__` | `NewPath` only |
| `_iter()` overrides (custom nested serialization) | 3 | 5 | `model_serializer` | affects *nested* `.dict()` in v1 |
| `GenericModel` | 4 | 7 | `BaseModel, Generic[T]` | `TypeDef`, `ParsedItem`, `LazyCommand` |
| `@validator` / `@root_validator` | 14 | 25 | `field_validator` / `model_validator` | 12 use `always=True`, 22 use `values` |
| `copy_on_model_validation="none"` | 11 | 27 | drop (v2 default = no copy) | |
| `allow_population_by_field_name` | 7 | 8 | `populate_by_name=True` | |
| `allow_mutation=False` | 1 | 2 | `frozen=True` | `TypeDef`, `CallableDef` |
| `underscore_attrs_are_private` | 4 | 8 | drop (v2 default) | |
| `Extra.forbid/ignore`, `BaseConfig` subclasses | 2 | 4 | string literals / `ConfigDict` | in dynamic `create_model` |
| `create_model(__config__=Config, __validators__=...)` | 3 | 3 | same API, `ConfigDict` | `SampleValidationInterface`, `DynamicKey`, `piper_dag` |
| `__fields__` / `__config__` | 8/8 | 24/15 | `model_fields` / `model_config` | |
| `field_info.extra[...]` flags | 3 | 5 | `json_schema_extra` | see §3.4 |
| `ModelField.outer_type_/.required/.alias/.name/.get_default()/.has_alias/.model_config` | 3 | ~30 | `FieldInfo.*` | CLI help + TUI, see §3.5 |
| `pydantic.v1.typing` helpers, `error_wrappers`, `display_errors` | 2 | 5 | `typing` / `ValidationError` | |
| `parse_obj` / `parse_raw` / `parse_obj_as` | 6 | 25 | `model_validate*` / `TypeAdapter` | mostly docstrings |
| `.dict()` / `.json()` / `.copy(update=)` | 10 | 26 | `model_dump*` / `model_copy` | v2 keeps deprecated aliases |
| `PrivateAttr` | 13 | 36 | unchanged | |
| custom `__init__(self, **data)` on models | 9 | 14 | unchanged (v2 honours custom `__init__` also in nested validation, §5-1) | |
| `Field(..., piper_port=/pipe_source=/is_required=/expand_help=)` | 10 | 68 | `json_schema_extra` (+ DeprecationWarning) | **public API**, §3.4 |
| `pydantic.v1.dataclasses.dataclass` (choixe AST) | 1 | 23 decorators | stdlib `dataclasses` | v2 breaks `init=False` + custom `__init__` (§5-6) |
| `pydantic.v1.color.Color` | 1 | 1 | `pydantic.color` (deprecated) or `pydantic_extra_types` | |
| `cls.__signature__ = ...` overrides | 2 | 3 | works in v2 (§5-7) | |
| `cls.schema()["title"]` | 1 | 1 | `model_config["title"]` | `piper_dag` |

---

## 3. Where the awkward features live

### 3.1 Custom-root "value wrapper" types — `pipelime/utils/pydantic_types.py`

`NumpyType`, `YamlInput`, `TypeDef[T]` (+`ItemType`, `BaseEntityType`),
`CallableDef` (+`ActionDef`), `StageInput`, `NodesDefinition`, `Transformation`,
`StageEntity`. Common pattern:

- `__root__: X` + `create()` + `.value` + `__get_validators__` yielding
  `cls.validate` (so the model **replaces its own validation** when used as a
  field type — accepting strings, classes, callables, mappings, instances…);
- `_iter()` override so that *nested* serialization emits a string / list /
  dict instead of the raw object (`TypeDef` → class path, `CallableDef` →
  symbol path, `NumpyType` → `{"object","dtype","order"}`);
- v1 semantics relied upon: top-level `.dict()` returns `{"__root__": v}`
  while nested `.dict()` unwraps; `cls(__root__=...)` construction;
  `cls.__fields__["__root__"].outer_type_` for generic introspection.

Special cases:

- `StageEntity(SampleStage, title="entity")` is a `__root__` model **that also
  inherits from `SampleStage`** (which sets `extra="forbid"`). v2 forbids
  `extra` on `RootModel` (§5-11), so it cannot be a `RootModel`; it must become
  a regular model with a validator/serializer that preserves the config shape
  (`entity: <action>` / `entity: {action: ..., input_type: ...}`).
- `ActionDef(CallableDef)` **overrides** `__get_validators__` to prepend a
  registry lookup, then delegates to the parent's `validate`.
- `Transformation` (`stages/augmentations.py`) is a `__root__: Dict` model
  with a `PrivateAttr` built in `__init__` from albumentations.

### 3.2 Command framework — `pipelime/piper/model.py`

- `PipelimeCommand(BaseModel, ABC, allow_population_by_field_name=True, extra="forbid")`
  with class kwargs `force_gc` / `no_default_checkpoint` consumed by
  `__init_subclass__` (v2 forwards unknown class kwargs the same way, §5-3).
- `@command` decorator builds a model class **dynamically with `type(...)`**:
  `__annotations__` from the function signature, defaults as `Field(...)`,
  config passed as class kwargs, positional-argument handling in a custom
  `__init__`, and a **forced `__signature__`** used by the help printer.
  v1 accepted *unannotated* parameters (type inferred from default); v2 raises
  `PydanticUserError` for non-annotated attributes → the decorator must
  synthesize annotations.
- `LazyCommand(GenericModel)` overrides `__setattr__`/`__getattr__` and
  `_LazyCommand.__new__` returns an instance of a different class.
- `get_inputs()/get_outputs()` read the **`piper_port` flag from
  `field_info.extra`** (see §3.4).
- `NodesDefinition` (`__root__: Mapping[str, PipelimeCommand]`) serializes each
  node as `{title: cmd.dict()}` via `_iter`; `DAGModel.purged_dict()` feeds
  choixe. **Polymorphic serialization** (field typed as the base class, value is
  a subclass) is essential here (§5-4).

### 3.3 Stages & entities — `pipelime/stages/base.py`, `entities.py`

- `SampleStage` base (`extra="forbid"`, `allow_population_by_field_name`);
  stages use `title="..."` class kwarg and `cls.__config__.title`.
- `StageInput` (`__root__: SampleStage`) parses `"name"` / `{"name": args}` via
  `create_stage_from_config` and **overrides `dict()`** to emit `{title: ...}` —
  in v1 the override is honoured for *nested* serialization too.
- `ParsedItem[ItTp, ValTp](GenericModel)` overrides `__get_validators__` to
  build itself from an `Item`, a parsed value or raw data; reads
  `__fields__[...].outer_type_` to discover its type parameters.
- `DynamicKey(...)` returns a `PrivateAttr` whose default is a slotted
  `ModelDynamicKey` object; `BaseEntity.__init__` walks
  `__private_attributes__` to set `owner = self`, and `validate(key)` builds a
  throw-away model with `create_model(__config__=BaseConfig subclass)`.
- `BaseEntity(extra="allow", arbitrary_types_allowed=True)`: custom `__init__`
  converts raw values into `Item`s using `outer_type_` / `.required`;
  `_iter` skips `None` and unwraps `ParsedItem` → `raw_item`; `merge()` uses
  `.dict()` + `__fields__`.
- **User-facing v1 semantics used in the docs**: `Optional[ImageItem]` fields
  *without* a default are optional (v1) but **required** in v2 (§5-5);
  `@validator("x", always=True)` with `values`.
- `EntityAction`: `validator("input_type", always=True)` inspects `values["action"]`.

### 3.4 `Field(...)` extra kwargs as pipelime metadata (**public API**)

`piper_port=PiperPortType.INPUT/OUTPUT` (68 refs in pipelime, used by every
downstream command), `pipe_source=True` (sequence pipes), `is_required=`,
`expand_help=`. In v2 these still land in `FieldInfo.json_schema_extra` but
emit `PydanticDeprecatedSince20` warnings at class-definition time and are
slated for removal (§5-9). `PydanticField*Mixin.pyd_field()` in
`commands/interfaces.py` wraps this for interface types.

### 3.5 CLI help / TUI introspection — `cli/pretty_print.py`, `cli/utils.py`, `cli/tui/utils.py`, `cli/tui/tui.py`

Built entirely on v1 `ModelField`: `.outer_type_` (which in v1 **strips
`Optional[...]`**), `.field_info.extra`, `.field_info.description/exclude`,
`.name`, `.alias` (always set in v1, `None` when unset in v2, §5-10),
`.has_alias`, `.model_config.allow_population_by_field_name`, `.required`,
`.get_default()`, `"__root__" in outer_type_.__fields__`, plus
`pydantic.v1.typing` helpers (`WithArgsTypes`, `is_union`, `typing_base`).
`show_field_alias_valerr()` mutates `ValidationError.errors()` in place and
reads `e.model` — neither works in v2 (§5-20).

### 3.6 Interfaces & compact forms — `commands/interfaces.py`, `commands/split_ops.py`

`GrabberInterface`, `InputDatasetInterface`, `OutputDatasetInterface`,
`Interval`, `ToyDatasetInterface`, `PercSplit`, `AbsoluteSplit`, … all define
`__get_validators__` to accept a **compact string form** (`"folder,True"`,
`"4,2"`, `"0.3,out"`) in addition to mappings. This is the CLI's primary
argument syntax. `validator(..., always=True)` + `values` cross-field checks
are common (`pipe`/`folder`, `exists_ok`, `filter_fn`/`filter_query`).

### 3.7 Sequences — `sequences/samples_sequence.py`, `pipes/*`, `sources/*`, `utils.py`, `grabber.py`

`SamplesSequence(SamplesSequenceBase, BaseModel)` — non-pydantic ABC first in
the MRO; `to_pipe()` iterates `__fields__` reading `.alias` and
`field_info.extra["pipe_source"]`; `piped_sequence`/`source_sequence` build
helper functions with `inspect.signature(cls)`; `Callable` fields
(`from_callable`); models are **pickled to worker processes** by the grabber.

### 3.8 Choixe — `choixe/ast/nodes.py`, `choixe/visitors/decoder.py`, `processor.py`

AST nodes are `pydantic.v1.dataclasses.dataclass` (23 decorators, several with
`init=False` + custom `__init__(*nodes)`); v2 pydantic dataclasses validate
positional args and break this pattern (§5-6). `Decoder` serializes any
`BaseModel` literal with `.json(by_alias=True)`; the `$model` directive calls
`import_symbol(s).parse_obj(a)` on **user** models.

### 3.9 Misc

`piper/progress/model.py` (`OperationInfo(frozen=True)`, `ProgressUpdate` —
JSON over ZMQ via `.json()`/`parse_raw()`); `piper/checkpoint.py`
(`LocalCheckpoint(Checkpoint, BaseModel)`, `Union[DirectoryPath, NewPath]`);
`commands/resume.py` (`conint`, `parse_obj`, `dict(exclude=)`);
`commands/tempman.py` (`ByteSize`); `stages/augmentations.py`
(`pydantic.v1.color.Color`); `cli/main.py` (`PlCliOptions.dict()`).

---

## 4. Risk map

### 4.1 Silent breakages (no error, wrong behaviour) — highest priority

1. **`__get_validators__` on a `BaseModel` subclass is ignored by v2** (§5-2,
   no warning): every compact form, every "value wrapper" type, `ParsedItem`,
   `EntityAction`, `StageInput`, `NodesDefinition` would stop accepting their
   alternative inputs.
2. **Nested serialization uses the *declared* type, not the runtime type**
   (§5-4, no warning in 2.12): `StageInput.root: SampleStage`,
   `NodesDefinition`, `Sequence[StageInput]`, any user field typed with a
   pipelime base class → `{}` in dumps, DAG/checkpoint/`to_pipe` corruption.
3. **`_iter()` / `dict()` overrides no longer affect nested dumps** → raw
   `np.ndarray` / classes / callables leak into serialized configs.
4. **`Optional[X]` without default becomes required** (§5-5) → downstream
   entities/commands raise `missing` on previously valid inputs.
5. `field_info.extra` → `json_schema_extra`: if not migrated, `get_inputs()` /
   `get_outputs()` / `pipe_source` detection return empty → DAG edges vanish.
6. `show_field_alias_valerr` becomes a no-op (cosmetic).
7. v1 `.dict()` on a custom-root model returned `{"__root__": v}`; v2
   `RootModel.model_dump()` returns `v` (§5-12) → any caller indexing
   `["__root__"]` breaks (documented usage in docstrings).

### 4.2 Loud breakages (exceptions) — easy to detect, must still be handled

- `RootModel` + `extra=` config (`StageEntity`), `__root__` as property name
  in class body (§5-B), non-annotated attributes in dynamically built commands,
  pydantic dataclasses with custom `__init__`, `ValidationError.model`,
  `pydantic.v1.typing` imports, `cls.schema()` on models with arbitrary types.

### 4.3 Downstream-facing changes that are unavoidable

- Downstream code that **imports `pydantic.v1`** (`Field`, `validator`,
  `PrivateAttr`) and subclasses pipelime models will break, or worse, silently
  misbehave (a v1 `FieldInfo` used as default on a v2 model is just an opaque
  default value). Downstream must switch to `import pydantic` in the same
  release. This is inherent to the shim removal.
- `@validator` → `@field_validator` (v2 keeps `@validator` as a deprecated
  alias; `values` → `info.data`, `always=True` → `validate_default=True`).


---

## 5. Empirical facts about pydantic 2.12.5 (probes in the session scratchpad)

| # | Question | Result |
|---|---|---|
| 1 | Is a custom `__init__` called on nested validation / `model_validate`? | **Yes** (v2 `custom_init`) |
| 2 | `__get_validators__` on a `BaseModel` subclass? | **Silently ignored** — no warning, plain `model_type` error on compact input |
| 3 | Unknown class kwargs (`force_gc=True`) with config kwargs? | Config keys consumed, rest forwarded to `__init_subclass__` |
| 4 | `model_dump()` of a field typed as base class holding a subclass? | **`{}`**, no warning. `SerializeAsAny` / `serialize_as_any=True` fix it. A **class-level** `__get_pydantic_core_schema__` wrapping serialization with a runtime-type dispatch also fixes it transparently for every field annotated with that base (python & json modes, aliases, exclude flags, nested containers, JSON schema OK, no warnings) |
| 5 | `Optional[int]` without default | **Required**. A `ModelMetaclass` subclass that injects `= None` for `Optional` annotations without a default restores v1 semantics |
| 6 | pydantic dataclass `init=False` + custom `__init__(*nodes)` | **Breaks** (positional args validated against fields) |
| 7 | Setting `Model.__signature__` after class creation | Honoured by `inspect.signature` |
| 8 | `Union[Type[BaseModel], str, Mapping[str,int]]` smart mode | Picks the expected member for `"str"`, `{}` and a class |
| 9 | `Field(0, piper_port="input")` | Stored in `json_schema_extra`, **`PydanticDeprecatedSince20` warning** |
| 10 | `FieldInfo.alias` when unset; signature with `populate_by_name` | `None`; signature uses the alias (`i`) like v1 |
| 11 | `RootModel` with `extra="forbid"` (directly or via a base) | `PydanticUserError` |
| 12 | `RootModel` dump shape | top-level → bare value; nested → bare value |
| 13 | `validate_default=True` + `field_validator` reading `info.data` | Works like v1 `always=True` + `values` |
| 14 | `create_model(__config__=ConfigDict, __validators__={...})` with aliases | Works |
| 15 | Pickling `RootModel` subclasses and parametrized generics | Works |
| 16 | `__getattr__`/`__setattr__` overrides on a model (`LazyCommand`) | Works |
| 17 | `PrivateAttr(default=<object>)` | deep-copied per instance (same as v1) |
| 18 | `pydantic.color.Color` | Available, deprecated (warns) |
| 19 | `model_copy(update=)` | No validation (same as v1 `copy(update=)`) |
| 20 | `ValidationError` | No `.model` attribute; `.errors()` returns a fresh list each call |
| 21 | `Type[X]` and `Callable` fields | Work, dumped as-is |
| B | `RootModel` subclass with `__init__(root=..., /, **data)` accepting `__root__=`; `__root__` property attached *after* class creation; `model_validator(mode="wrap")` coercion + `model_serializer` | All work, incl. nested round-trip and instance pass-through identity. A `__root__` property **inside** the class body is rejected |
| C | Compact form via `model_validator(mode="wrap")` with instance pass-through and `extra="forbid"` | Works; `extra_forbidden` errors keep the right `loc` |
| E | `class TypeDef(RootModel[Type[T]], Generic[T], frozen=True)`; `wrapped_type()` via `model_fields["root"].annotation` | Works (`is_subclass_of` error on wrong type; frozen enforced; hashable) |
| F | `NewPath(Path)` with `__get_pydantic_core_schema__` inside `Union[DirectoryPath, NewPath]` | Works (returns `PosixPath`, as v1's `path_validator` did) |

---

## 6. Test suite facts

- **2406 tests** collected; `tests/pipelime/commands` = **1303** (heavily
  parametrized over `nproc × prefetch × lazy`, multiprocessing → slow).
- The **non-`commands` subset (1072 tests) runs in 84 s**, of which ~70 s are
  TUI (`test_base.py::test_resume_with_tui` 18 s, `test_tui.py` ~23 s), ZMQ
  (10 s) and grabber multiprocessing (~18 s). The pydantic-dense modules
  (`utils`, `stages`, `piper` (non-progress), `sequences` (non-grabber),
  `choixe`) run in **~15 s**.
- ~135 v1-specific API usages inside tests (29 files): mostly `.dict()`, a few
  test-local models using `pyd.validator`, `BaseConfig`/`Extra`,
  `create_model`, `__root__=` kwargs, `parse_obj_as`, `ValidationError` from
  `pydantic.v1`. `tests/sample_data/cli/{extra_commands,extra_operators,ckpt_dag}.py`
  are "downstream-like" modules importing `pydantic.v1`.
- **MRO lesson (verified):** pydantic `BaseModel` defines `__iter__`,
  `__eq__`, `__repr__`, `__copy__`, `__getattr__`, `__setattr__`…. Classes such as
  `SamplesSequence(SamplesSequenceBase, BaseModel)`, `DataStream(...)` and
  `LocalCheckpoint(Checkpoint, BaseModel)` put a non-pydantic base *before*
  `BaseModel` on purpose (e.g. so that `SamplesSequenceBase.__iter__` yields
  samples rather than `(name, value)` tuples). The MRO position of these mixins
  is load-bearing and must not be "tidied" to silence warnings.
