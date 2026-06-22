# Pydantic V1→V3 Migration + FAST Test Mode — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace every `pydantic.v1` usage in `pipelime/` with the native modern Pydantic API (Pydantic 2.x), and add a FAST test mode that keeps coverage high while slashing runtime.

**Architecture:** Two phases. Phase 1 adds a `--fast` pytest mode (used as the inner-loop gate for Phase 2). Phase 2 migrates all 37 files in three bottom-up layers (leaf utilities → core framework → consumers/TUI), applying a shared set of conversion recipes, with the test suite as the executable spec.

**Tech Stack:** Python 3.10–3.12, Pydantic 2.11.x (native API), pydantic-core, pytest, pytest-cov, pytest-asyncio.

## Global Constraints

- Keep the dependency pin `pydantic>=1.10.17,<3` — write only forward-compatible native-V2 code; do **not** bump to `pydantic>=3`. (Update the lower bound to `>=2.5` is allowed; the `<3` ceiling stays.)
- Adopt Pydantic V2 defaults where behavior differs; do **not** add shims to recreate V1 behavior. Record every user-facing change in `MIGRATION.md`.
- Python support floor: `>=3.10,<3.13` (unchanged).
- Zero `pydantic.v1` references in `pipelime/` at completion: `grep -rl "pydantic.v1" pipelime` must return nothing.
- Normal `pytest` behavior must be unchanged when `--fast` / `PIPELIME_TEST_FAST` are absent.
- Use `import pydantic as pyd` as the canonical alias (mirrors the existing `import pydantic.v1 as pyd` convention) so per-file diffs stay small.

---

## Conversion Recipes (reference — cited by Phase 2 tasks)

These are the canonical transformations. Tasks cite them by number; apply the cited recipe to the constructs the task lists.

### R1 — Model config knobs
V1 passes config as class keyword args, e.g. `class X(pyd.BaseModel, extra="forbid", copy_on_model_validation="none", allow_population_by_field_name=True)`. Keep the class-kwarg style (V2 supports it) but fix the keys:

| V1 key | V2 action |
|---|---|
| `extra="forbid"` / `"allow"` / `"ignore"` | keep as-is |
| `copy_on_model_validation="none"` | **remove** (no V2 equivalent; V2 does not copy on validation by default) |
| `allow_population_by_field_name=True` | rename → `populate_by_name=True` |
| `allow_mutation=False` | rename → `frozen=True` |
| `arbitrary_types_allowed=True` | keep as-is |
| `underscore_attrs_are_private=...` | **remove** (automatic in V2) |
| `smart_union=...` | **remove** (default in V2) |
| `pyd.Extra.ignore` / `pyd.Extra.forbid` | replace with string `"ignore"` / `"forbid"` |
| inner `class Config(pyd.BaseConfig): ...` | replace with `model_config = pyd.ConfigDict(...)` using the same renamed keys |

### R2 — `@validator` → `@field_validator`
```python
# V1
@pyd.validator("field_name")
def _v(cls, v):
    ...
    return v
# V2
@pyd.field_validator("field_name")
@classmethod
def _v(cls, v):
    ...
    return v
```
- `@pyd.validator("f", always=True)`: add `validate_default=True` to that field's `Field(...)` (so the validator runs on defaults), then convert as above. If the field has no `Field()`, wrap it: `f: T = pyd.Field(default, validate_default=True)`.
- Validator that reads sibling fields via `values`: change signature to `def _v(cls, v, info: pyd.ValidationInfo)` and read `info.data` instead of `values`.
- `@pyd.validator("f", pre=True)` → `@pyd.field_validator("f", mode="before")` (none currently, but apply if found).

### R3 — `@root_validator` → `@model_validator`
```python
# V1  @pyd.root_validator  def _v(cls, values): ... ; return values
# V2 (pre-validation, dict in):
@pyd.model_validator(mode="before")
@classmethod
def _v(cls, data): ...; return data
# V2 (post-validation, model in) when it inspected fully-built fields:
@pyd.model_validator(mode="after")
def _v(self): ...; return self
```
Pick `before` if the V1 validator mutated/validated the raw input mapping; pick `after` if it cross-checked already-parsed fields.

### R4 — Method/attribute renames (mechanical)
| V1 | V2 |
|---|---|
| `m.dict(...)` | `m.model_dump(...)` |
| `m.json(...)` | `m.model_dump_json(...)` |
| `Model.parse_obj(x)` | `Model.model_validate(x)` |
| `Model.parse_raw(s)` | `Model.model_validate_json(s)` |
| `Model.parse_file(p)` | `Model.model_validate_json(Path(p).read_text())` |
| `pyd.parse_obj_as(T, x)` | `pyd.TypeAdapter(T).validate_python(x)` |
| `pyd.parse_raw_as(T, s)` | `pyd.TypeAdapter(T).validate_json(s)` |
| `m.copy(...)` | `m.model_copy(...)` |
| `Model.__fields__` | `Model.model_fields` |
| `Model.construct(...)` | `Model.model_construct(...)` |
| `m.schema()` | `m.model_json_schema()` |
- `model_dump`/`model_dump_json` keep the same `by_alias=`, `exclude_none=`, etc. kwargs.

### R5 — `__root__` model → `RootModel`
```python
# V1
class X(pyd.BaseModel, extra="forbid", copy_on_model_validation="none"):
    __root__: SomeType
    @classmethod
    def __get_validators__(cls): yield cls.validate
    @classmethod
    def validate(cls, value): ...; return cls(__root__=...)
    def _iter(self, *a, **k):
        for key, v in super()._iter(*a, **k): yield key, <serialized v>
# V2
class X(pyd.RootModel[SomeType]):
    model_config = pyd.ConfigDict(...)   # only if non-default knobs needed
    @pyd.model_validator(mode="before")
    @classmethod
    def _coerce(cls, value): ...; return <raw SomeType value>   # return the inner value, NOT cls(...)
    @pyd.model_serializer
    def _ser(self): return <serialized self.root>
    @property
    def value(self): return self.root
```
- Replace all `self.__root__` with `self.root`; `cls(__root__=v)` with `cls(v)` (RootModel takes the root positionally) — but inside a `mode="before"` validator just **return the inner value** and let RootModel wrap it.
- Remove `__get_validators__` (a `RootModel`/`BaseModel` subclass is automatically usable as a field type).
- `cls.__fields__["__root__"].outer_type_` → `cls.model_fields["root"].annotation`.
- Serialization no longer nests under `__root__`: `x.model_dump()` returns the serialized root value directly. Update any caller that did `x.dict()["__root__"]` to just `x.model_dump()`. **Document this in MIGRATION.md.**

### R6 — `GenericModel` → generic `BaseModel`/`RootModel`
```python
# V1
import pydantic.v1.generics as pydg
class X(pydg.GenericModel, t.Generic[T], ...): __root__: t.Type[T]
# V2
class X(pyd.RootModel[t.Type[T]], t.Generic[T]): ...
# (for non-root generics: class X(pyd.BaseModel, t.Generic[T]): ...)
```
Remove the `pydantic.v1.generics` import.

### R7 — Custom scalar type (`__get_validators__` / `__modify_schema__`) → core schema
For types that are **not** models (e.g. `NewPath(Path)`):
```python
from pydantic import GetCoreSchemaHandler, GetJsonSchemaHandler
from pydantic_core import core_schema

class NewPath(Path):
    extension: t.Optional[str] = None

    @classmethod
    def __get_pydantic_core_schema__(cls, source, handler: GetCoreSchemaHandler):
        return core_schema.no_info_after_validator_function(
            cls._validate, handler(Path)
        )

    @classmethod
    def __get_pydantic_json_schema__(cls, schema, handler: GetJsonSchemaHandler):
        js = handler(schema)
        js.update(exists=False)
        if cls.extension is not None:
            js.update(extension=cls.extension)
        return js

    @classmethod
    def _validate(cls, value: Path) -> Path:
        ...  # former body of validate()
```

### R8 — Dynamic `create_model`
```python
# V1
class Config(pyd.BaseConfig): arbitrary_types_allowed = True; extra = pyd.Extra.ignore
pyd.create_model("M", __config__=Config, __validators__={...}, **fields)
# V2
pyd.create_model(
    "M",
    __config__=pyd.ConfigDict(arbitrary_types_allowed=True, extra="ignore"),
    __validators__={name: pyd.field_validator(fname)(classmethod(fn))},
    **fields,
)
```
- In V2, `__validators__` values must be the result of `field_validator(...)` applied to a **classmethod**.
- `FieldInfo, Undefined` from `pydantic.v1.fields` → `from pydantic.fields import FieldInfo` and `from pydantic_core import PydanticUndefined as Undefined`.

### R9 — `ModelField` introspection → `FieldInfo`
`pydantic.v1.fields.ModelField` is gone. `model.model_fields` is `dict[str, FieldInfo]`.

| V1 `field: ModelField` | V2 |
|---|---|
| `field.name` | not on `FieldInfo` — pass the dict **key** alongside |
| `field.alias` | `field.alias` (may be `None`) |
| `field.field_info.description` | `field.description` |
| `field.get_default()` | `field.get_default(call_default_factory=True)` |
| `field.outer_type_` / `field.type_` | `field.annotation` |
| `field.required` | `field.is_required()` |
Functions taking a bare `ModelField` must be changed to also receive the field **name** (the `model_fields` key), since `FieldInfo` has no `.name`.

---

# PHASE 1 — FAST test mode

### Task 1: `--fast` flag, env var, and `slow` marker plumbing

**Files:**
- Modify: `tests/conftest.py` (top-level, add hooks)

**Interfaces:**
- Produces: pytest option `--fast`; marker `slow`; the predicate is read elsewhere via `request.config.getoption("--fast")`.

- [ ] **Step 1: Write the failing test**

Create `tests/pipelime/test_fast_mode.py`:
```python
def test_fast_option_registered(pytestconfig):
    # --fast defaults to False unless passed or PIPELIME_TEST_FAST is set
    assert pytestconfig.getoption("--fast") in (True, False)


def test_slow_marker_registered(pytestconfig):
    markers = pytestconfig.getini("markers")
    assert any(m.startswith("slow") for m in markers)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/pipelime/test_fast_mode.py -v`
Expected: FAIL — `ValueError: no option named '--fast'`.

- [ ] **Step 3: Implement the plumbing in `tests/conftest.py`**

Add at the top of `tests/conftest.py` (after imports):
```python
def pytest_addoption(parser):
    parser.addoption(
        "--fast",
        action="store_true",
        default=bool(os.environ.get("PIPELIME_TEST_FAST")),
        help="Run a fast, high-coverage subset: collapse parametrization and "
        "deselect tests marked 'slow'.",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "slow: heavy test, deselected when --fast is given"
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--fast"):
        return
    skip_slow = pytest.mark.skip(reason="deselected in --fast mode")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)
```
(`os` and `pytest` are already imported in `conftest.py`.)

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/pipelime/test_fast_mode.py -v`
Expected: PASS (both tests).

- [ ] **Step 5: Verify normal mode still collects everything**

Run: `pytest --collect-only -q 2>/dev/null | tail -1`
Expected: still ~2406 tests collected (no `--fast`).
Run: `pytest --fast --collect-only -q 2>/dev/null | tail -1`
Expected: same collection count (deselection happens at run-time via skip, not collection) — confirms the hook loads without error.

- [ ] **Step 6: Commit**

```bash
git add tests/conftest.py tests/pipelime/test_fast_mode.py
git commit -m "test: add --fast flag, PIPELIME_TEST_FAST env var, and slow marker"
```

---

### Task 2: `fast_params` helper to collapse parametrization

**Files:**
- Create: `tests/_fast.py`
- Test: `tests/pipelime/test_fast_mode.py` (extend)

**Interfaces:**
- Produces: `fast_params(full: list, *, fast: list | None = None) -> list` and `FAST = bool(os.environ.get("PIPELIME_TEST_FAST"))`. When `FAST`, returns `fast` if given else `full[:1]`; otherwise returns `full`. Used directly inside `@pytest.mark.parametrize(...)` decorators (which are evaluated at import time, so they read the env var, not the pytest option).

- [ ] **Step 1: Write the failing test**

Append to `tests/pipelime/test_fast_mode.py`:
```python
import importlib


def test_fast_params_collapses(monkeypatch):
    monkeypatch.setenv("PIPELIME_TEST_FAST", "1")
    import tests._fast as f
    importlib.reload(f)
    assert f.fast_params([0, 2]) == [0]
    assert f.fast_params([2, 4], fast=[2]) == [2]


def test_fast_params_full_when_not_fast(monkeypatch):
    monkeypatch.delenv("PIPELIME_TEST_FAST", raising=False)
    import tests._fast as f
    importlib.reload(f)
    assert f.fast_params([0, 2]) == [0, 2]
    assert f.fast_params([2, 4], fast=[2]) == [2, 4]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/pipelime/test_fast_mode.py -k fast_params -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'tests._fast'`.

- [ ] **Step 3: Implement `tests/_fast.py`**

```python
import os

FAST = bool(os.environ.get("PIPELIME_TEST_FAST"))


def fast_params(full, *, fast=None):
    """Return the parametrize values to use.

    In FAST mode (PIPELIME_TEST_FAST set), return `fast` if provided,
    else the first element of `full`. Otherwise return `full` unchanged.
    """
    if not FAST:
        return list(full)
    if fast is not None:
        return list(fast)
    return list(full)[:1]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/pipelime/test_fast_mode.py -k fast_params -v`
Expected: PASS (both).

- [ ] **Step 5: Commit**

```bash
git add tests/_fast.py tests/pipelime/test_fast_mode.py
git commit -m "test: add fast_params helper to collapse parametrization in fast mode"
```

---

### Task 3: Apply `fast_params` to the heaviest parametrized suites

**Files:**
- Modify (each `@pytest.mark.parametrize` whose values are `nproc`, `prefetch`, or `lazy`): primarily `tests/pipelime/sequences/test_grabber.py`, `tests/pipelime/sequences/test_operations.py`, `tests/pipelime/sequences/test_samples_sequences.py`, `tests/pipelime/commands/*.py`, `tests/pipelime/sequences/test_validation.py`. Find them with:
  `grep -rln 'parametrize("nproc"\|parametrize("prefetch"\|parametrize("lazy"' tests`

**Interfaces:**
- Consumes: `fast_params` from `tests/_fast.py` (Task 2).

- [ ] **Step 1: Locate every target parametrize**

Run: `grep -rn 'parametrize("nproc"\|parametrize("prefetch"\|parametrize("lazy"' tests`
Expected: a list of ~30+ decorator sites.

- [ ] **Step 2: Convert each site**

For every match, wrap the value list in `fast_params(...)` and add the import. Representative fast picks:
```python
# before
@pytest.mark.parametrize("nproc", [0, 2])
@pytest.mark.parametrize("prefetch", [2, 4])
@pytest.mark.parametrize("lazy", [True, False])
# after  (add: from tests._fast import fast_params)
@pytest.mark.parametrize("nproc", fast_params([0, 2], fast=[2]))     # keep real multiprocessing path
@pytest.mark.parametrize("prefetch", fast_params([2, 4]))            # -> [2]
@pytest.mark.parametrize("lazy", fast_params([True, False]))        # -> [True]
```
Rationale for `nproc` fast pick = `[2]`: exercises the multiprocessing code path (the riskier one) rather than the trivial serial path. Other axes collapse to their first value.

- [ ] **Step 3: Verify fast mode shrinks the run**

Run: `PIPELIME_TEST_FAST=1 pytest tests/pipelime/sequences/test_grabber.py --collect-only -q 2>/dev/null | tail -1`
Expected: substantially fewer tests than the same command without `PIPELIME_TEST_FAST=1`.

- [ ] **Step 4: Verify normal mode is unchanged**

Run: `pytest tests/pipelime/sequences/test_grabber.py --collect-only -q 2>/dev/null | tail -1`
Expected: identical count to pre-change (re-run on `HEAD~1` to confirm if unsure).

- [ ] **Step 5: Run both modes green**

Run: `pytest tests/pipelime/sequences -q` then `PIPELIME_TEST_FAST=1 pytest tests/pipelime/sequences -q`
Expected: both PASS.

- [ ] **Step 6: Commit**

```bash
git add tests/
git commit -m "test: collapse nproc/prefetch/lazy parametrization under fast mode"
```

---

### Task 4: Tag genuinely heavy tests `slow` and baseline coverage delta

**Files:**
- Modify: individual slow test functions (identified by timing).
- Create: `tests/README.md`

**Interfaces:**
- Consumes: `slow` marker (Task 1).

- [ ] **Step 1: Identify the slowest tests**

Run: `pytest --durations=25 -q 2>/dev/null | sed -n '/slowest durations/,$p'`
Expected: a ranked list. Candidates: ZMQ receiver tests, minio, large-dataset writers/grabber, DAG end-to-end.

- [ ] **Step 2: Tag the top offenders**

Add `@pytest.mark.slow` above each test (or `pytestmark = pytest.mark.slow` for a whole heavy module) for tests that are (a) slow **and** (b) redundant with cheaper coverage of the same code path. Do **not** tag a test if it is the only cover for its code path.

- [ ] **Step 3: Baseline coverage in both modes**

Run: `pytest --cov=pipelime --cov-report=term-missing -q 2>/dev/null | tail -3`
Run: `PIPELIME_TEST_FAST=1 pytest --fast --cov=pipelime --cov-report=term-missing -q 2>/dev/null | tail -3`
Expected: fast-mode total coverage within ~1–2% of full; record both numbers in `tests/README.md`.

- [ ] **Step 4: Write `tests/README.md`**

```markdown
# Running the tests

Full suite (default):

    pytest

Fast suite — high coverage, low runtime (collapses nproc/prefetch/lazy
matrices and deselects @pytest.mark.slow tests):

    pytest --fast
    # or
    PIPELIME_TEST_FAST=1 pytest

Coverage baseline (update when adding large suites):
- full:  <FILL FROM STEP 3>% in <FILL>s
- fast:  <FILL FROM STEP 3>% in <FILL>s
```
Replace the `<FILL>` placeholders with the real numbers measured in Step 3.

- [ ] **Step 5: Commit**

```bash
git add tests/
git commit -m "test: mark heavy tests slow; document fast mode + coverage baseline"
```

---

# PHASE 2 — Pydantic native-API migration

> Each task ends by running its module's tests **and** the fast suite. The existing tests are the spec. Where a test asserts V1-specific behavior that legitimately changed under V2 (error text, JSON-schema shape, `__root__` envelope), update the test to the V2 output and add a bullet to `MIGRATION.md` (Task 20 collects them; add as you go).

## Layer 1 — leaf utilities

### Task 5: Migrate `pipelime/choixe/ast/nodes.py` and `choixe/visitors/decoder.py`

**Files:**
- Modify: `pipelime/choixe/ast/nodes.py`, `pipelime/choixe/visitors/decoder.py`
- Test: `tests/pipelime/choixe/`

**Interfaces:**
- Produces: same public classes/functions, now on native Pydantic.

- [ ] **Step 1: Inventory the V1 constructs**

Run: `grep -nE "pydantic\.v1|\.dict\(|\.json\(|parse_obj|parse_raw|__fields__|validator|__root__|GenericModel|class Config|Extra\." pipelime/choixe/ast/nodes.py pipelime/choixe/visitors/decoder.py`
Expected: a concrete list to convert.

- [ ] **Step 2: Apply recipes**

Change `import pydantic.v1 as pyd` → `import pydantic as pyd` (and `from pydantic.v1 import X` → `from pydantic import X`). Apply **R1** (config), **R2/R3** (validators), **R4** (method renames), **R5** (`__root__`), **R6** (`GenericModel`) for each construct found in Step 1.

- [ ] **Step 3: Run the module tests**

Run: `pytest tests/pipelime/choixe -q`
Expected: PASS. If a test asserts V1-only behavior, update it to V2 output and note it for `MIGRATION.md`.

- [ ] **Step 4: Confirm no v1 imports remain in these files**

Run: `grep -n "pydantic.v1" pipelime/choixe/ast/nodes.py pipelime/choixe/visitors/decoder.py`
Expected: no output.

- [ ] **Step 5: Fast suite gate**

Run: `pytest --fast -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pipelime/choixe tests/
git commit -m "refactor: migrate choixe nodes/decoder to native pydantic"
```

---

### Task 6: Migrate custom scalar type `NewPath` in `pipelime/utils/pydantic_types.py`

**Files:**
- Modify: `pipelime/utils/pydantic_types.py:19-64` (`NewPath`, `new_file_path`)
- Test: `tests/pipelime/utils/test_pydantic_types.py`

**Interfaces:**
- Produces: `NewPath` usable as a field annotation; `new_file_path(ext)` factory unchanged.

- [ ] **Step 1: Run current NewPath tests to know the contract**

Run: `pytest tests/pipelime/utils/test_pydantic_types.py -k "path or Path" -v`
Expected: PASS on V1 (baseline behavior to preserve where sensible).

- [ ] **Step 2: Apply recipe R7**

Replace `__get_validators__` + `__modify_schema__` with `__get_pydantic_core_schema__` + `__get_pydantic_json_schema__` per **R7**, moving the body of `validate` into `_validate`. Keep `new_file_path` and the dynamic subclass creation as-is.

- [ ] **Step 3: Run NewPath tests**

Run: `pytest tests/pipelime/utils/test_pydantic_types.py -k "path or Path" -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add pipelime/utils/pydantic_types.py
git commit -m "refactor: port NewPath to pydantic v2 core-schema API"
```

---

### Task 7: Migrate root-model custom types in `pipelime/utils/pydantic_types.py`

**Files:**
- Modify: `pipelime/utils/pydantic_types.py` — `NumpyType` (67-165), `YamlInput` (179-280), `TypeDef`/`ItemType` (286-425), `CallableDef` (428-620)
- Test: `tests/pipelime/utils/test_pydantic_types.py`

**Interfaces:**
- Produces: each type keeps `.create(...)`, `.value`, `__call__`/`__str__` where present; serialization now returns the root value directly (no `__root__` key).

- [ ] **Step 1: Baseline**

Run: `pytest tests/pipelime/utils/test_pydantic_types.py -q`
Expected: PASS on V1.

- [ ] **Step 2: Convert each type with R5/R6**

For `NumpyType`, `YamlInput`, `CallableDef`: `class X(pyd.RootModel[<inner>])` per **R5** — move `validate` into a `@pyd.model_validator(mode="before")` returning the **inner** value, move `_iter` serialization into `@pyd.model_serializer`, replace `self.__root__`→`self.root`, drop `__get_validators__`. For `TypeDef` (generic) use **R6**: `class TypeDef(pyd.RootModel[t.Type[TRoot]], t.Generic[TRoot])`; fix `wrapped_type()` to `t.get_args(cls.model_fields["root"].annotation)[0]`. `create()` stays `return cls.model_validate(value)` (replace `cls.validate(value)`). Keep `arbitrary_types_allowed=True` in `model_config` for `NumpyType`.

- [ ] **Step 3: Update internal callers of the `__root__` envelope**

Run: `grep -rn '\["__root__"\]\|__root__=' pipelime`
For each remaining hit, replace `x.dict()["__root__"]` → `x.model_dump()` and `cls(__root__=v)` → `cls(v)`. (`piper/model.py` is handled in Task 11; only fix `pydantic_types.py` here.)

- [ ] **Step 4: Run tests**

Run: `pytest tests/pipelime/utils/test_pydantic_types.py -q`
Expected: PASS. Update any test asserting the `{"__root__": ...}` shape to the unwrapped value; note for `MIGRATION.md`.

- [ ] **Step 5: Commit**

```bash
git add pipelime/utils/pydantic_types.py tests/
git commit -m "refactor: port NumpyType/YamlInput/TypeDef/CallableDef to RootModel"
```

---

### Task 8: Migrate dynamic schema builder + remaining models in `pydantic_types.py`

**Files:**
- Modify: `pipelime/utils/pydantic_types.py` — `ItemValidationModel` (628-697), `SampleValidationInterface` (700-789)
- Test: `tests/pipelime/utils/test_pydantic_types.py`, `tests/pipelime/sequences/test_validation.py`, `tests/pipelime/commands/test_validate.py`

**Interfaces:**
- Produces: `SampleValidationInterface.schema_model`, `.as_pipe()`, `.append_validator()`; `ItemValidationModel.make_field()`, `.make_validator_method()` returning a V2 `field_validator`.

- [ ] **Step 1: Baseline**

Run: `pytest tests/pipelime/utils/test_pydantic_types.py tests/pipelime/sequences/test_validation.py tests/pipelime/commands/test_validate.py -q`
Expected: PASS on V1.

- [ ] **Step 2: Apply R1 + R8**

- `class Config(pyd.BaseConfig)` inside `_make_schema` → `pyd.ConfigDict(arbitrary_types_allowed=True, extra="ignore" if self.ignore_extra_keys else "forbid")` and pass as `__config__=` per **R8** (drop the `pyd.Extra` reference).
- `make_validator_method`: the generated function returns `pyd.validator(field_name)(fn_helper)` → `pyd.field_validator(field_name)(classmethod(fn_helper))` per **R8**.
- `as_pipe`: `self.dict(by_alias=True)` → `self.model_dump(by_alias=True)` per **R4**.
- Apply **R1** to the two model class declarations.
- `_validator_callable = pyd.PrivateAttr()` stays (PrivateAttr exists in V2).
- The `__init__` override calling `super().__init__(**data)` then setting a private attr is valid in V2; keep it.

- [ ] **Step 3: Run tests**

Run: `pytest tests/pipelime/utils/test_pydantic_types.py tests/pipelime/sequences/test_validation.py tests/pipelime/commands/test_validate.py -q`
Expected: PASS.

- [ ] **Step 4: Confirm file is v1-free**

Run: `grep -n "pydantic.v1" pipelime/utils/pydantic_types.py`
Expected: no output.

- [ ] **Step 5: Fast suite gate + commit**

Run: `pytest --fast -q` → PASS.
```bash
git add pipelime/utils/pydantic_types.py tests/
git commit -m "refactor: port pydantic_types dynamic schema builder to v2"
```

---

## Layer 2 — core framework

### Task 9: Migrate `pipelime/stages/base.py` and `stages/entities.py`

**Files:**
- Modify: `pipelime/stages/base.py`, `pipelime/stages/entities.py`
- Test: `tests/pipelime/stages/test_base_stages.py`, `tests/pipelime/stages/test_entities.py`

**Interfaces:**
- Produces: `SampleStage`, `StageInput`, entity base classes — public API unchanged.

- [ ] **Step 1: Inventory + baseline**

Run: `grep -nE "pydantic\.v1|__root__|GenericModel|class Config|@pyd?\.?validator|root_validator|\.dict\(|parse_obj|__fields__|Extra\.|copy_on_model_validation|allow_population_by_field_name|allow_mutation" pipelime/stages/base.py pipelime/stages/entities.py`
Run: `pytest tests/pipelime/stages/test_base_stages.py tests/pipelime/stages/test_entities.py -q` (baseline PASS).

- [ ] **Step 2: Apply recipes**

Apply **R1** (the `extra=...,copy_on_model_validation=...` class kwargs and the inner `class Config(pyd.BaseConfig)` at `entities.py:184`), **R2** (the `@pyd.validator("action")`, `@pyd.validator("input_type", always=True)` — add `validate_default=True` to `input_type`'s Field), **R5** (any `__root__`), **R4** (method renames). Change the import line.

- [ ] **Step 3: Run tests**

Run: `pytest tests/pipelime/stages/test_base_stages.py tests/pipelime/stages/test_entities.py -q`
Expected: PASS (adjust V1-only assertions; note for MIGRATION.md).

- [ ] **Step 4: Commit**

```bash
git add pipelime/stages/base.py pipelime/stages/entities.py tests/
git commit -m "refactor: migrate stages base/entities to native pydantic"
```

---

### Task 10: Migrate remaining `pipelime/stages/*` and `pipelime/sequences/*`

**Files:**
- Modify: `stages/key_transformations.py`, `stages/item_replacement.py`, `stages/item_sources.py`, `stages/item_info.py`, `stages/augmentations.py`; `sequences/utils.py`, `sequences/__init__.py`, `sequences/grabber.py`, `sequences/samples_sequence.py`, `sequences/pipes/{writers,mapping,operations,base,validation}.py`, `sequences/sources/{toy_dataset,readers,raw,from_callable}.py`
- Test: `tests/pipelime/stages/`, `tests/pipelime/sequences/`

**Interfaces:**
- Consumes: migrated `stages/base.py`, `pydantic_types.py`.

- [ ] **Step 1: Inventory + baseline**

Run: `grep -rnE "pydantic\.v1|__root__|@pyd?\.?validator|root_validator|\.dict\(|\.json\(|parse_obj|parse_raw|__fields__|Extra\.|copy_on_model_validation|allow_population_by_field_name" pipelime/stages pipelime/sequences`
Run: `pytest tests/pipelime/stages tests/pipelime/sequences -q` (baseline).

- [ ] **Step 2: Apply recipes per file**

Work file-by-file. Notable sites (from the inventory): `@pyd.validator(...)` in `key_transformations.py:47`, `item_replacement.py:68`, `item_replacement.py:130` (`@pyd.root_validator` → **R3**), `augmentations.py:83`; `@validator("must_exist", always=True)` in `sources/readers.py` (×3) and `@pyd.validator("exists_ok", always=True)` in `pipes/writers.py:51`, `sources/toy_dataset.py:57`, `pipes/operations.py:29` → **R2** with `validate_default=True`. `augmentations.py`/`base.py` `__root__` → **R5**. All `import pydantic.v1` → `import pydantic`.

- [ ] **Step 3: Run tests in waves**

Run: `pytest tests/pipelime/stages -q` then `pytest tests/pipelime/sequences -q`
Expected: PASS. Adjust V1-only assertions; note for MIGRATION.md.

- [ ] **Step 4: Commit**

```bash
git add pipelime/stages pipelime/sequences tests/
git commit -m "refactor: migrate remaining stages and sequences to native pydantic"
```

---

### Task 11: Migrate the dynamic command framework `pipelime/piper/model.py`

**Files:**
- Modify: `pipelime/piper/model.py`
- Test: `tests/pipelime/piper/test_command_decorator.py`, `tests/pipelime/piper/test_commands.py`, `tests/pipelime/piper/test_dag.py`, `tests/pipelime/piper/test_dag_parsers.py`

**Interfaces:**
- Produces: `PipelimeCommand`, the `@command` decorator and `_FnModel` dynamic class, `DAGModel` (`__root__: Mapping[str, PipelimeCommand]`).

- [ ] **Step 1: Baseline**

Run: `pytest tests/pipelime/piper/test_command_decorator.py tests/pipelime/piper/test_commands.py tests/pipelime/piper/test_dag.py tests/pipelime/piper/test_dag_parsers.py -q`
Expected: PASS on V1.

- [ ] **Step 2: Convert imports and field construction**

- `from pydantic.v1.fields import FieldInfo, Undefined` → `from pydantic.fields import FieldInfo` + `from pydantic_core import PydanticUndefined as Undefined` (**R8**).
- `import pydantic.v1 as pyd` / `from pydantic.v1 import ...` → native.
- Config class kwargs on `PiperInfo` (327) and the `__config_kwargs` plumbing → **R1** (rename `copy_on_model_validation`/`allow_population_by_field_name`).

- [ ] **Step 3: Convert the `DAGModel` root model**

`__root__: t.Mapping[str, PipelimeCommand]` (≈line 507) → **R5**: `class DAGModel(pyd.RootModel[t.Mapping[str, PipelimeCommand]])`; `cls(__root__=plnodes)` → `cls(plnodes)`; `self.__root__` → `self.root`; the `assert k == "__root__"` serialization loop → `@pyd.model_serializer`.

- [ ] **Step 4: Convert the `GenericModel` command wrapper**

The `class ...(GenericModel, t.Generic[CmdTp], ...)` at ≈line 474 → **R6**.

- [ ] **Step 5: Verify dynamic command creation still works**

Run: `pytest tests/pipelime/piper/test_command_decorator.py -q`
Expected: PASS — this exercises `_make_field`/`_FnModel`. If `create_model`/field defaults misbehave, confirm `Undefined` maps to `PydanticUndefined` and `Field(...)` defaults are passed as `(annotation, FieldInfo)` tuples.

- [ ] **Step 6: Run the rest + commit**

Run: `pytest tests/pipelime/piper -q`
Expected: PASS.
```bash
git add pipelime/piper/model.py tests/
git commit -m "refactor: migrate piper command framework + DAGModel to native pydantic"
```

---

### Task 12: Migrate `pipelime/piper/checkpoint.py` and `piper/progress/model.py`

**Files:**
- Modify: `pipelime/piper/checkpoint.py`, `pipelime/piper/progress/model.py`
- Test: `tests/pipelime/piper/test_checkpoint.py`, `tests/pipelime/piper/progress/`

**Interfaces:**
- Consumes: migrated `piper/model.py`.

- [ ] **Step 1: Inventory + baseline**

Run: `grep -nE "pydantic\.v1|@pyd?\.?validator|root_validator|\.dict\(|parse_obj|parse_raw|__root__|class Config" pipelime/piper/checkpoint.py pipelime/piper/progress/model.py`
Run: `pytest tests/pipelime/piper/test_checkpoint.py tests/pipelime/piper/progress -q` (baseline).

- [ ] **Step 2: Apply recipes**

`@pyd.validator("folder")` at `checkpoint.py:133` → **R2**. Method renames **R4**. Import line. Config knobs **R1**.

- [ ] **Step 3: Run tests + commit**

Run: `pytest tests/pipelime/piper/test_checkpoint.py tests/pipelime/piper/progress -q` → PASS.
```bash
git add pipelime/piper/checkpoint.py pipelime/piper/progress/model.py tests/
git commit -m "refactor: migrate piper checkpoint + progress model to native pydantic"
```

---

### Task 13: Layer-2 gate

- [ ] **Step 1: Confirm Layer-2 files are v1-free**

Run: `grep -rl "pydantic.v1" pipelime/stages pipelime/sequences pipelime/piper`
Expected: no output.

- [ ] **Step 2: Fast suite**

Run: `pytest --fast -q`
Expected: PASS.

- [ ] **Step 3: Commit (if any test fixups)**

```bash
git add -A && git commit -m "test: layer-2 migration fixups" || echo "nothing to commit"
```

---

## Layer 3 — consumers & introspection

### Task 14: Migrate `pipelime/commands/*`

**Files:**
- Modify: `commands/interfaces.py`, `commands/general.py`, `commands/piper.py`, `commands/split_ops.py`, `commands/shell.py`, `commands/tempman.py`, `commands/toy_dataset.py`, `commands/resume.py`
- Test: `tests/pipelime/commands/`

**Interfaces:**
- Consumes: migrated framework (Layer 2).

- [ ] **Step 1: Inventory + baseline**

Run: `grep -rnE "pydantic\.v1|@pyd?\.?validator|root_validator|\.dict\(|\.json\(|parse_obj|parse_raw|__fields__|__root__|class Config|Extra\.|copy_on_model_validation|allow_population_by_field_name" pipelime/commands`
Run: `pytest tests/pipelime/commands -q` (baseline).

- [ ] **Step 2: Apply recipes**

Many `@pyd.validator(..., always=True)` across `interfaces.py` (folder/pipe/exists_ok/key_format/file) → **R2** with `validate_default=True`. `@validator("filter_fn", always=True)` (`general.py:810`), `@pyd.validator("operations")` (`general.py:312`), `@validator("folder_debug", always=True)` (`piper.py:505`), `@validator("ckpt", always=True)` (`resume.py:29`) → **R2**. `__root__` in `general.py` → **R5**. Method renames **R4**. Imports + **R1** throughout.

- [ ] **Step 3: Run tests in waves + commit**

Run: `pytest tests/pipelime/commands -q`
Expected: PASS (adjust V1-only assertions; note for MIGRATION.md).
```bash
git add pipelime/commands tests/
git commit -m "refactor: migrate commands to native pydantic"
```

---

### Task 15: Migrate TUI field introspection `pipelime/cli/tui/utils.py` + `tui.py`

**Files:**
- Modify: `pipelime/cli/tui/utils.py`, `pipelime/cli/tui/tui.py`
- Test: `tests/pipelime/cli/test_tui.py`

**Interfaces:**
- Produces: `init_tui_field(name, field, args)`, `init_stageinput_tui_field(name, field, cmd_args)`, `get_field_type(field)` — **note the added `name` parameter** (FieldInfo has no `.name`).

- [ ] **Step 1: Baseline**

Run: `pytest tests/pipelime/cli/test_tui.py -q`
Expected: PASS on V1.

- [ ] **Step 2: Apply recipe R9**

- `from pydantic.v1.fields import ModelField` → `from pydantic.fields import FieldInfo`.
- Change signatures to receive the field **name** alongside the `FieldInfo`: `init_tui_field(field, args)` → `init_tui_field(name, field, args)` (and the stageinput variant). Update call sites in `tui.py` to iterate `model_fields.items()` and pass `(name, field_info)`.
- Inside: `field.name` → `name`; `field.alias` → `field.alias or name`; `field.field_info.description` → `field.description`; `field.get_default()` → `field.get_default(call_default_factory=True)`; in `get_field_type`, `field.outer_type_`/`field.type_` → `field.annotation`.
- `field.model_config.allow_population_by_field_name` references (pretty_print/utils handled in Task 16) — here update any `allow_population_by_field_name` → `populate_by_name`.

- [ ] **Step 3: Run tests**

Run: `pytest tests/pipelime/cli/test_tui.py -q`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add pipelime/cli/tui tests/
git commit -m "refactor: port TUI field introspection to pydantic v2 FieldInfo"
```

---

### Task 16: Migrate `pipelime/cli/*` (main, utils, pretty_print)

**Files:**
- Modify: `pipelime/cli/main.py`, `pipelime/cli/utils.py`, `pipelime/cli/pretty_print.py`
- Test: `tests/pipelime/cli/test_base.py`

**Interfaces:**
- Consumes: migrated framework + TUI.

- [ ] **Step 1: Inventory + baseline**

Run: `grep -nE "pydantic\.v1|\.dict\(|\.json\(|parse_obj|parse_raw|__fields__|field_info|outer_type_|allow_population_by_field_name|model_config\." pipelime/cli/main.py pipelime/cli/utils.py pipelime/cli/pretty_print.py`
Run: `pytest tests/pipelime/cli/test_base.py -q` (baseline).

- [ ] **Step 2: Apply recipes**

`field.model_config.allow_population_by_field_name` (`pretty_print.py:274`, `utils.py:769`) → `field.model_config["populate_by_name"]` (note: in V2 `model_config` is a `dict`, access by key; and `populate_by_name` is the renamed key). `field.has_alias` → `field.alias is not None`. `__fields__` → `model_fields` (**R4/R9**). Method renames **R4**. Imports.

- [ ] **Step 3: Run tests + commit**

Run: `pytest tests/pipelime/cli/test_base.py -q` → PASS.
```bash
git add pipelime/cli/main.py pipelime/cli/utils.py pipelime/cli/pretty_print.py tests/
git commit -m "refactor: migrate cli main/utils/pretty_print to native pydantic"
```

---

### Task 17: Sweep any stragglers

**Files:**
- Modify: whatever `grep` still finds.

- [ ] **Step 1: Find all remaining v1 references**

Run: `grep -rn "pydantic.v1" pipelime`
Expected: ideally empty. If not, list the files.

- [ ] **Step 2: Convert each with the matching recipe**

Apply R1–R9 as appropriate to each remaining hit (e.g. `cli/tui/utils.py` leftovers, `sequences/__init__.py` re-exports).

- [ ] **Step 3: Find remaining V1-only API calls**

Run: `grep -rnE "\.dict\(|\.json\(|parse_obj|parse_raw|parse_file|__fields__|\.construct\(|\.schema\(|update_forward_refs|GenericModel|ModelField|Extra\.|copy_on_model_validation|allow_population_by_field_name|allow_mutation|underscore_attrs_are_private" pipelime`
Expected: empty (excluding unrelated `.dict(`/`.json(` on non-pydantic objects — verify each before changing). Convert true positives with **R4**.

- [ ] **Step 4: Full suite**

Run: `pytest -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "refactor: sweep remaining pydantic v1 usages" || echo "nothing to commit"
```

---

### Task 18: Update dependency floor and import-time sanity

**Files:**
- Modify: `pyproject.toml` (dependencies)

- [ ] **Step 1: Raise the lower bound, keep the ceiling**

In `pyproject.toml`, change `"pydantic>=1.10.17,<3"` → `"pydantic>=2.5,<3"`. Remove the now-stale `# this enables import pydantic.v1` comment.

- [ ] **Step 2: Import-time check**

Run: `python -c "import pipelime.cli.main, pipelime.commands.general, pipelime.utils.pydantic_types; print('ok')"`
Expected: `ok` (no import errors, no deprecation warnings about `pydantic.v1`).

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "build: require pydantic>=2.5,<3"
```

---

### Task 19: Write `MIGRATION.md`

**Files:**
- Create: `MIGRATION.md` (repo root)

- [ ] **Step 1: Collect the behavior changes noted during Phase 2**

Gather every "note for MIGRATION.md" bullet recorded while migrating (serialization changes, error-format changes, stricter coercion, renamed methods on public models).

- [ ] **Step 2: Write `MIGRATION.md`**

```markdown
# Migration notes: Pydantic v1 → v2 (vX.Y.0)

pipelime now uses the native Pydantic v2 API internally. If you subclass
pipelime models, parse/serialize them, or depend on their JSON schema, note:

## Method renames on pipelime models
- `.dict()` → `.model_dump()`
- `.json()` → `.model_dump_json()`
- `.parse_obj(...)` → `.model_validate(...)`
- `.parse_raw(...)` → `.model_validate_json(...)`
- `Model.__fields__` → `Model.model_fields`

## Serialization shape
- Root types (`NumpyType`, `YamlInput`, `TypeDef`/`ItemType`, `CallableDef`,
  `DAGModel`) no longer wrap their value under a `"__root__"` key. `model_dump()`
  returns the value directly. Access the inner value via `.value` / `.root`.

## Stricter validation
- Pydantic v2 is stricter about type coercion. Inputs that were silently coerced
  under v1 may now raise `ValidationError`. <list concrete cases found>

## Error and JSON-schema formats
- `ValidationError` messages and `model_json_schema()` output follow v2 formats.

## Config
- Custom `model_config` keys: `allow_population_by_field_name` → `populate_by_name`,
  `allow_mutation=False` → `frozen=True`; `copy_on_model_validation` removed.
```
Replace `<list concrete cases found>` and `vX.Y.0` with real values.

- [ ] **Step 3: Commit**

```bash
git add MIGRATION.md
git commit -m "docs: add MIGRATION.md for pydantic v2 user-facing changes"
```

---

### Task 20: Final verification

- [ ] **Step 1: Zero v1 references**

Run: `grep -rl "pydantic.v1" pipelime | wc -l`
Expected: `0`.

- [ ] **Step 2: Full suite, both modes**

Run: `pytest -q`
Expected: PASS.
Run: `pytest --fast -q`
Expected: PASS, noticeably faster.

- [ ] **Step 3: Coverage delta within target**

Run: `pytest --cov=pipelime -q 2>/dev/null | tail -1` and `PIPELIME_TEST_FAST=1 pytest --fast --cov=pipelime -q 2>/dev/null | tail -1`
Expected: fast within ~1–2% of full; update `tests/README.md` numbers if they drifted.

- [ ] **Step 4: Lint/format**

Run: `black pipelime tests && flake8 pipelime || true`
Expected: formatting clean (fix anything trivial).

- [ ] **Step 5: Final commit**

```bash
git add -A && git commit -m "chore: finalize pydantic v2 migration" || echo "nothing to commit"
```

---

## Self-Review (completed by plan author)

- **Spec coverage:** FAST mode (Tasks 1–4) ↔ spec §2; layered migration leaf/core/consumers (Tasks 5–17) ↔ spec §3 layers 1–3; conversion rules (Recipes R1–R9) ↔ spec §4 table; adopt-V2-defaults + test-update policy ↔ spec §5; deliverables MIGRATION.md (Task 19), dep pin (Task 18), zero-v1 metric (Tasks 17/20) ↔ spec §6. All spec sections map to tasks.
- **Placeholders:** the only intentional `<FILL>` placeholders are measured runtime/coverage numbers (Task 4/19/20) that cannot be known until run — each step says exactly which command produces the value.
- **Type consistency:** `fast_params`/`FAST` defined in Task 2 and consumed in Task 3; TUI signatures gain a `name` param consistently in Task 15 and called that way in Task 16; recipe names R1–R9 referenced consistently.
```
