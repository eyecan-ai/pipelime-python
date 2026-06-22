# Pydantic V1 → V3 Migration + FAST Test Mode — Design

**Date:** 2026-06-22
**Status:** Approved (design); pending implementation plan
**Branch:** `feature/pydanticant`

## Background

`pipelime-python` currently builds all of its models on the `pydantic.v1`
compatibility shim that ships *inside* Pydantic 2.x. Installed Pydantic is
**2.11.7**; the dependency is pinned `pydantic>=1.10.17,<3` (the `<3` is what
keeps `import pydantic.v1` available).

Pydantic **V3** (the upcoming major) removes the `pydantic.v1` namespace
entirely. Therefore the engineering work to become "V3-ready" is to migrate all
`pydantic.v1` usage onto the **native modern Pydantic API**. Code written this
way runs on the installed 2.11.x today and is forward-compatible with V3.

### Current usage (measured)

- **37 files** under `pipelime/` import `pydantic.v1`.
- Predominant style: `import pydantic.v1 as pyd`, plus
  `from pydantic.v1 import BaseModel/Field/validator/...` and
  `from pydantic.v1.generics import GenericModel`.
- Test suite: **2406 collected tests / ~400 test functions**, heavily
  parametrized (45 `nproc`/`prefetch`/`lazy` combinations) — the reason it is slow.

### V1 API surface to migrate (measured)

| V1 construct | Count | Native target | Difficulty |
|---|---|---|---|
| `__root__` models | 7 files / 63 refs | `RootModel[T]` + `.root` | High (structural) |
| `@validator` / `root_validator` (incl. `always=True`) | 25 sites | `@field_validator` (+ `validate_default`) / `@model_validator` | Medium |
| `.dict()` / `.json()` / `parse_obj` / `parse_raw` / `parse_file` | ~50 calls | `model_dump` / `model_dump_json` / `model_validate` / `model_validate_json` | Low |
| `__fields__` | 24 | `model_fields` | Low |
| `GenericModel` | 4 files | `BaseModel` + `Generic[T]` | Low |
| `ModelField` (TUI field introspection) | `cli/tui/utils.py` | `FieldInfo` / `ValidationInfo` | High (API redesigned) |
| Config knobs (`copy_on_model_validation`, `allow_population_by_field_name`, `Extra.*`, `allow_mutation`, `underscore_attrs_are_private`, `smart_union`) | many | `ConfigDict` | Medium |
| Dynamic model build (`piper/model.py` `_make_field` / `create_model`) | 1 framework | native `create_model` / `FieldInfo` | High (framework core) |

The hardest parts are pipelime's *own* framework layer: the `__root__`-based
models, the TUI field introspection, and the dynamic command-model builder.

## Decisions (locked)

1. **Target:** native modern Pydantic API; keep the `<3` pin; write only
   forward-compatible code. (No hard bump to `pydantic>=3` in this effort.)
2. **FAST mode:** a `--fast` pytest flag (+ `PIPELIME_TEST_FAST` env var) that
   collapses combinatorial parametrization to one representative combo and
   deselects a small set of `@pytest.mark.slow` tests. Optimize for *high
   coverage, low runtime* — cut redundant parametrization, not code paths.
3. **Back-compat:** adopt Pydantic V2 defaults, accept minor user-facing breaks,
   and document them in a root `MIGRATION.md`.
4. **Execution:** one migration effort, committed/reviewed in **bottom-up
   layers**; the FAST suite must be green before each layer lands.

## Scope & Phasing

Two sequential phases, one spec. Phase 1 ships first because its FAST suite is
the inner-loop tooling for Phase 2.

### Phase 1 — FAST test mode

- Add to `tests/conftest.py`:
  - `pytest_addoption("--fast", ...)`, also honoring `PIPELIME_TEST_FAST=1`.
  - registration of a `slow` marker.
  - a `pytest_collection_modifyitems` hook that deselects `slow` tests when fast.
- Add a shared helper (e.g. `tests/_fast.py` exposing `fast_params(full, fast)`)
  used inside `@pytest.mark.parametrize` so matrices like `nproc×prefetch×lazy`
  collapse to **one representative combo** in fast mode (e.g.
  `nproc=0, prefetch=2, lazy=True`) while keeping the full matrix normally.
- Tag a small number of genuinely heavy tests with `@pytest.mark.slow`.
- **Coverage preserved:** nearly every test *function* still runs in fast mode.
- Short usage note in a `tests/README`.

**Success bar:** `pytest --fast` keeps line coverage within ~1–2% of the full
run and finishes in a fraction of the wall-clock time; normal `pytest` behavior
is unchanged when the flag/env var are absent.

### Phase 2 — Migration, by layer

Bottom-up; FAST suite green before each layer lands.

- **Layer 1 — leaf utilities:** `utils/pydantic_types.py`,
  `choixe/ast/nodes.py`, `choixe/visitors/decoder.py`. Establishes the patterns
  (`ConfigDict`, `RootModel`, `field_validator`).
- **Layer 2 — core framework:** `stages/base.py`, `stages/entities.py`,
  `piper/model.py` (dynamic `create_model`/`_make_field`),
  `piper/progress/model.py`, `piper/checkpoint.py`, `sequences/*`. The hard
  structural work: `__root__`→`RootModel`, `GenericModel`→`Generic[T]`,
  validator-signature rewrites.
- **Layer 3 — consumers & introspection:** all `commands/*`, `cli/*` including
  `cli/tui/utils.py` (`ModelField`→`FieldInfo`), `cli/pretty_print.py`,
  `cli/utils.py`.

## Per-construct conversion rules

| From (`pydantic.v1`) | To (native) |
|---|---|
| `import pydantic.v1 as pyd` | `import pydantic as pyd` |
| `class X(BaseModel, extra="forbid", copy_on_model_validation="none", allow_population_by_field_name=True)` | `model_config = ConfigDict(extra="forbid", revalidate_instances="never", populate_by_name=True)` |
| `allow_mutation=False` | `frozen=True` |
| `Extra.ignore` / `Extra.forbid` | `"ignore"` / `"forbid"` |
| `underscore_attrs_are_private`, `smart_union` | drop (native defaults) |
| `__root__: T` | `RootModel[T]`; access via `.root` |
| `class X(GenericModel, Generic[T])` | `class X(BaseModel, Generic[T])` |
| `@validator("f")` | `@field_validator("f")` |
| `@validator("f", always=True)` | `@field_validator("f")` + `validate_default=True` on the field, or `@model_validator` |
| `@root_validator` | `@model_validator(mode="before")` or `mode="after"` |
| validator with `field: ModelField` | `@field_validator` + `ValidationInfo`, or `FieldInfo` introspection |
| `.dict(...)` / `.json(...)` | `.model_dump(...)` / `.model_dump_json(...)` |
| `parse_obj` / `parse_raw` / `parse_file` | `model_validate` / `model_validate_json` / read-then-validate |
| `Model.__fields__` | `Model.model_fields` |

Notes on known behavior shifts to handle by **adopting V2 defaults**:

- V2 coercion is stricter; tests that relied on lax coercion are updated to V2
  behavior and the change is recorded in `MIGRATION.md`.
- Error objects and JSON-schema output differ; any test asserting exact error
  text / schema shape is updated to V2 output.
- `RootModel` serialization unwraps to the root value (no `{"__root__": ...}`
  envelope) — a user-facing change to note.

## Verification

- Per layer: `pytest --fast` green.
- Per phase completion: full `pytest` green.
- Tracked metric: `grep -rl "pydantic.v1" pipelime | wc -l` trends to **0**.
- Behavioral diffs surfaced by tests are resolved by adopting V2 behavior,
  updating the test, and recording the change in `MIGRATION.md`.

## Deliverables

- All 37 files migrated; **zero** `pydantic.v1` references in `pipelime/`.
- FAST test mode (`--fast` flag / `PIPELIME_TEST_FAST` env var) + helper.
- Root `MIGRATION.md` listing user-facing behavior changes (serialization,
  method renames, stricter validation, `RootModel` unwrapping).
- Updated `tests/conftest.py` and `tests/_fast.py`.

## Out of scope (YAGNI)

- Bumping the dependency to `pydantic>=3` (separate follow-up once V3 is stable).
- pytest-xdist parallelization.
- Any refactor not required by the migration.
