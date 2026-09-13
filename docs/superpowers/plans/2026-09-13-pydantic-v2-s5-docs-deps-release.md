# S5 — Migration guide, docs, dependencies, version, release checklist

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship pipelime 3.0.0: the downstream migration guide, updated docs/examples, `pyproject.toml` dependencies, the version bump, the warning-free check, tox on all supported Pythons, and the maintainer-only downstream smoke test.

**Architecture:** Documentation only, plus packaging. The contract module drops its temporary v1 branch. Nothing under `pipelime/` changes except `pipelime/__init__.py` (version).

**Tech Stack:** hatchling, tox, Sphinx docs (Markdown via myst).

**Spec:** `docs/superpowers/specs/2026-09-12-pydantic-v2-migration-design.md` §6.

## Global Constraints

- `pydantic>=2.10,<3`, `pydantic-extra-types`, `pytest-xdist` in the `tests` extra; version `3.0.0`.
- pipelime is warning-free under `-W error::pydantic.PydanticDeprecatedSince20` (`make test-warnfree`).

---

### Task S5-T1: Dependencies and version

**Files:**
- Modify: `pyproject.toml:45`, `pipelime/__init__.py:5`

- [ ] **Step 1: Edit**

`pyproject.toml` dependencies: replace `"pydantic>=1.10.17,<3",         # this enables `import pydantic.v1`` with
```toml
    "pydantic>=2.10,<3",
    "pydantic-extra-types",
```
(the `tests` extra already lists `pytest-xdist` since S0.)

`pipelime/__init__.py`: `__version__ = "3.0.0"`.

- [ ] **Step 2: Reinstall and verify the floor**

Run: `.venv/bin/python -m pip install -e ".[tests,draw]" && .venv/bin/python -c "import pipelime, pydantic, pydantic_extra_types; print(pipelime.__version__, pydantic.VERSION)"`
Expected: `3.0.0 2.x`. Then, in a throwaway venv, verify the lower bound once: `python -m venv /tmp/pl310 && /tmp/pl310/bin/pip install -e ".[tests]" "pydantic==2.10.*" && /tmp/pl310/bin/python -m pytest -q -o addopts="" tests/pipelime/utils tests/pipelime/test_pydantic_contract.py` → passed (record the pydantic version used in the ledger).

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml pipelime/__init__.py
git commit -m "build: require pydantic>=2.10,<3 and pydantic-extra-types; bump to 3.0.0"
```

---

### Task S5-T2: The migration guide

**Files:**
- Create: `docs/migration/pydantic_v2.md`
- Modify: `docs/index.md` (link), `README.md` (one-line note in the install/changes section)

- [ ] **Step 1: Write the guide**

```markdown
# Migrating to pipelime 3 (pydantic v2)

pipelime 3.0 is built on the native **pydantic v2** API. pipelime 2.x used the
`pydantic.v1` compatibility layer; pydantic 3 removes it, so every project that
defines commands, stages, sequences or entities must move too. pipelime keeps the
behaviour you rely on — the changes below are the only ones needed.

## 1. Change the imports (required)

```python
# before
import pydantic.v1 as pyd
from pydantic.v1 import Field, PrivateAttr, validator

# after
import pydantic as pyd
from pydantic import PrivateAttr
from pipelime.piper import Field          # pydantic.Field + pipelime flags (piper_port, ...)
```

`pipelime.piper.Field` accepts everything `pydantic.Field` accepts plus
`piper_port=`, `pipe_source=`, `expand_help=`, `is_required=`. Plain
`pydantic.Field(..., piper_port=...)` still works but pydantic prints a
deprecation warning for the extra keyword.

If a subclass of a pipelime model still contains a `pydantic.v1` `Field`,
`PrivateAttr` or `@validator`, pipelime raises a `TypeError` naming the attribute
at import time — a v1 `Field` on a v2 model would otherwise silently become an
opaque default value.

## 2. Validators

| pipelime 2.x (`pydantic.v1`) | pipelime 3 (pydantic v2) |
|---|---|
| `@validator("x")` | `@field_validator("x")` + `@classmethod` |
| `@validator("x", always=True)` | `@field_validator("x")` and `Field(..., validate_default=True)` |
| `def check(cls, v, values)` | `def check(cls, v, info: pydantic.ValidationInfo)` and `info.data` |
| `@root_validator` | `@model_validator(mode="after")` on an instance method returning `self` |

`@validator` still exists in pydantic v2 as a deprecated alias, so old code keeps
working with a warning; `values` is not available there — use `info.data`.

## 3. What stays the same

- `Optional[X]` fields without a default are optional; `x: T = None` accepts an
  explicit `None`; numbers are coerced into `str` fields.
- `NumpyType(__root__=...)`, `.__root__`, `.value`, `.create()`, `.validate()`
  and the `{"__root__": ...}` shape of `.dict()` on value wrappers
  (`model_dump()` returns the bare value, as in pydantic v2).
- Compact forms (`"folder,true"`, `"4,2"`, `"0.3,out"`), `Field(piper_port=...)`
  discovery, polymorphic dumps of stages/commands inside other models, DAG and
  pipe configs written by pipelime 2.x (including `entity: {__root__: ...}`).
- `.dict()`, `.json()`, `parse_obj()` keep working (pydantic marks them deprecated);
  prefer `model_dump()`, `model_dump_json()`, `model_validate()`.

## 4. New

- Modern type hints (`list[int]`, `dict[str, X]`, `tuple[int, str]`,
  `X | None`, `X | Y`) are fully supported in commands, stages, sequences and
  entities, including `pipelime help` and the TUI.
- `SamplesSequence.to_pipe()` works with string fields; `@command` functions
  receive `**kwargs` expanded.

## 5. Behaviour differences you may notice

- Validation is pydantic v2's: unions are resolved in "smart" mode instead of
  left-to-right, error messages have the v2 format, and a few lax coercions of
  v1 are gone (e.g. `"1.5"` is not an `int`).
- Choixe `$model` requires a pydantic v2 model.
- `pipelime help` and DAG validation errors show `name / alias` exactly as before.
```

- [ ] **Step 2: Link it** from `docs/index.md` (a "Migration to 3.0" entry in the table of contents/toctree next to the other top-level pages) and add to `README.md` a sentence under the installation section: "pipelime ≥ 3.0 requires pydantic v2 — see `docs/migration/pydantic_v2.md` if you upgrade from 2.x."

- [ ] **Step 3: Commit**

```bash
git add docs/migration/pydantic_v2.md docs/index.md README.md
git commit -m "docs: pydantic v2 migration guide"
```

---

### Task S5-T3: Docs and examples

**Files:**
- Modify: `docs/tutorials/ml_tutorial/item_creation.md`, `docs/operations/stages.md`, `docs/operations/commands.md`, `docs/cli/piper.md`, `docs/advanced/validation.md`, `docs/operations/pipes.md`, `docs/advanced/entry_points.md`, `docs/tutorials/ml_tutorial/convert_to_underfolder.md`, `examples/cli/my_command.py`, `examples/cli/interfaces.md`, `examples/piper/fakecli.py`, `examples/fake_repo/stages/invert.py`, `examples/fake_repo/stages/avg_color.py`, `examples/sequences/my_pipe.py`

- [ ] **Step 1: Apply the guide to every snippet** (find them with `grep -rn "pydantic.v1\|@validator\|@pyd.validator\|\.dict()\|piper_port=" docs examples --include="*.md" --include="*.py" | grep -v docs/superpowers`)

- `from pydantic.v1 import Field` / `from pydantic import Field` in snippets that pass `piper_port=`/`pipe_source=` → `from pipelime.piper import Field`; other `pydantic.v1` imports → `pydantic`.
- `@validator("x")` → `@field_validator("x")` + `@classmethod` (imports `from pydantic import field_validator`); `@validator("grayscale", always=True)` (stages.md:417) → `@field_validator("grayscale")` with the field declared as `grayscale: pli.ImageItem = Field(None, validate_default=True)` and `values` → `info.data`; `@validator("*")` (item_creation.md:84) → `@field_validator("*")`.
- `x.dict()` (stages.md:118) → `x.model_dump()`.
- `Optional[pli.ImageItem]`-style entity fields in the docs stay as they are (supported).
- Add modern-hint spellings where a snippet declares list/dict/optional fields (`typing.List[int]` → `list[int]`, `Optional[X]` → `X | None`) in `docs/operations/commands.md` and `docs/operations/stages.md` examples.

- [ ] **Step 2: Build the docs once** (optional if Sphinx is not installed): `make docs` → no new warnings from the edited pages.

- [ ] **Step 3: Commit**

```bash
git add docs examples
git commit -m "docs: pydantic v2 imports, validators and modern type hints in examples"
```

---

### Task S5-T4: Drop the contract module's v1 branch, warning-free run, tox

**Files:**
- Modify: `tests/pipelime/test_pydantic_contract.py`

- [ ] **Step 1: Simplify the dual import block**

Replace the block from `# --- dual pydantic import` to `THIS_FILE = ...` with:
```python
import pydantic as pyd

from pipelime.piper import Field

V1 = False  # kept so the xfail markers read as documentation of the 2.x behaviour
```
and delete the now-dead `if V1:` branches (`test_stage_titles`, `test_stage_entity_dump_roundtrip`, `test_alias_error_formatting`, `test_tui`) keeping only the v2 path; `dump()`/`parse_as()` become thin wrappers over `model_dump`/`TypeAdapter`. Remove the three `xfail(V1, ...)` markers.

- [ ] **Step 2: Warning-free check**

Run: `make test-warnfree`
Expected: passed. Any `PydanticDeprecatedSince20` traced to `pipelime/` is fixed at the source (the contract test `test_raw_pydantic_field_with_flags` silences its own, intentional warning).

- [ ] **Step 3: tox on all supported interpreters**

Run: `.venv/bin/python -m tox -q` (needs the 3.10–3.13 interpreters available; otherwise run `make test-full` on 3.11 and CI on the PR covers the matrix).
Expected: green.

- [ ] **Step 4: Commit and ledger**

```bash
git add tests/pipelime/test_pydantic_contract.py
git commit -m "test(contract): drop the pydantic.v1 compatibility branch"
```

---

### Task S5-T5: Downstream smoke test and release checklist (maintainer)

- [ ] **Step 1: Smoke test on one real company project** (maintainer-only; the executor stops here and asks)

1. In the project, install this branch: `pip install -e /path/to/pipelime-python` (or `pip install git+...@pydantic_v2`).
2. Apply the guide §1 (`pydantic.v1` → `pydantic`, `pipelime.piper.Field`) and §2 (validators) mechanically.
3. Run the project's test suite and its main CLI entry points (`pipelime help <cmd>`, one DAG run, one TUI open).
4. Watch for: `TypeError ... uses a pydantic.v1 object` (fix the import), `Input should be a valid dictionary or instance of ...` on a compact form (report: contract gap), `{}` where a stage/command dump was expected (report), `missing` errors on `Optional` fields (report), `PydanticDeprecatedSince20` warnings from project code (switch to `pipelime.piper.Field`/`field_validator`).
5. Report findings in the ledger; anything that is a pipelime bug goes back to the relevant subtask as a new contract test + fix before release.

- [ ] **Step 2: Release checklist**

- `make test-full`, `make test-warnfree`, tox green; CI green on the PR to `develop`.
- `docs/migration/pydantic_v2.md` linked from the docs index and README.
- `git log` on `pydantic_v2` shows one commit per task; ledger complete.
- Open the PR `pydantic_v2` → `develop` with the migration guide summary; the PR description ends with the attribution lines from the session's system reminder.
