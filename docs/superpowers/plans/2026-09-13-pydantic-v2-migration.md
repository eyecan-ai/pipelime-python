# Pydantic v1 → v2 Migration — Master Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate every pipelime model from the `pydantic.v1` shim to the native pydantic v2 API while preserving v1 semantics for downstream code (option A), split into 6 subtasks (S0–S5) that can be executed across multiple sessions.

**Architecture:** A small compat toolkit (`pipelime/utils/pydantic_compat.py`: `PipelimeModel` with a metaclass restoring v1 `Optional` semantics and guarding against v1 leftovers, polymorphic nested serialization, `PipelimeRootModel`, a pipelime `Field` wrapper, and `FieldView` introspection) is built first; the model layers are then converted bottom-up along the import graph (`pydantic_types` → the model graph `stages`+`sequences`+`piper`+`commands`, which is one unit because the stage/command registry in `cli/utils.py` imports all of them → `cli` → `choixe`), each gated by module tests plus contract tests written *before* any source change.

**Tech Stack:** Python 3.10–3.13, pydantic ≥2.10,<3 (installed: 2.12.5), pydantic-core, pydantic-extra-types, pytest + pytest-cov + pytest-xdist, tox.

**Spec:** `docs/superpowers/specs/2026-09-12-pydantic-v2-migration-design.md` (design) and `docs/superpowers/specs/2026-09-12-pydantic-v2-migration-analysis.md` (inventory + verified pydantic facts). Read both before executing any sub-plan.

## Global Constraints

- **Never** consult, diff against, cherry-pick from, or cite `origin/feature/pydanticant`. Everything is implemented from scratch.
- Compatibility policy is **option A**: downstream code needs only `import pydantic.v1` → `import pydantic` (+ `@validator` → `@field_validator` when they use `values`/`always`). pipelime restores v1 semantics: `Optional[X]` without default is optional; `x: T = None` accepts explicit `None`; numbers are coerced into `str` fields; nested dumps are polymorphic; `cls(__root__=...)`, `.__root__`, `.dict()` envelope on root wrappers; `Field(piper_port=...)` accepted.
- Dependencies after migration: `pydantic>=2.10,<3`, `pydantic-extra-types` (runtime); `pytest-xdist` added to the `tests` extra. Version bumps to `3.0.0` (S5 only).
- Modern type hints (`list[int]`, `dict[str, X]`, `tuple[...]`, `X | None`, `X | Y`) are first-class everywhere; `typing.List`/`Optional` keep working.
- MRO of classes that put a non-pydantic base before `BaseModel` (`SamplesSequence`, `DataStream`, `LocalCheckpoint`, the `PydanticField*Mixin` interfaces, `SplitBase`) is **preserved** as is.
- Tests are the oracle: a test may be edited only when it uses a v1 API *form* in test-local code; every edit is recorded in `tests/TEST_CHANGES.md`. `tests/sample_data/cli/*.py` receive exactly the migration-guide edit.
- No `pydantic.v1` import may remain under `pipelime/` at the end of S4 except the `try`-guarded one in `pydantic_compat.py`. pipelime must be warning-free under `-W error::pydantic.PydanticDeprecatedSince20` after S5.
- Work on branch `pydantic_v2`; one commit per task; never squash/rebase mid-migration.
- Test commands always use the project venv: `.venv/bin/python -m pytest ...`. Tier 0 = `.venv/bin/python -m pytest -q -o addopts="" -p no:cacheprovider tests/pipelime/utils tests/pipelime/stages tests/pipelime/piper tests/pipelime/sequences tests/pipelime/choixe tests/pipelime/test_pydantic_contract.py --deselect tests/pipelime/sequences/test_grabber.py --ignore tests/pipelime/piper/progress` (the `make test-tier0` target defined in S0 wraps exactly this).

---

## File map

| File | Responsibility | Subtask |
|---|---|---|
| `tests/conftest.py` | xdist per-file grouping, `zmq` group, tier aliases | S0 |
| `tests/pipelime/test_pydantic_contract.py` | contract tests (dual v1/v2 imports until S5) | S0 |
| `tests/TEST_CHANGES.md` | ledger of test edits with justification | S0 |
| `pipelime/utils/pydantic_compat.py` | the toolkit (§3 of the spec) | S1 |
| `tests/pipelime/utils/test_pydantic_compat.py` | toolkit unit tests | S1 |
| `pipelime/utils/pydantic_types.py` | value wrappers, `NewPath`, validation interfaces | S1 |
| `pipelime/stages/*.py` | stages, entities, augmentations | S2a |
| `pipelime/sequences/*.py` | sequences, pipes, sources, grabber, `DataStream` | S2a |
| `pipelime/piper/model.py`, `checkpoint.py`, `progress/model.py`, `pipelime/piper/__init__.py` | command framework, `Field` re-export | S2b |
| `pipelime/commands/*.py` | interfaces + commands | S2b |
| `pipelime/cli/utils.py` (`_symbol_name`, imports, `format_validation_error`) | minimal registry/error compat needed by S2 | S2b |
| `pipelime/cli/pretty_print.py`, `utils.py` (rest), `tui/utils.py`, `tui/tui.py`, `main.py` | help, TUI, error display | S3 |
| `pipelime/choixe/ast/nodes.py`, `visitors/decoder.py`, `visitors/processor.py` | AST dataclasses, `$model` | S4 |
| `docs/migration/pydantic_v2.md`, docs, `examples/`, `README.md`, `pyproject.toml`, `pipelime/__init__.py` | guide, docs, deps, version | S5 |
| `docs/superpowers/plans/2026-09-13-pydantic-v2-migration-progress.md` | the ledger | S0 |

## Sub-plans (execute in order)

| # | Plan | Gate |
|---|---|---|
| S0 | `2026-09-13-pydantic-v2-s0-test-infra-and-contract-tests.md` | contract tests green on v1 (expected xfails documented); Tier 2 green with xdist |
| S1 | `2026-09-13-pydantic-v2-s1-toolkit-and-pydantic-types.md` | `tests/pipelime/utils` + toolkit tests |
| S2a | `2026-09-13-pydantic-v2-s2a-stages-and-sequences.md` | import smoke + direct-construction tests (no registry) |
| S2b | `2026-09-13-pydantic-v2-s2b-piper-and-commands.md` | `tests/pipelime/{stages,sequences,piper,commands}` + contract tests minus the CLI ones (**S2 gate**) |
| S3 | `2026-09-13-pydantic-v2-s3-cli.md` | **Tier 2 full suite green** |
| S4 | `2026-09-13-pydantic-v2-s4-choixe.md` | choixe tests + Tier 2 |
| S5 | `2026-09-13-pydantic-v2-s5-docs-deps-release.md` | Tier 2 + warning-free Tier 0 + tox + maintainer smoke test |

Between S1 and the end of S2b, `import pipelime.stages` / `pipelime.sequences` / `pipelime.commands` fail (v1 and v2 models cannot nest, and the stage/command registry imports the whole graph). S2a and S2b list exactly which checks are expected to pass at each step.

## Session protocol

**Start of every session**
1. `git status` must be clean and `git branch --show-current` must print `pydantic_v2`.
2. Read: analysis spec → design spec → this master plan → the ledger → the sub-plan of the current subtask.
3. Re-run the gate command recorded in the ledger for the last *completed* task and confirm it is green before touching code.

**End of every session (even mid-task)**
1. Commit finished tasks; leave unfinished work uncommitted only if it is a single task in progress and say so in the ledger.
2. Update the ledger: task status, gate command + result, commit hash, surprises.

## Ledger format (`docs/superpowers/plans/2026-09-13-pydantic-v2-migration-progress.md`)

```markdown
# Pydantic v2 migration — progress ledger

| Subtask | Task | Status | Gate command | Result | Commit | Notes |
|---|---|---|---|---|---|---|
| S0 | S0-T1 xdist grouping | done | `make test-full` | 2406 passed | abc1234 | — |

## Surprises / deviations from the plan
- (date) (subtask/task) what happened, what was decided, where it is recorded
```

---

### Task M1: Create the ledger

**Files:**
- Create: `docs/superpowers/plans/2026-09-13-pydantic-v2-migration-progress.md`

- [ ] **Step 1: Write the ledger file** with the header and empty table from "Ledger format" above, plus one row per sub-plan task (S0-T1 … S5-Tn, status `todo`). Copy task names from the sub-plans.

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/plans/2026-09-13-pydantic-v2-migration-progress.md
git commit -m "docs: add pydantic v2 migration progress ledger"
```

Then open `2026-09-13-pydantic-v2-s0-test-infra-and-contract-tests.md`.
