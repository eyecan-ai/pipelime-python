# Pydantic v2 migration — progress ledger

| Subtask | Task | Status | Gate command | Result | Commit | Notes |
|---|---|---|---|---|---|---|
| S0 | S0-T1 (xdist + tiers, + M1 ledger) | done | `make test-full` | 2401 passed, 5 skipped in 118.22s (0:01:58) | 84e5a1a | |
| S0 | S0-T2 (TEST_CHANGES ledger) | done | `pytest tests/pipelime/test_pydantic_contract.py` | n/a (docs) | 23235d9 | TEST_CHANGES ledger + Makefile comment fix |
| S0 | S0-T3 (contract module skeleton) | done | module import | 0 tests, imports | 9e96852 | dual v1/v2 import block |
| S0 | S0-T4 (contracts: compact forms) | done | `-k CompactForms` | 6 passed | 68c6ed0 | splits test uses a new subfolder (tmp_path exists) |
| S0 | S0-T5 (contracts: root wrappers) | done | `-k RootWrappers` | 5 passed | 8bfc517 + 443b101 | 4 pins corrected to v1 facts; ContractItem replaced by a non-Item hierarchy (registry pollution) |
| S0 | S0-T6 (contracts: Optional semantics & polymorphic dumps) | done | `-k "V1Optional or Polymorphic"` | 7 passed | ce8483d + 597ed3d | registry-isolation fixture added |
| S0 | S0-T7 (contracts: command framework) | done | `-k CommandFramework` | 10 passed, 2 xfailed | b79baa4 + 597ed3d | unannotated None-default @command param is v2-only (xfail on v1) |
| S0 | S0-T8 (contracts: stages/entities/sequences) | done | `-k "StagesAndEntities or TestSequences"` | 7 passed, 1 xfailed | e5d7e35 + 32497f5 | NpyNumpyItem; annotations un-stringified; ContractPipe reimport guard |
| S0 | S0-T9 (contracts: validation interfaces, errors, help snapshot) | done | `-k "ValidationInterfaces or HelpRendering"` | 8 passed | 015c34b | help snapshot committed (regen opt-in via PIPELIME_CONTRACT_REGEN=1) |
| S0 | S0-T10 (contracts: modern type hints) | done | `make test-full` | 2447 passed, 5 skipped, 4 xfailed, 114 s | 7df2b04 + 443b101 | S0 gate; 4 expected xfails on v1: var-keyword expansion, unannotated None default, to_pipe str recursion, TUI on UnionType |
| S1 | S1-T1 (PipelimeModel) | done | `pytest tests/pipelime/utils/test_pydantic_compat.py -W error` | 32 passed | 9685aa6, 7e30c38, 733621d, 4f830bf | metaclass captures the class-statement frame at the right depth (guarded use of two pydantic private helpers) — re-verify on the pydantic 2.10 floor in S5-T1 |
| S1 | S1-T2 (PipelimeRootModel) | done | same | 32 passed | 1f4ef35, 4f830bf, 0c8bcff | `__pydantic_base_init__` needed for mapping roots; `__init__` unwraps an instance of its own type |
| S1 | S1-T3 (Field wrapper) | done | same | 32 passed | e1c7b67, 4f830bf, 600f3ca | legacy v1 `Field` kwargs forwarded to pydantic; `regex=` translated to `pattern=` |
| S1 | S1-T4 (introspection) | done | same | 32 passed | 33f96e4 | `FieldView` carries `owner`/`populate_by_name`; `type_info` handles both union spellings |
| S1 | S1-T5 (pydantic_types: NewPath) | done | `-k NewPath` (deferred to T8, module didn't import until then) | 43 passed (combined w/ T6-T8) | 600f3ca | |
| S1 | S1-T6 (NumpyType/YamlInput) | done | `-k "NumpyType or YamlInput or NewPath"` (deferred to T8) | 43 passed (combined w/ T5,T7,T8) | 0c8bcff | |
| S1 | S1-T7 (TypeDef/ItemType/CallableDef) | done | `-k "ItemType or CallableDef"` (deferred to T8, module didn't import until then) | 43 passed (combined w/ T8) | 1267c71 | `wrapped_type()` probed directly against the compat base; string-annotation regression test added |
| S1 | S1-T8 (validation interfaces) | done | `pytest tests/pipelime/utils tests/pipelime/items tests/pipelime/choixe` | 551 passed, 1 failed | 7d710e5 | `test_items.py::test_disabled_serialization_modes` fails (transitively imports `pipelime.sequences`, which does not import until S2a/S2b convert its v1 models off the now-v2 `ItemType`/`CallableDef`/`YamlInput`); left failing per "do not fix those packages" — see task-S1-T7-8-report.md |
| S2a | S2a-T1 (stages/base) | done | import smoke + `StageInput` snippet | ok | 8c894d4 | `SampleStage`/`StageInput` on v2; `StageInput.dict()` == `model_dump()` (`{title: args}`) |
| S2a | S2a-T2 (stages/entities) | done | T2 smoke snippet (six `StageEntity` call shapes) | ok | 8c894d4 | `StageEntity` = regular stage + compat `__init__` + wrap validator/serializer + `__root__` property; `_normalize` peels envelopes in a loop |
| S2a | S2a-T3 (other stages) | done | `pytest tests/pipelime/stages` | see S2a-T5 (categorised) | 8c894d4 | `Transformation` as `PipelimeRootModel`; `Color` from pydantic_extra_types; `root_validator` → `model_validator(after)` |
| S2a | S2a-T4 (samples_sequence) | done | `list(toy_dataset(2))` yields Samples; `to_pipe()` on str fields | ok | 3d42045 | MRO preserved; `to_pipe` no longer recurses into `str` (bug fix) |
| S2a | S2a-T5 (pipes/sources/utils/grabber) | done | `pytest tests/pipelime/stages tests/pipelime/sequences --deselect test_grabber.py` | 332 passed, 31 failed — all expected mid-migration (2 by-name registry lookups → S2b-T7; 29 test modules on `pydantic.v1` forms → S2b-T9) | e8de53a (+ toolkit 5a453ec) | `test_grabber.py` with workers crash-loops until S2b (spawned workers import the v1 registry) — part of the S2b gate |
| S2b | S2b-T1 (progress models) | todo | | | | |
| S2b | S2b-T2 (checkpoint) | todo | | | | |
| S2b | S2b-T3 (piper/model) | todo | | | | |
| S2b | S2b-T4 (interfaces) | todo | | | | |
| S2b | S2b-T5 (split_ops) | todo | | | | |
| S2b | S2b-T6 (other commands) | todo | | | | |
| S2b | S2b-T7 (cli/utils minimal) | todo | | | | |
| S2b | S2b-T8 (choixe decode/$model) | todo | | | | |
| S2b | S2b-T9 (test edits + S2 gate) | todo | | | | |
| S3 | S3-T1 (pretty_print) | todo | | | | |
| S3 | S3-T2 (TUI) | todo | | | | |
| S3 | S3-T3 (main.py) | todo | | | | |
| S3 | S3-T4 (full suite gate) | todo | | | | |
| S4 | S4-T1 (choixe dataclasses) | todo | | | | |
| S4 | S4-T2 (Tier 2 gate) | todo | | | | |
| S5 | S5-T1 (deps + version) | todo | | | | |
| S5 | S5-T2 (migration guide) | todo | | | | |
| S5 | S5-T3 (docs/examples) | todo | | | | |
| S5 | S5-T4 (contract cleanup + warnfree + tox) | todo | | | | |
| S5 | S5-T5 (downstream smoke + release checklist) | todo | | | | |

## Surprises / deviations from the plan
- (S2a) Toolkit additions in 5a453ec: the polymorphic serializer now composes with a declared/inherited `@model_serializer` (the S1 version overwrote the schema's `serialization` slot, which would have dropped `StageInput._serialize`/`BaseEntity._serialize`; `return_schema` is forwarded only for `when_used="always"`); `PipelimeRootModel` gets the same polymorphic hook as `PipelimeModel`; `GenericBeforeBaseModelWarning` is suppressed for classes without free type parameters (`SamplesSequence(SamplesSequenceBase(t.Sequence[Sample]), PipelimeModel)` keeps its MRO).
- (S2a) `test_grabber.py` with `num_workers>0` crash-loops instead of erroring while the registry is still v1: `_GrabContext.wrk_init` runs `PipelimeSymbolsHelper.import_everything()` in every spawned worker and `multiprocessing.Pool` respawns crashing workers forever. Resolves in S2b; the swallowed worker-init error is a pre-existing weakness worth a follow-up.
- (S1) `PipelimeRootModel.__init__` is flagged `__pydantic_base_init__ = True` (as `RootModel.__init__` is): without it pydantic treats the custom `__init__` as validation-relevant and turns `model_validate(<dict>)` into `cls(**dict)`, breaking every wrapper whose root is a mapping (`YamlInput`, later `NodesDefinition`/`Transformation`). Toolkit change made during S1-T6, signed off by the controller.
- (S0) Contract fixtures must not define `Item` subclasses (global item registry) nor rely on v1 mapping unannotated `None` defaults to `Any`; the contract module isolates the pipelime symbol registry per test (`_clean_registry()` + autouse fixture) because `import_symbol` registers every loaded module as an extra module and re-exported stages then count as duplicates (pre-existing quirk, parked).
- (S0) `CallableDef.args_type` will resolve string annotations in S1-T7 (modules with `from __future__ import annotations` crashed `EntityAction` inference on v1).
- S0-T1: per-file xdist grouping was dropped in favour of a per-worker
  isolated pipelime user dir (see the plan's S0-T1 Step 2) — per-file groups
  serialised the two largest test files onto one worker each and made the
  parallel run slower than serial.
- (S1-T8) The S1-T5..T8 brief's "gates are tests/pipelime/utils, items,
  choixe only" and "all must be green" collide with its own documented
  expected breakage: `tests/pipelime/items/test_items.py::TestItems::
  test_disabled_serialization_modes` imports `pipelime.sequences` inline,
  which (as predicted) fails to import once `ItemType`/`CallableDef` are
  v2 — so this one test in an in-scope gate directory fails until S2a/S2b
  convert `pipelime.sequences`. Left failing rather than "fixed" by editing
  the sequences package (explicitly out of scope) or the test (not a v1-API
  edit). Verified this test passed at 4f830bf (pre-S1-T5) and fails the same
  way with only S1-T5/T6 applied, i.e. it is not a regression introduced by
  T7/T8 specifically — it is inherent to converting `pydantic_types.py`
  ahead of its downstream consumers.

## Resuming in a new session (written 2026-09-15 after S2a)

Everything needed to continue lives in git; nothing depends on the old chat session.

1. **State:** S0, S1, S2a complete (see the rows above). HEAD = `4d8145d`, tree clean,
   branch `pydantic_v2`. Expected at rest: `pytest tests/pipelime/utils tests/pipelime/choixe`
   green; `tests/pipelime/stages tests/pipelime/sequences` = 332 passed / 31 failed
   (all mid-migration: by-name registry lookups and test modules still on `pydantic.v1`
   forms); `pipelime.commands`/`piper`/`cli` and the contract module do not import yet.
2. **Next:** S2b — `docs/superpowers/plans/2026-09-13-pydantic-v2-s2b-piper-and-commands.md`.
   Execute its tasks in this order (dependency ruling): T1, T2, **T7** (cli/utils minimal
   compat — `piper/model.py` imports `resolve_pipelime_command`/`format_validation_error`
   from it), T3, T4, T5, T6, T8, T9. Diff base for reviews: `4d8145d`.
3. **S2b gate additions (hand-offs from S2a):** the gate must include
   `tests/pipelime/sequences/test_grabber.py` (workers crash-loop while the registry is v1)
   and the contract `TestSequences::test_to_pipe_roundtrip` must pass (by-name lookup).
4. **How:** invoke `superpowers:subagent-driven-development` on
   `docs/superpowers/plans/2026-09-13-pydantic-v2-migration.md`. Its git-ignored workspace
   `.superpowers/sdd/2026-09-13-pydantic-v2-migration/` (if still on disk) holds the SDD
   ledger `progress.md` with every ruling, the briefs (`task-S2b-brief.md` is already
   assembled in the order above; `brief.sh PLAN LABEL` extracts one task by its `S<k>-T<n>`
   label since the stock `task-brief` script only matches numeric task headings), reports and
   review packages. If that directory is gone, recreate the ledger from this file — the
   rulings that matter for S2b are recorded in the "Surprises / deviations" section above and
   in the plan amendments already committed.
5. **Process facts learned:** subagents died on API session limits three times (opus resets
   ~02:50, sonnet ~20:20 Europe/Rome) — the ledger + per-task commits recovered every time;
   prefer dispatching the large S2b batch early in a limit window and commit per task.
   Reviewers found real gaps in every subtask so far; never skip the task review.
