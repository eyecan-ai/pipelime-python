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
| S2b | S2b-T1 (progress models) | done | `pytest tests/pipelime/piper/progress` | 46 passed | feeccd2 | `OperationInfo`/`ProgressUpdate` on `PipelimeModel`; ZMQ wire on `model_dump_json`/`model_validate_json` |
| S2b | S2b-T2 (checkpoint) | done | import smoke (`LocalCheckpoint(folder=...)`, existing and new folder) | ok | 3474703 | |
| S2b | S2b-T3 (piper/model) | done | smoke snippet (command w/ alias + ports, `lazy()`, `@command` with `*args`/`**kwargs`, `NodesDefinition`/`DAGModel` round trip); gate at T9 | ok | 5beb53b | `field_extra` dropped from the brief's import list (unused); `pipelime.piper.Field` re-export placed after the model imports (isort) |
| S2b | S2b-T4 (interfaces) | done | standalone-module smoke (every compact form, `pyd_field` flags, origin instance for `OutputValueInterface[int]`); gate at T9 | ok | a4e6c6f | `CompactFormModel._validate_compact` has the 3-branch form of the dispatch decision (any `BaseModel` instance → `handler(value)`) |
| S2b | S2b-T5 (split_ops) | done | gate at T9 (`tests/pipelime/commands/test_split.py`) | passed | 2928a63 | every `pyd.Field(` → pipelime `Field(` (none carried `piper_port` directly; keeps the brief's `Field` import used) |
| S2b | S2b-T6 (other commands) | done | `import pipelime.commands` + registry smoke; DAG/`piper_dag`/`FilterCommand` smoke; gate at T9 | ok | befd146 | `SetMetadataCommand.filter_fn` (re-declared) also gets `validate_default=True` (the inherited `always=True` validator applied to it in v1); `choixe_parser.py` `parse_obj` → `model_validate` |
| S2b | S2b-T7 (cli/utils minimal) | done | import smoke; gate at T9 | ok | cdb9e55 (+ 5ce9b94) | `resolve_pipelime_command`/`format_validation_error`; 5ce9b94: `_load_symbols` no longer flags the *same class* seen from two modules as a duplicate (pre-existing, reproduced on `main`; made every grabber worker crash-loop after `tests/pipelime/stages` registered `test_entities.py`) |
| S2b | S2b-T8 (choixe decode/$model) | done | `pytest tests/pipelime/choixe` | 442 passed | e7246c4 | `test_processor.py::test_model` compares `model_dump()` + class name: `$model` imports the test *file* as a second module, so v2's type-aware `__eq__` no longer equates the two `MyModel` classes (v1 compared dicts) |
| S2b | S2b-T9 (test edits + S2 gate) | done | (1) `pytest tests/pipelime/{stages,sequences,piper,utils,choixe,items}` (2) `pytest -n auto --dist loadgroup tests/pipelime/commands` (3) `pytest tests/pipelime/test_pydantic_contract.py -k "not HelpRendering and not test_help and not test_tui"` | (1) 1041 passed, 4 skipped, **8 failed** — all `test_command_decorator.py::test_is_command[*]`, which call `pretty_print.print_models_short_help`/`print_model_info` (`__config__`, S3-T1 scope) (2) 1302 passed, 1 skipped (3) 47 passed; `-W error tests/pipelime/utils/test_pydantic_compat.py` 41 passed | e2753b5 (+ f383470, f943809, e938a7a) | `test_grabber.py` runs and passes inside gate (1) (43 s, no crash-loop); both contract xfails now pass. Toolkit: f383470 bool→str coercion for `str` fields (v1 parity: `varpos("a", 1, True)`); f943809 `NumpyType._coerce` keeps an ndarray as is. Two contract pins corrected (see TEST_CHANGES.md). Warnings are only test-side `parse_obj`/`dict`/`json` deprecations (S5) |
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
- (S2b) **`test_is_command` ×8 fail at the S2 gate and cannot pass before S3-T1:**
  `tests/pipelime/piper/test_command_decorator.py::test_is_command` calls
  `pipelime.cli.pretty_print.print_models_short_help`/`print_model_info`, which still read
  `model_cls.__config__` (v1). `pretty_print.py` is S3 scope and was not touched; the 8
  failures are the only red in gate (1). Same shape as the S1-T8 precedent.
- (S2b) **The `test_grabber.py` crash-loop was not (only) the v1 registry:** it reproduces
  byte-for-byte on `main` when `tests/pipelime/stages` runs first in the same process.
  `test_entities.py` imports a symbol from its own file path, which registers the test file
  as an extra module; the file re-exports `StageEntity`, and `_load_symbols` flagged the same
  class object seen from `pipelime.stages` and from the extra module as `Duplicate stage
  'entity'` in every spawned worker (the S0 ledger had parked this quirk with a per-test
  registry isolation in the contract module). Fixed at the source (5ce9b94: identity check
  in both duplicate checks, regression test `tests/pipelime/cli/test_symbols_helper.py`).
  The `_classpath` overwrite of a re-exported class (cosmetic, pre-existing) is untouched.
- (S2b) **Toolkit addition f383470 — bools coerced to `str` fields:** v1's `str_validator`
  took the `int` path for bools (`True` → `"True"`); v2's `coerce_numbers_to_str` leaves
  bools out. `test_command_decorator.py::test_varpos/test_varkw` (`*a: str` given `True`)
  and CLI values such as `+name true` (YAML-parsed before validation) rely on it. Every
  `str` core schema among a pipelime model's *own* fields gets a before-validator; nested
  models keep their own rule (as with `coerce_numbers_to_str`). Unit tests added (incl.
  `Union[bool, str]` keeping the bool member, JSON schema unchanged, per-model scope).
- (S2b) **`NumpyType._coerce` copied an ndarray input** (`np.array(arr)`), failing the
  contract pin `NumpyType(__root__=arr).__root__ is arr` (v1 kept identity through the
  `arbitrary_types_allowed` isinstance check). Fixed in f943809 (S1 module touched).
- (S2b) **Two contract pins corrected to verified facts** (rows in `tests/TEST_CHANGES.md`):
  `TestSequences::test_to_pipe_roundtrip` expected the bare key `"contract_pipe"`, but
  `_add_operator_path` (byte-identical to 2.x) serializes operators defined outside
  `pipelime` with their module path — the pin was never exercised on v1 (strict xfail on the
  `to_pipe` str-recursion bug); `TestModernTypeHints::test_validation` pinned v1
  left-to-right resolution for `int | str` given `"5"`, whereas the design spec's risks
  section accepts v2 smart unions (`c.f == (5 if V1 else "5")`).
- (S2b) `test_processor.py::test_model` (choixe) compares `model_dump()` and the class name
  instead of `==`: `$model` loads the test *file* as a second module, so the two `MyModel`
  classes differ and v2's `__eq__` (type-aware) no longer equates them as v1's dict
  comparison did. Recorded in `tests/TEST_CHANGES.md`.
- (S2b) `SetMetadataCommand` re-declares `filter_fn`; on v1 the inherited `always=True`
  validator still ran on it, so on v2 both declarations carry `validate_default=True`.
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
