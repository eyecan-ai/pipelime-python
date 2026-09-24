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
| S2b | S2b-T4 (interfaces) | done | standalone-module smoke (every compact form, `pyd_field` flags, origin instance for `OutputValueInterface[int]`); gate at T9 | ok | a4e6c6f + 26a45b3 | `CompactFormModel._validate_compact` has the 3-branch form of the dispatch decision (any `BaseModel` instance → `handler(value)`); fix round 1 (26a45b3): an instance of the generic *origin* (`OutputValueInterface(...)` into `OutputValueInterface[int]`) is kept by identity, as v1 did |
| S2b | S2b-T5 (split_ops) | done | gate at T9 (`tests/pipelime/commands/test_split.py`) | passed | 2928a63 | every `pyd.Field(` → pipelime `Field(` (none carried `piper_port` directly; keeps the brief's `Field` import used) |
| S2b | S2b-T6 (other commands) | done | `import pipelime.commands` + registry smoke; DAG/`piper_dag`/`FilterCommand` smoke; gate at T9 | ok | befd146 | `SetMetadataCommand.filter_fn` (re-declared) also gets `validate_default=True` (the inherited `always=True` validator applied to it in v1); `choixe_parser.py` `parse_obj` → `model_validate` |
| S2b | S2b-T7 (cli/utils minimal) | done | import smoke; gate at T9 | ok | cdb9e55 (+ 5ce9b94) | `resolve_pipelime_command`/`format_validation_error`; 5ce9b94: `_load_symbols` no longer flags the *same class* seen from two modules as a duplicate (pre-existing, reproduced on `main`; made every grabber worker crash-loop after `tests/pipelime/stages` registered `test_entities.py`) |
| S2b | S2b-T8 (choixe decode/$model) | done | `pytest tests/pipelime/choixe` | 442 passed | e7246c4 | `test_processor.py::test_model` compares `model_dump()` + class name: `$model` imports the test *file* as a second module, so v2's type-aware `__eq__` no longer equates the two `MyModel` classes (v1 compared dicts) |
| S2b | S2b-T9 (test edits + S2 gate) | done | (1) `pytest tests/pipelime/{stages,sequences,piper,utils,choixe,items}` (2) `pytest -n auto --dist loadgroup tests/pipelime/commands` (3) `pytest tests/pipelime/test_pydantic_contract.py -k "not HelpRendering and not test_help and not test_tui"` | (1) 1041 passed, 4 skipped, **8 failed** — all `test_command_decorator.py::test_is_command[*]`, which call `pretty_print.print_models_short_help`/`print_model_info` (`__config__`, S3-T1 scope) (2) 1302 passed, 1 skipped (3) 47 passed; `-W error tests/pipelime/utils/test_pydantic_compat.py` 41 passed | e2753b5 (+ f383470, f943809, e938a7a) | `test_grabber.py` runs and passes inside gate (1) (43 s, no crash-loop); both contract xfails now pass. Toolkit: f383470 bool→str coercion for `str` fields (v1 parity: `varpos("a", 1, True)`); f943809 `NumpyType._coerce` keeps an ndarray as is. Two contract pins corrected (see TEST_CHANGES.md). Warnings are only test-side `parse_obj`/`dict`/`json` deprecations (S5) |
| S3 | S3-T1 (pretty_print) | done | `pytest -o addopts="" tests/pipelime/test_pydantic_contract.py -k "HelpRendering or test_help"` | 2 passed (snapshot byte-identical, unchanged); `tests/pipelime/piper/test_command_decorator.py` 17 passed (the 8 `test_is_command` failures fixed) | b08e333 | help on `FieldView`/`type_info`; modern hints render `[int, ...]`, `{str: float}`, `(int, str)`, `int \| str`, `X \| None` → `X`; `inspect.getdoc(inner_type)` fallback kept |
| S3 | S3-T2 (TUI) | done | `pytest -o addopts="" tests/pipelime/cli/test_tui.py tests/pipelime/test_pydantic_contract.py -k "tui or TUI or ModernTypeHints"` | 48 passed (contract `test_tui` passes, no xfail) | 1cfe6ab | also `are_stageinput_args_present` (not in the brief, same v1 loop); `test_tui.py` v1 forms edited (TEST_CHANGES.md); `x: T = None` shows `Optional[T]` (ruling 2, no test pins `T`) |
| S3 | S3-T3 (main.py) | done | `pytest -o addopts="" tests/pipelime/cli`; leftover `grep -rn "pydantic.v1" pipelime` | 94 passed; grep → only the error-message text in `choixe/visitors/processor.py:223` (outside the excluded `pydantic_compat.py`/`choixe/ast/nodes.py`) | 05d08f6 (+ ac52fa9, 93716fd, 2403b61) | run path prints `format_validation_error(e, cmd_cls)`; `show_field_alias_valerr` **kept** (see Surprises); extras: ac52fa9 `LazyCommand` unset required field → `None`, 93716fd `_classpath` of re-exported symbols kept, 2403b61 `ClassicPiperGraphCommand` on `PipelimeModel` (`-n auto tests/pipelime/commands` 1304 passed, 1 skipped) |
| S3 | S3-T4 (full suite gate) | done | (1) `make test-full` (2) `make test-tier1` (3) `pytest -o addopts="" -W error tests/pipelime/utils/test_pydantic_compat.py` | (1) 2504 passed, 5 skipped (S0 baseline skips), **0 failed, 0 xfailed**, 123.44 s at d51f2ba (2503 passed at 2403b61) (2) 1992 passed, 4 skipped, 436 s at 2403b61; after fix round 1: 1995 passed, 4 skipped, 439 s at 9865e06 (3) 45 passed (46 after fix round 1) | d51f2ba (self-review fix), fix round 1: 8885c6b (I1), 9865e06 (I2) + docs commits | no failure to triage; pydantic deprecation warnings are all test-side (`parse_obj`/`dict`/`json`, raw `pyd.Field(piper_port=)` in `tests/sample_data/cli/ckpt_dag.py`) — S5 |
| S4 | S4-T1 (choixe dataclasses) | done | `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/choixe` | 0a684a4: 442 passed in 1.24s; after 85a3558 (node type-check restoration, controller ruling): 449 passed (7 new regression tests) in 1.26s | 0a684a4 (+ 85a3558) | only the import lines changed at 0a684a4; `nodes.py` imports no `pydantic` at all now; no hash/equality literal broke, so no `tests/TEST_CHANGES.md` entry. Controller ruling on the probe concern (see Surprises): 85a3558 adds `Node.__post_init__` restoring construction-time type checks on `Node`-typed fields (mostly `HashNode`), so malformed input that used to raise now raises again (`ChoixeParsingError`, via `_parse_token`'s `except TypeError`, or via `_parse_dict`'s catch-all); container-typed fields, `Any`, and the 5 custom-`__init__` node types (`ListNode`, `DictBundleNode`, `StrBundleNode`, `SweepNode`, `RandNode`) stay unchecked, matching v1 |
| S4 | S4-T2 (Tier 2 gate) | done | `make test-full` | 0a684a4: 2506 passed, 5 skipped, 0 failed, 119.79 s; after 85a3558: 2513 passed (+7 new tests), 5 skipped, 0 failed, 121.49 s (0:02:01) — matches the S3 gate counts plus the new regression tests | 99da1e5 (+ e8c898a) | before/after probe of 17 malformed choixe inputs — see Surprises below and `task-S4-report.md` |
| S5 | S5-T1 (deps + version) | todo | | | | |
| S5 | S5-T2 (migration guide) | todo | | | | |
| S5 | S5-T3 (docs/examples) | todo | | | | |
| S5 | S5-T4 (contract cleanup + warnfree + tox) | todo | | | | |
| S5 | S5-T5 (downstream smoke + release checklist) | todo | | | | |

## Surprises / deviations from the plan
- (S4, 2026-09-24) **Controller ruling — construction-time node type checks restored
  (85a3558):** stdlib dataclasses dropped the field-type validation pydantic v1's
  dataclasses used to perform at construction; per the probe below and pipelime's
  backward-compat policy (absorb dependency-semantics changes inside pipelime rather than
  changing behaviour silently), `Node.__post_init__` restores it for fields directly typed
  to a `Node` subclass, `Optional[<Node subclass>]`, or a `Union` of `Node` subclasses
  (container-typed fields, `Any`, and the 5 nodes with a custom `__init__` stay unchecked,
  matching v1). Effect: the 9 token-form cases that used to leak an uncaught
  `pydantic.v1.ValidationError` now raise `ChoixeParsingError` instead (an improvement —
  same as the 3 dict-form cases, which already raised `ChoixeParsingError` both before and
  after S4-T1). Document for the S5 migration guide: callers that used to catch a raw
  pydantic `ValidationError` around `choixe.ast.parser.parse` for a handful of
  token-directive shapes (`$var`, `$import`, `$symbol`, `$index`, `$item`, `$date`, `$tmp`,
  `$cmd`) must now catch `ChoixeParsingError` instead — a strictly better contract, but a
  visible one.
- (S4, 2026-09-24) Before/after probe (`nodes.py` off pydantic v1 dataclasses, pre-fix): of
  17 deliberately malformed choixe inputs (wrong Node subtype injected into a typed field),
  12 changed outcome — 9 that used to leak an uncaught pydantic `ValidationError` out of
  `_parse_token` (only `TypeError` is caught there) and 3 that used to raise a proper
  `ChoixeParsingError` out of `_parse_dict`'s catch-all parsed *silently*, with the
  wrong-typed node embedded; the 442-test choixe suite did not exercise any of these
  shapes, so it stayed green. Reported DONE_WITH_CONCERNS; resolved by 85a3558 above. Full
  inputs/outcomes in `task-S4-report.md`.
- (S3) **`show_field_alias_valerr` kept, by controller ruling** (it replaces ruling 3, which
  assumed nothing imports it): `test_pydantic_contract.py::TestValidationInterfaces::test_alias_error_formatting`
  imports it outside its `if V1:` branch, and it is a public `pipelime.cli.utils` name that
  downstream code may import. Nothing under `pipelime/` calls it any more.
- (S3) `cli/tui/utils.py::are_stageinput_args_present` (not listed in the brief) had the same
  `__fields__`/`.alias` loop as `is_tui_needed`; converted to `iter_fields`/`effective_alias`.
- (S3) Ruling 1b supersedes the S2b note above: `_load_symbols` now sets `_classpath` only for
  classes defined in the loaded file (`sym_cls.__module__ == module_.__name__`); a re-exported
  pipelime class keeps its pipelime class path (regression assertions in `test_symbols_helper.py`).
- (S3) **Help type names, self-review fix d51f2ba:** the brief's `_human_readable_type` rendered
  a value among the type args via `str()` (`Tuple[int, ...]` → `(int, Ellipsis)`, 2.x:
  `(int, ellipsis)`) and a `TypeVar` as `~T` (2.x: `T`). Restored the 2.x fallback (a value by
  its class, named typing objects by `__name__`); `Annotated[...]` still prints as written. The
  only remaining difference found is `NewType("N", int)`: 2.x printed the literal `NewType`, now `N`.
  Pinned by the new contract test `TestHelpRendering::test_type_names`.
- (S3 fix round 1) **Validation errors printed as Rich markup (8885c6b):** the run path passed
  `format_validation_error(...)` to `print_error` unescaped, so every `[type=...]` suffix was dropped
  and a `[/x]`-like span in a message raised `rich.errors.MarkupError` in place of the
  `ValidationError`. Now escaped; regression test `test_base.py::test_validation_error_text_is_not_markup`.
- (S3 fix round 1) **Nested constrained types shown as `Annotated[...]` (9865e06):** v2 constrained
  types (`PositiveInt`, `NonNegativeInt`, `DirectoryPath`, ...) are `Annotated[X, <constraints>]`;
  top-level metadata is stripped by pydantic, nested metadata was printed raw in help and TUI
  (`SplitCommand.shuffle` → `bool | Annotated[int, Gt(gt=0)]`). New toolkit helper
  `strip_annotated` (recursive, returns the type itself when there is nothing to strip, so every
  existing string is unchanged); help and TUI show `X`. v1's constrained-type names are not restored
  (controller ruling). The help signature line (`inspect.formatannotation`) is unchanged and still
  shows nested `Annotated[...]` as written.
- (S3) `LazyCommand.__getattr__` of an unset required field returned `PydanticUndefined` since S2b;
  it returns `None` again as v1 `ModelField.get_default()` did (ac52fa9, contract regression test).
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
  Fix round 1 (de622f8): pydantic hands back the *stored* schema of a built class on every
  reference from another model, so both hooks (this one and the S2a polymorphic
  serializer) stacked a new layer per reference (38–40 on the dataset interfaces,
  `RecursionError` from `model_json_schema()` past ~200 references). `_apply_v1_hooks`
  now marks the schema's `metadata` with the class and is idempotent per class (a
  subclass's fresh schema gets its own hooks); the walk follows only schema-bearing keys
  (a dict default `{"type": "str"}` was rewritten), skips strict `str` schemas
  (`StrictStr` rejects bools, as on v1) and already wrapped ones. Unit tests added.
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

## Resuming in a new session (written 2026-09-24 after S4)

Everything needed to continue lives in git; nothing depends on the old chat session.

1. **State:** S0, S1, S2a, S2b, S3, S4 complete (see the rows above), including the controller-ruled
   node type-check restoration (85a3558). Tree clean on branch `pydantic_v2` (HEAD = the commit of this
   ledger update). Expected at rest — the S4 gate: `make test-full` = 2513 passed / 5 skipped /
   **0 failed, 0 xfailed** (~2 min); `.venv/bin/python -m pytest -q -o addopts="" -W error
   tests/pipelime/utils/test_pydantic_compat.py` = 46 passed; `.venv/bin/python -m pytest -q -o
   addopts="" tests/pipelime/choixe` = 449 passed (442 + 7 new regression tests for
   `Node.__post_init__`); `grep -rn "pydantic.v1" pipelime` → only the guarded import in
   `pydantic_compat.py` and the error-message text at `choixe/visitors/processor.py:223` (fine: a
   string, not an import) — `choixe/ast/nodes.py` no longer imports pydantic at all.
2. **Next:** S5 — `docs/superpowers/plans/2026-09-13-pydantic-v2-s5-docs-deps-release.md`. Diff base
   for the S5 review: the HEAD of this ledger update.
3. **Items for S5 (migration guide / cleanup), carried from S2b and S3:** `int | str` given `"5"` keeps
   `"5"` under v2 smart unions (spec-accepted; document); `NumpyType.create(arr)` keeps the ndarray by
   identity on every path (v1 copied on the `validate` path); bools are coerced to `str` fields
   (`"True"`) as v1 did — a model-level `ConfigDict(strict=True)` is not honoured by that hook (docstring
   wording, S5-T4); `Path` fields given a bool are rejected (v1 parity); help/TUI render constrained types
   by their base type (`PositiveInt` → `int`, `Annotated[...]` metadata stripped) and TUI shows
   `Optional[T]` for `x: T = None`; raw `pydantic.Field(piper_port=...)` (the compat-policy form, e.g.
   `tests/sample_data/cli/ckpt_dag.py`) emits `PydanticDeprecatedSince20` — S5-T4 (warn-free) must decide
   whether pipelime suppresses it or the guide documents `pipelime.piper.Field`. Deprecated-but-working
   forms in tests (`parse_obj`, `X(__root__=...)`, `.__root__`) are left for the S5-T4 warn-free pass.
   `show_field_alias_valerr` is kept as a public alias of `format_validation_error` (controller ruling).
   From S4 (task review): token-form choixe type errors surface as the generic "does not validate…"
   `ChoixeParsingError` (`parser.py:332` drops the `TypeError` detail — document, or chain the message);
   direct node construction with a wrong type raises `TypeError` (v1: `pydantic.v1.ValidationError`, a
   `ValueError`) and v1's tuple/dict → node coercion is gone; `$cmd` processing leaks a still-running
   subprocess (`ResourceWarning` under `-W error`, `test_processor.py::test_cmd`, pre-existing).
4. **Deferred minors for the final whole-branch review (after S5):** (S4) `test_nodes.py` `test_custom_init_nodes_stay_unchecked` proves nothing (only container fields; use `RandNode(..., n=<non-Node>)` or drop it) and no test pins the `Union` branch (`InstanceNode(args=ListNode())`) or `None` on a required field; `FieldView.extra` is `{}` when
   `json_schema_extra` is callable (pipelime `Field` merges flags into a callable when the user passes one)
   → help shows such a port as PARAMETER; `FieldView.root_type` does not strip `Optional`;
   `strip_annotated` rebuilds `collections.abc.Callable[[P], R]` as a plain `GenericAlias` (brackets lost,
   display only) — return as is when `type(tp) is not types.GenericAlias`; the help *signature* line
   (`inspect.formatannotation`) still prints nested `Annotated[...]`; pre-existing: bare `t.List` renders
   `[, ...]`, a string forward-ref annotation renders `str`. The full list of rulings and deferred minors
   of every subtask is in the SDD ledger (next item).
5. **How:** invoke `superpowers:subagent-driven-development` on
   `docs/superpowers/plans/2026-09-13-pydantic-v2-migration.md`. Its git-ignored workspace
   `.superpowers/sdd/2026-09-13-pydantic-v2-migration/` (if still on disk) holds the SDD ledger
   `progress.md` with every ruling, the briefs (`brief.sh PLAN LABEL` extracts one task by its `S<k>-T<n>`
   label from the sub-plan), reports and review packages. If that directory is gone, recreate the ledger
   from this file.
6. **Process facts learned:** subagents died on API session limits four times (S0–S2b); the ledger +
   per-task commits recovered every time — commit per task, write the report incrementally. Reviewers
   found real gaps in every subtask so far (S3: Rich markup swallowing/replacing the `ValidationError`,
   raw `Annotated[...]` in help — both inherited from the plan's own code); never skip the task review, and
   dispatch reviews of toolkit changes on the most capable model.
