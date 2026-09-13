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
| S1 | S1-T1 (PipelimeModel) | todo | | | | |
| S1 | S1-T2 (PipelimeRootModel) | todo | | | | |
| S1 | S1-T3 (Field wrapper) | todo | | | | |
| S1 | S1-T4 (introspection) | todo | | | | |
| S1 | S1-T5 (pydantic_types: NewPath) | todo | | | | |
| S1 | S1-T6 (NumpyType/YamlInput) | todo | | | | |
| S1 | S1-T7 (TypeDef/CallableDef) | todo | | | | |
| S1 | S1-T8 (validation interfaces) | todo | | | | |
| S2a | S2a-T1 (stages/base) | todo | | | | |
| S2a | S2a-T2 (stages/entities) | todo | | | | |
| S2a | S2a-T3 (other stages) | todo | | | | |
| S2a | S2a-T4 (samples_sequence) | todo | | | | |
| S2a | S2a-T5 (pipes/sources/utils/grabber) | todo | | | | |
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
- (S0) Contract fixtures must not define `Item` subclasses (global item registry) nor rely on v1 mapping unannotated `None` defaults to `Any`; the contract module isolates the pipelime symbol registry per test (`_clean_registry()` + autouse fixture) because `import_symbol` registers every loaded module as an extra module and re-exported stages then count as duplicates (pre-existing quirk, parked).
- (S0) `CallableDef.args_type` will resolve string annotations in S1-T7 (modules with `from __future__ import annotations` crashed `EntityAction` inference on v1).
- S0-T1: per-file xdist grouping was dropped in favour of a per-worker
  isolated pipelime user dir (see the plan's S0-T1 Step 2) — per-file groups
  serialised the two largest test files onto one worker each and made the
  parallel run slower than serial.
