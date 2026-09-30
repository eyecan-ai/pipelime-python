# S4 — Choixe AST on stdlib dataclasses

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the last `pydantic.v1` import under `pipelime/` (outside the toolkit's guarded one): the choixe AST nodes move from `pydantic.v1.dataclasses.dataclass` to the standard library `dataclasses.dataclass`.

**Architecture:** The 23 decorators keep their exact `init=`/`eq=`/`unsafe_hash=` arguments; the parser already builds nodes with correctly typed values, and no code relies on dataclass validation (verified: the parser passes `Node` instances and `(case, body)` tuples). `HashNode.__hash__` hashes `self.__dict__.values()`, which under stdlib dataclasses contains only the fields (pydantic v1 added `__pydantic_initialised__`), so hashes change value but keep their semantics.

**Tech Stack:** stdlib `dataclasses`.

**Spec:** `docs/superpowers/specs/2026-09-12-pydantic-v2-migration-design.md` §4.7 (the decoder/`$model` lines were done in S2b-T8).

## Global Constraints

- `pipelime/choixe/ast/nodes.py` must not import pydantic at all afterwards.
- The choixe test suite (`tests/pipelime/choixe`, 442 tests, fast) is the oracle; no test edits are expected.

---

### Task S4-T1: `choixe/ast/nodes.py`

**Files:**
- Modify: `pipelime/choixe/ast/nodes.py:4-7`

- [ ] **Step 1: Swap the decorator import**

```python
from dataclasses import dataclass, field
```
replacing both `from dataclasses import field` and `from pydantic.v1.dataclasses import dataclass`. No other line changes.

- [ ] **Step 2: Run the choixe suite**

Run: `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/choixe`
Expected: passed. If a test fails on node *equality*, check that the class in question defines `__eq__` itself (`HashNode`) or uses `eq=False` exactly as before — stdlib and pydantic dataclasses honour these flags identically; if a test fails on *hash* values compared against literals, update the literal and record it in `tests/TEST_CHANGES.md` as "hash includes only fields now".

- [ ] **Step 3: Leftover grep**

Run: `grep -rn "pydantic.v1" pipelime`
Expected: only the `try:` block in `pipelime/utils/pydantic_compat.py`.

- [ ] **Step 4: Commit**

```bash
git add pipelime/choixe/ast/nodes.py
git commit -m "refactor(choixe): AST nodes on stdlib dataclasses"
```

---

### Task S4-T2: Tier 2 gate

- [ ] **Step 1: Run** `make test-full 2>&1 | tail -5`
Expected: green, same counts as the S3 gate.

- [ ] **Step 2: Ledger** — record the S4 gate. Then open `2026-09-13-pydantic-v2-s5-docs-deps-release.md`.
