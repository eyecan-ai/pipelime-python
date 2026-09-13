"""Contract tests for the pydantic v1 → v2 migration (design spec §5.2).

Every test here pins a behaviour that downstream code relies on and that the
existing suite does not target directly. They must pass on the pydantic.v1 code
(before the migration) AND on the native v2 code (after), so the pydantic API is
imported through the dual block below. `V1` is True until subtask 1 lands.

Tests marked `xfail(V1, strict=True)` pin *new* behaviour (modern type hints in
the TUI, bug fixes) and are expected to fail before the migration.
"""
from __future__ import annotations

import pickle
import typing as t
from pathlib import Path

import numpy as np
import pytest

import pipelime.items as pli
import pipelime.utils.pydantic_types as plt

# --- dual pydantic import ----------------------------------------------------
V1 = not hasattr(plt.NumpyType, "model_dump")
if V1:  # before subtask 1
    import pydantic.v1 as pyd

    Field = pyd.Field
else:  # after subtask 1
    import pydantic as pyd
    from pipelime.piper import Field  # type: ignore[no-redef]

THIS_FILE = Path(__file__).resolve().as_posix()
MODULE = __name__  # `tests.pipelime.test_pydantic_contract` — use this for symbols whose
# *identity* matters: importing through `THIS_FILE` re-executes the module and yields
# different class/function objects.


def dump(model, **kwargs) -> t.Any:
    """`model_dump` on v2, `.dict()` on v1 — same semantics except the top-level
    root-model envelope, which has its own tests below."""
    if hasattr(model, "model_dump"):
        return model.model_dump(**kwargs)
    return model.dict(**kwargs)


def parse_as(tp, value):
    """Validate `value` as type `tp` (v1 `parse_obj_as`, v2 `TypeAdapter`)."""
    if V1:
        return pyd.parse_obj_as(tp, value)
    return pyd.TypeAdapter(tp).validate_python(value)


def make_model(name: str, **fields):
    """A plain pydantic model with the given `name=(type, default)` fields."""
    return pyd.create_model(name, **fields)


def contract_identity(x):
    return x


def contract_action(x):
    """Entity action used by the StageEntity contracts (importable by class path)."""
    return x
