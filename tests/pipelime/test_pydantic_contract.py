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


# --- compact forms (spec §4.5) -------------------------------------------------
from pipelime.commands.interfaces import (
    ExtendedInterval,
    GrabberInterface,
    InputDatasetInterface,
    Interval,
    OutputDatasetInterface,
    OutputValueInterface,
)
from pipelime.commands.split_ops import AbsoluteSplit, PercSplit, Splits


class TestCompactForms:
    def test_grabber(self):
        H = make_model("H", g=(GrabberInterface, GrabberInterface.pyd_field()))
        assert dump(H(g="4,3").g) == {
            "num_workers": 4,
            "prefetch": 3,
            "allow_nested_mp": False,
        }
        assert H(g=2).g.num_workers == 2
        assert H(g="4,3,true").g.allow_nested_mp is True
        assert H().g.num_workers == 0
        g0 = GrabberInterface(num_workers=9)
        assert H(g=g0).g is g0  # instance pass-through keeps identity
        with pytest.raises(pyd.ValidationError):
            H(g="abc")
        with pytest.raises(pyd.ValidationError):
            H(g={"bad": 1})  # extra="forbid" still enforced

    def test_input_dataset(self, tmp_path: Path):
        H = make_model("H", i=(InputDatasetInterface, ...))
        inp = H(i=f"{tmp_path},true").i
        assert inp.folder == tmp_path.resolve() and inp.skip_empty is True
        assert H(i=str(tmp_path)).i.skip_empty is False
        assert H(i={"folder": str(tmp_path)}).i.folder == tmp_path.resolve()
        with pytest.raises(pyd.ValidationError):
            H(i=f"{tmp_path},maybe")

    def test_output_dataset(self, tmp_path: Path):
        H = make_model("H", o=(OutputDatasetInterface, ...))
        out = H(o=f"{tmp_path / 'new'},true,true").o
        assert out.folder == (tmp_path / "new").resolve()
        assert out.exists_ok is True
        assert out.serialization.override == {"DEEP_COPY": None}

    def test_intervals(self):
        H = make_model("H", a=(Interval, ...), b=(ExtendedInterval, ...))
        h = H(a="2:5", b="1:9:2")
        assert (h.a.start, h.a.stop) == (2, 5)
        assert (h.b.start, h.b.stop, h.b.step) == (1, 9, 2)
        assert H(a=3, b=[1, 2]).a.start == 3
        assert H(a=":4", b={"stop": 2}).a.stop == 4
        with pytest.raises(pyd.ValidationError):
            H(a="1:2:3", b=1)

    def test_output_value(self, tmp_path: Path):
        H = make_model("H", v=(OutputValueInterface[int], ...))
        v = H(v=f"{tmp_path / 'value.json'},true").v
        assert v.exists_ok is True and v.file == (tmp_path / "value.json").resolve()

    def test_splits_union_resolution(self, tmp_path: Path):
        """Union[AbsoluteSplit, PercSplit, Sequence[...]] must resolve as in v1."""
        H = make_model("H", s=(Splits.any_split_t, ...))
        expected = {
            "1": (AbsoluteSplit, 1),
            1: (AbsoluteSplit, 1),
            0.5: (PercSplit, 0.5),
            "0.5": (PercSplit, 0.5),
            "none": (AbsoluteSplit, None),
            "10": (AbsoluteSplit, 10),
        }
        for value, (cls, size) in expected.items():
            s = H(s=value).s
            assert type(s) is cls, value
            assert (s.length if cls is AbsoluteSplit else s.fraction) == size
        s = H(s=f"1,{tmp_path / 'abs_out'}").s
        assert type(s) is AbsoluteSplit and s.output.folder == (
            tmp_path / "abs_out"
        ).resolve()
        s = H(s=f"0.3,{tmp_path / 'perc_out'}").s
        assert type(s) is PercSplit and s.output.folder == (
            tmp_path / "perc_out"
        ).resolve()
        assert [type(x) for x in H(s=[0.5, "0.5"]).s] == [PercSplit, PercSplit]
        assert type(H(s={"fraction": 0.2}).s) is PercSplit
