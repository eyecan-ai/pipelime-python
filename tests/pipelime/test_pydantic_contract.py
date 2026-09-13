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


# --- root value wrappers (spec §3.2, §4.1) --------------------------------------
class TestRootWrappers:
    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_numpy_type_v1_surface(self):
        arr = np.array([[1, 2], [3, 4]])
        nt = plt.NumpyType(__root__=arr)  # v1 construction spelling
        assert nt.__root__ is arr and nt.value is arr
        assert nt.dict() == {"__root__": {"object": [[1, 2], [3, 4]], "dtype": "int64"}}
        nt2 = plt.NumpyType.create([1, 2, 3])
        assert nt2.value.tolist() == [1, 2, 3]
        nt3 = plt.NumpyType.create({"object": [1, 2], "dtype": "float32"})
        assert nt3.value.dtype == np.float32
        assert plt.NumpyType.create(nt) is nt

    def test_numpy_type_nested(self):
        H = make_model(
            "H", t=(plt.NumpyType, Field(default_factory=lambda: plt.NumpyType.create([0])))
        )
        h = H(t=[[1, 2], [3, 4]])
        d = dump(h)
        assert d == {"t": {"object": [[1, 2], [3, 4]], "dtype": "int64"}}
        assert H(**d).t.value.tolist() == [[1, 2], [3, 4]]  # round trip
        assert H().t.value.tolist() == [0]
        nt = plt.NumpyType.create([7])
        assert H(t=nt).t is nt

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_yaml_input(self, tmp_path: Path):
        yi = plt.YamlInput.create({"a": [1, 2]})
        assert yi.value == {"a": [1, 2]} and yi.dict() == {"__root__": {"a": [1, 2]}}
        f = tmp_path / "cfg.yaml"
        f.write_text("a:\n  b: 3\n")
        assert plt.YamlInput.create(f"{f}:a.b").value == 3
        assert plt.YamlInput.create(str(f)).value == {"a": {"b": 3}}
        H = make_model("H", c=(plt.YamlInput, ...))
        assert dump(H(c=[1, True, "s"])) == {"c": [1, True, "s"]}
        # a required, non-Optional field rejects an explicit None before the
        # custom __root__ validator ever runs (pydantic v1 field-level None guard);
        # only `YamlInput.create(None)`/`.validate(None)` (no enclosing field) can
        # build the None-valued wrapper.
        # v1 rejects an explicit None for a *required* wrapper field before the type's
        # validators run; the v2 design accepts it as `YamlInput(None)` (documented
        # difference, design ruling in the migration ledger)
        if V1:
            with pytest.raises(pyd.ValidationError):
                H(c=None)
        else:
            assert H(c=None).c.value is None

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_type_def_and_item_type(self):
        it = plt.ItemType.create("ImageItem")
        assert it.value is pli.ImageItem
        assert plt.ItemType.create(pli.PngImageItem).value is pli.PngImageItem
        assert plt.ItemType(__root__=pli.ImageItem).__root__ is pli.ImageItem
        # `_type_to_string` only strips the `pipelime.items.` default class path
        # when doing so leaves no further dot; ImageItem/PngImageItem live in the
        # `pipelime.items.image_item` submodule, so the reduced name still has a
        # dot in it and the full dotted path is kept instead.
        assert it.dict() == {"__root__": "pipelime.items.image_item.ImageItem"}
        assert str(it) == "pipelime.items.image_item.ImageItem"
        H = make_model("H", tp=(plt.ItemType, ...))
        assert dump(H(tp="PngImageItem")) == {
            "tp": "pipelime.items.image_item.PngImageItem"
        }
        assert H(tp=f"{MODULE}.ContractItem").tp.value is ContractItem
        assert dump(H(tp=f"{MODULE}.ContractItem")) == {"tp": f"{MODULE}.ContractItem"}
        # `import_symbol` raises a bare `ImportError` on failure; pydantic v1 only
        # auto-wraps `ValueError`/`TypeError`/`AssertionError` from validators into
        # `ValidationError`, and `TypeDef._string_to_type` does not catch it, so an
        # unresolvable class path propagates unwrapped.
        with pytest.raises(ImportError):
            H(tp="not.an.Item")
        assert hash(plt.ItemType.create("ImageItem")) == hash(plt.ItemType.create("ImageItem"))
        assert plt.ItemType.wrapped_type() is pli.Item

    @pytest.mark.filterwarnings("ignore::DeprecationWarning")
    def test_callable_def(self):
        cd = plt.CallableDef.create(contract_identity)
        assert cd.value is contract_identity and cd(3) == 3
        assert cd.dict() == {"__root__": f"{__name__}.contract_identity"}
        H = make_model("H", fn=(plt.CallableDef, ...))
        # a field typed `CallableDef` holds the wrapper, not the raw callable
        # (same `.value` accessor as `ItemType`/`TypeDef` above); only the
        # top-level `CallableDef.create(...)` result exposes `.value is <fn>`.
        assert H(fn=f"{MODULE}.contract_identity").fn.value is contract_identity
        assert H(fn=f"{THIS_FILE}:contract_identity").fn(5) == 5  # file-path form
        assert H(fn="lambda x: x + 1").fn(1) == 2
        assert H(fn={f"{MODULE}.ContractCallable": [10]}).fn(1) == 11
        assert cd.args_type == [None] and cd.has_var_positional is False
        with pytest.raises(pyd.ValidationError):
            H(fn=42)


# `test_callable_def` also imports `contract_identity` via `THIS_FILE:...`
# (file-path form), which makes `import_symbol` load *this same file* a
# second time under a bare-stem module name (see `import_module_from_file`);
# that re-executes every top-level statement, including this class body.
# `ItemFactory` (pipelime/items/base.py) registers file extensions in a
# process-global dict at class-creation time and raises if an extension is
# already taken, so the second execution collides with the first (normal,
# package-qualified) import of this module. Reuse the already-registered
# class instead of letting that collision propagate.
if ".contract" in type(pli.Item).ITEM_CLASSES:
    ContractItem = type(pli.Item).ITEM_CLASSES[".contract"]
else:

    class ContractItem(pli.Item):
        """Item subclass used by the TypeDef contracts (never instantiated)."""

        @classmethod
        def file_extensions(cls):  # pragma: no cover
            return [".contract"]

        @classmethod
        def decode(cls, fp):  # pragma: no cover
            return fp.read()

        @classmethod
        def encode(cls, value, fp):  # pragma: no cover
            fp.write(value)


class ContractCallable:
    def __init__(self, offset: int):
        self.offset = offset

    def __call__(self, x):
        return x + self.offset


# --- v1 Optional / None-default semantics (spec §3.1.2) ------------------------
from pipelime.piper import PipelimeCommand, PiperPortType
from pipelime.sequences import SamplesSequence, build_pipe
from pipelime.sequences.pipes import PipedSequenceBase
import pipelime.sequences as pls
from pipelime.stages import (
    SampleStage,
    StageCompose,
    StageIdentity,
    StageInput,
    StageLambda,
)
from pipelime.stages.entities import BaseEntity


class OptCommand(PipelimeCommand, title="contract-opt"):
    a: t.Optional[int]
    b: int = None  # type: ignore[assignment]
    c: t.Optional[str] = Field(None, description="c")
    d: t.Optional[float]  # all annotations are strings here (`from __future__ import annotations`)

    def run(self) -> None:
        pass


class OptStage(SampleStage, title="contract-opt-stage"):
    a: t.Optional[int]
    b: int = None  # type: ignore[assignment]

    def __call__(self, x):
        return x


class OptEntity(BaseEntity):
    label: t.Optional[pli.NumpyItem]
    image: pli.ImageItem = None  # type: ignore[assignment]


class TestV1OptionalSemantics:
    def test_command(self):
        c = OptCommand()
        assert (c.a, c.b, c.c, c.d) == (None, None, None, None)
        assert OptCommand(a=None, b=None, d=None).b is None
        assert OptCommand(a=1, b=2).b == 2
        with pytest.raises(pyd.ValidationError):
            OptCommand(b="x")

    def test_stage(self):
        assert OptStage().a is None and OptStage(b=None).b is None

    def test_entity(self):
        e = OptEntity()
        assert e.label is None and e.image is None
        e = OptEntity(label=[1, 2])
        assert isinstance(e.label, pli.NumpyItem)
        assert dump(e) == {"label": e.label}  # None fields are skipped in dumps


# --- polymorphic nested serialization (spec §3.1.1) ----------------------------
class PolyHost(pyd.BaseModel):
    stage: SampleStage
    stages: t.List[SampleStage] = []
    cmd: t.Optional[PipelimeCommand] = None


class TestPolymorphicDumps:
    def test_stage_in_plain_model(self):
        h = PolyHost(stage=StageCompose([StageIdentity()]), stages=[OptStage(a=3)])
        d = dump(h)
        assert d["stage"] == {"stages": [{"identity": {}}]}
        assert d["stages"] == [{"a": 3, "b": None}]

    def test_stage_input_dump(self):
        # `StageInput.validate` resolves bare stage names (here "compose"/"identity")
        # through `PipelimeSymbolsHelper`, which lazily scans every module it has ever
        # been told about (`std_modules` plus any `extra_modules` registered by a
        # dynamic import elsewhere) for `SampleStage`/`PipelimeCommand` subclasses and
        # raises on any two carrying the same title. `test_type_def_and_item_type`/
        # `test_callable_def` above register *this* module twice under different
        # names (its dotted path, and its file path — the latter via the `THIS_FILE:`
        # form, which reimports the file under a bare module name and so re-executes
        # every top-level class statement, including `OptCommand`/`OptStage`); once
        # both are registered, the module-level `OptCommand`/`OptStage` fixtures
        # collide with their own re-executed copies purely by title, unrelated to the
        # "compose"/"identity" lookup this test actually needs. Reset the registry to
        # just the std modules first, the same isolation idiom already used by
        # `test_tui.py`/`test_command_decorator.py` (`set_extra_modules`), so this
        # lookup only ever sees pipelime's own built-in stages.
        from pipelime.cli.utils import PipelimeSymbolsHelper

        PipelimeSymbolsHelper.set_extra_modules([])
        si = StageInput.validate({"compose": {"stages": ["identity", "identity"]}})
        assert dump(si) == {"compose": {"stages": [{"identity": {}}, {"identity": {}}]}}
        assert isinstance(si.__root__, StageCompose)

    def test_command_in_plain_model(self):
        h = PolyHost(stage=StageIdentity(), cmd=OptCommand(a=4))
        assert dump(h)["cmd"] == {"a": 4, "b": None, "c": None, "d": None}
        assert dump(h, exclude_none=True)["cmd"] == {"a": 4}

    def test_sequence_iteration_yields_samples(self):
        seq = SamplesSequence.toy_dataset(2)
        assert all(isinstance(x, pls.Sample) for x in seq)


# --- command framework (spec §4.2) ---------------------------------------------
from pipelime.piper import command
from pipelime.piper.model import LazyCommand, NodesDefinition


class PortsCommand(PipelimeCommand, title="contract-ports"):
    inp: int = Field(1, alias="i", piper_port=PiperPortType.INPUT)
    out: int = Field(2, alias="o", piper_port=PiperPortType.OUTPUT)
    prm: int = Field(3)

    def run(self) -> None:
        pass


class TestCommandFramework:
    def test_piper_ports(self):
        c = PortsCommand()
        assert c.get_inputs() == {"inp": 1} and c.get_outputs() == {"out": 2}
        assert PortsCommand(i=5, o=6).inp == 5  # by alias
        assert PortsCommand(inp=5, out=6).out == 6  # by name (populate_by_name)
        assert PortsCommand.command_title() == "contract-ports"
        assert PortsCommand().command_name == "contract-ports"

    def test_raw_pydantic_field_with_flags(self):
        """`pydantic.Field(piper_port=...)` (the pre-3.0 spelling) keeps working;
        on v2 pydantic emits a deprecation warning at class creation, which is
        expected and documented in the migration guide."""
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)

            class RawPortsCommand(PipelimeCommand, title="contract-raw-ports"):
                inp: int = pyd.Field(1, piper_port=PiperPortType.INPUT)
                out: int = pyd.Field(2, piper_port=PiperPortType.OUTPUT)

                def run(self) -> None:
                    pass

        assert RawPortsCommand().get_inputs() == {"inp": 1}
        assert RawPortsCommand().get_outputs() == {"out": 2}

    def test_numbers_coerced_to_str(self):
        """v1 coerced numbers to `str` fields (CLI values are parsed before validation)."""
        assert OptCommand(c=5).c == "5"
        assert OptCommand(c=2.5).c == "2.5"

    def test_dump_by_alias(self):
        assert dump(PortsCommand(), by_alias=True) == {"i": 1, "o": 2, "prm": 3}
        assert dump(PortsCommand()) == {"inp": 1, "out": 2, "prm": 3}

    def test_extra_forbidden(self):
        with pytest.raises(pyd.ValidationError):
            PortsCommand(nope=1)

    def test_class_kwargs(self):
        class GcCommand(PipelimeCommand, title="gc", force_gc=True, no_default_checkpoint=True):
            def run(self) -> None:
                pass

        assert GcCommand._force_gc is True
        assert GcCommand.save_to_default_checkpoint() is False
        assert PortsCommand.save_to_default_checkpoint() is True

    def test_command_decorator_signature(self):
        calls = []

        @command(title="contract-fn")
        def fn(a: int, b: str = "x", *args: int, c: float = 1.0, **kw: int):
            calls.append((a, b, args, c, kw))

        cmd = fn(1, "y", 2, 3, c=2.5, z=7)
        assert cmd.command_title() == "contract-fn"
        assert (cmd.a, cmd.b, cmd.args, cmd.c) == (1, "y", (2, 3), 2.5)
        import inspect

        params = list(inspect.signature(fn).parameters)
        assert params == ["a", "b", "args", "c", "kw"]
        with pytest.raises(TypeError):
            fn(1, 2, 3, args=(4,))  # var-positional not allowed as keyword
        with pytest.raises(pyd.ValidationError):
            fn("notanint")

    def test_command_decorator_unannotated(self):
        @command
        def fn(a=1, b="s", c=None):
            pass

        cmd = fn()
        assert (cmd.a, cmd.b, cmd.c) == (1, "s", None)
        assert fn(a=2).a == 2
        with pytest.raises(pyd.ValidationError):
            fn(a="x")  # v1 inferred `int` from the default

    @pytest.mark.xfail(V1, reason="spec §4.2 bug fix: **kwargs expanded with **", strict=True)
    def test_command_decorator_var_keyword_expansion(self):
        seen = {}

        @command
        def fn(a: int, **kw: int):
            seen.update(kw)

        fn(1, x=2, y=3)()
        assert seen == {"x": 2, "y": 3}

    def test_lazy_command(self):
        lc = PortsCommand.lazy()(inp=9)
        assert isinstance(lc, LazyCommand)
        assert lc.inp == 9
        assert lc.out == 2  # declared default
        assert lc.command_title() == "contract-ports"
        lc.prm = 5
        real = lc()
        assert isinstance(real, PortsCommand) and (real.inp, real.prm) == (9, 5)
        with pytest.raises(AttributeError):
            lc.nope

    def test_nodes_definition_dump_and_validate(self):
        # Resolving a `f"{MODULE}...."` node here goes through `get_pipelime_command`,
        # which (like `StageInput.validate` in `test_stage_input_dump` above) always
        # triggers a `PipelimeSymbolsHelper` scan first, and then registers this module
        # as an "extra module" as a side effect of the dotted-path lookup itself. This
        # module also imports several already-titled `SampleStage` classes at top level
        # (`StageCompose`, `StageIdentity`, ...) for use as fixtures, so once it is
        # registered, the *next* scan finds them a second time and collides with
        # pipelime's own `pipelime.stages` definitions, unrelated to the "contract-
        # ports" node this test actually resolves. Reset before each lookup (the
        # registration happens again after each one) so it only ever sees pipelime's
        # own built-in commands/stages; see `test_stage_input_dump` for the mechanism.
        from pipelime.cli.utils import PipelimeSymbolsHelper

        PipelimeSymbolsHelper.set_extra_modules([])
        nodes = NodesDefinition.create(
            {"n1": {f"{MODULE}.PortsCommand": {"i": 4}}, "n2": PortsCommand(o=8)}
        )
        assert isinstance(nodes.value["n1"], PortsCommand)
        d = nodes.dict()  # v1 envelope on a root wrapper
        assert d == {
            "__root__": {
                "n1": {"contract-ports": {"inp": 4, "out": 2, "prm": 3}},
                "n2": {"contract-ports": {"inp": 1, "out": 8, "prm": 3}},
            }
        }
        assert dump(nodes, by_alias=True)["__root__" if V1 else "n1"] is not None
        H = make_model("H", nodes=(NodesDefinition, ...))
        PipelimeSymbolsHelper.set_extra_modules([])
        h = H(nodes={"n": {f"{MODULE}.PortsCommand": {}}})
        assert isinstance(h.nodes.value["n"], PortsCommand)
        assert dump(h) == {"nodes": {"n": {"contract-ports": {"inp": 1, "out": 2, "prm": 3}}}}
        assert dump(h, by_alias=True) == {"nodes": {"n": {"contract-ports": {"i": 1, "o": 2, "prm": 3}}}}
