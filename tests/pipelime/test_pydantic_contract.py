"""Contract tests for the pydantic v1 → v2 migration (design spec §5.2).

Every test here pins a behaviour that downstream code relies on and that the
existing suite does not target directly. They were written against pipelime 2.x
(the `pydantic.v1` code) and ran on both sides of the migration; the pydantic.v1
branch was dropped in S5. Where pipelime 3 deliberately differs from 2.x, a
comment starting with "2.x:" records the old behaviour (see
docs/migration/pydantic_v2.md).
"""
from __future__ import annotations

import inspect
import os
import pickle
import typing as t
from pathlib import Path

import numpy as np
import pydantic as pyd
import pytest

import pipelime.items as pli
import pipelime.utils.pydantic_types as plt
from pipelime.piper import Field

THIS_FILE = Path(__file__).resolve().as_posix()
MODULE = __name__  # `tests.pipelime.test_pydantic_contract` — use this for symbols whose
# *identity* matters: importing through `THIS_FILE` re-executes the module and yields
# different class/function objects.


def dump(model, **kwargs) -> t.Any:
    """`model_dump` (2.x: `.dict()` — same semantics except the top-level root-model
    envelope, which has its own tests below)."""
    return model.model_dump(**kwargs)


def parse_as(tp, value):
    """Validate `value` as type `tp` (2.x: `parse_obj_as`)."""
    return pyd.TypeAdapter(tp).validate_python(value)


def make_model(name: str, **fields):
    """A plain pydantic model with the given `name=(type, default)` fields."""
    return pyd.create_model(name, **fields)


def contract_identity(x):
    return x


def contract_action(x):
    """Entity action used by the StageEntity contracts (importable by class path)."""
    return x


def _clean_registry() -> None:
    """Forget the extra modules registered by class-path imports.

    `import_symbol` registers every module it loads as a pipelime "extra module";
    this test module re-exports pipelime stages at top level, so a later
    registry scan (stage-by-name / command-title lookups) would report them as
    duplicates. Call this right before such a lookup when the same test resolved a
    `MODULE`-qualified symbol earlier.
    """
    from pipelime.cli.utils import PipelimeSymbolsHelper

    PipelimeSymbolsHelper.set_extra_modules([])


@pytest.fixture(autouse=True)
def _isolated_registry():
    _clean_registry()
    yield
    _clean_registry()


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
        # an explicit None for a *required* wrapper field is accepted as
        # `YamlInput(None)` (documented difference, design ruling in the migration
        # ledger). 2.x: rejected by the pydantic v1 field-level None guard before the
        # type's validators ran; only `YamlInput.create(None)` built that wrapper.
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
        H2 = make_model("H2", tp=(ContractTypeDef, ...))
        assert H2(tp="ContractSub").tp.value is ContractSub  # default class path applied
        assert H2(tp=f"{MODULE}.ContractSub").tp.value is ContractSub
        assert dump(H2(tp=ContractSub)) == {"tp": "ContractSub"}  # reduced through default_class_path
        assert ContractTypeDef.wrapped_type() is ContractBase
        with pytest.raises(pyd.ValidationError):
            H2(tp=f"{MODULE}.ContractCallable")  # not a ContractBase subclass
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


class ContractBase:
    """Base of a user-defined type hierarchy for the TypeDef contracts (not an Item:
    defining an Item subclass would register it in pipelime's global item registry
    and change what the CLI enumerates for `--data-cache "*"`)."""


class ContractSub(ContractBase):
    pass


class ContractTypeDef(plt.TypeDef[ContractBase]):
    @classmethod
    def default_class_path(cls) -> str:
        return f"{MODULE}."


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


# `Optional[X] = Field(<no default>, ...)`: optional in 2.x (pydantic.v1 gave it `None`)
class OptFieldCommand(PipelimeCommand, title="contract-opt-field"):
    a: t.Optional[int] = Field(description="a")
    b: t.Optional[str] = Field(alias="bb", piper_port=PiperPortType.PARAMETER)
    c: t.Optional[int] = Field(..., description="`...` is still required")

    def run(self) -> None:
        pass


class OptFieldStage(SampleStage, title="contract-opt-field-stage"):
    a: t.Optional[int] = Field(description="a")

    def __call__(self, x):
        return x


class OptFieldPipe(PipedSequenceBase, title="contract_opt_field_pipe"):
    # not registered as a pipe: built directly
    a: t.Optional[int] = Field(description="a")

    def size(self) -> int:
        return self.source.size()

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source.get_sample(idx)


class TestV1OptionalSemantics:
    def test_optional_field_without_default(self):
        c = OptFieldCommand(c=None)
        assert (c.a, c.b, c.c) == (None, None, None)
        assert OptFieldCommand(a=1, bb="x", c=2).b == "x"
        with pytest.raises(pyd.ValidationError):
            OptFieldCommand()  # `c` is `Field(...)`: required, as in 2.x
        assert OptFieldStage().a is None
        seq = OptFieldPipe(source=SamplesSequence.toy_dataset(2))
        assert seq.a is None and len(seq) == 2

    def test_command_decorator_optional_field_without_default(self):
        @command
        def contract_opt_fn(
            a: t.Optional[int] = Field(description="a"), b: t.Optional[int] = None
        ):
            pass

        cmd = contract_opt_fn()
        assert (cmd.a, cmd.b) == (None, None)
        assert contract_opt_fn(a=2).a == 2

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
        # through `PipelimeSymbolsHelper`, which would otherwise report this module's
        # top-level `StageCompose`/`StageIdentity` re-exports as duplicates of
        # pipelime's own (see `_clean_registry`).
        _clean_registry()
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
        def fn(a=1, b="s"):
            pass

        cmd = fn()
        assert (cmd.a, cmd.b) == (1, "s")
        assert fn(a=2).a == 2
        with pytest.raises(pyd.ValidationError):
            fn(a="x")  # v1 inferred `int` from the default

    # 2.x: unannotated param with None default raised a v1 ConfigError (spec §4.2 → Any)
    def test_command_decorator_unannotated_none_default(self):
        @command
        def fn(a=1, c=None):
            pass

        assert (fn().a, fn().c) == (1, None)
        assert fn(c="anything").c == "anything"

    # 2.x: **kwargs passed as a single dict keyword (spec §4.2 bug fix: expanded with **)
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

    def test_lazy_command_unset_required_field(self):
        """v1 `ModelField.get_default()` of a required field is `None` (not a sentinel)."""

        class ReqCommand(PipelimeCommand, title="contract-req"):
            x: int
            y: t.List[int] = Field(default_factory=list)

            def run(self) -> None:
                pass

        lc = ReqCommand.lazy()()
        assert lc.x is None
        assert lc.y == []

    def test_nodes_definition_dump_and_validate(self):
        # Resolving a `f"{MODULE}...."` node re-registers this module as an "extra
        # module" (see `_clean_registry`); reset before each lookup below, since the
        # registration happens again after each one.
        _clean_registry()
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
        assert dump(nodes, by_alias=True)["n1"] is not None  # 2.x: `.dict()` → "__root__"
        H = make_model("H", nodes=(NodesDefinition, ...))
        _clean_registry()
        h = H(nodes={"n": {f"{MODULE}.PortsCommand": {}}})
        assert isinstance(h.nodes.value["n"], PortsCommand)
        assert dump(h) == {"nodes": {"n": {"contract-ports": {"inp": 1, "out": 2, "prm": 3}}}}
        assert dump(h, by_alias=True) == {"nodes": {"n": {"contract-ports": {"i": 1, "o": 2, "prm": 3}}}}


# --- stages & entities (spec §4.3) ---------------------------------------------
from pipelime.stages import StageEntity
from pipelime.stages.entities import DynamicKey, EntityAction, ParsedData, ParsedItem


class ContractMeta(pyd.BaseModel):
    name: str


class ContractInput(BaseEntity):
    image: pli.ImageItem
    meta: ParsedItem[pli.MetadataItem, ContractMeta]
    _dyn = DynamicKey(pli.NumpyItem, [1, 2, 3])


class ContractOutput(BaseEntity):
    image: pli.ImageItem
    meta: ParsedData[ContractMeta]
    extra: t.Optional[pli.NumpyItem]


def annotated_action(x: ContractInput) -> ContractOutput:
    return ContractOutput.merge(x, meta=ContractMeta(name=x.meta().name + "!"), extra=None)


# This module's `from __future__ import annotations` stringifies this function's
# annotations too. Pydantic *model* fields resolve such forward-ref strings via
# `resolve_annotations`/the class's `__module__` globals at class-creation time (that
# is why `OptCommand`/`PortsCommand`/etc. above work fine); but `EntityAction`'s
# `input_type` inference goes through `CallableDef.args_type`, which reads
# `inspect.signature(...).parameters[...].annotation` on a plain callable with no
# forward-ref resolution at all, so it would receive the literal string
# `"ContractInput"` and crash inside `issubclass(BaseEntity, "ContractInput")`.
# Restore real objects to match how action callables are annotated in ordinary
# (non-`__future__`) modules, which is how `EntityAction`/`StageEntity` are used
# in practice.
annotated_action.__annotations__ = {"x": ContractInput, "return": ContractOutput}


def _sample() -> pls.Sample:
    return pls.Sample(
        {
            "image": pli.PngImageItem(np.zeros((4, 4, 3), dtype=np.uint8)),
            "meta": pli.JsonMetadataItem({"name": "n"}),
            "other": pli.NpyNumpyItem(np.array([9])),  # NumpyItem is abstract; use a concrete subclass
        }
    )


class TestStagesAndEntities:
    def test_stage_input_forms(self):
        assert isinstance(StageInput.validate("identity").__root__, StageIdentity)
        assert isinstance(StageInput.validate(StageIdentity()).__root__, StageIdentity)
        si = StageInput.validate({"compose": {"stages": ["identity"]}})
        assert isinstance(si.__root__, StageCompose)
        s = StageIdentity()
        assert StageInput.validate(StageInput.validate(s)).__root__ is s

    def test_stage_titles(self):
        from pipelime.utils.pydantic_types import CallableDef  # noqa: F401

        assert StageCompose.model_config.get("title") == "compose"  # 2.x: `__config__.title`
        lam = StageLambda(contract_identity)
        assert lam.func.value is contract_identity

    def test_entity_parsing_and_merge(self):
        x = _sample()
        e = ContractInput(**x)
        assert isinstance(e.meta, ParsedItem) and e.meta().name == "n"
        assert e.meta.raw_item is x["meta"]
        assert e.other is x["other"]  # extra="allow" forwards unknown items
        assert e._dyn.validate("other")() .tolist() == [9]
        out = annotated_action(e)
        assert out.meta().name == "n!" and out.extra is None
        d = dump(out)
        assert set(d) == {"image", "meta", "other"}  # None skipped, ParsedItem -> raw item
        assert isinstance(d["meta"], pli.MetadataItem) and d["meta"]() == {"name": "n!"}

    def test_entity_action_inference(self):
        ea = EntityAction(action=annotated_action)
        assert ea.input_type.value is ContractInput
        ea = EntityAction(action=contract_action)
        assert ea.input_type.value is BaseEntity
        with pytest.raises(pyd.ValidationError):
            EntityAction(action=lambda: None)  # needs one positional arg
        H = make_model("H", ea=(EntityAction, ...))
        assert H(ea=f"{MODULE}.annotated_action").ea.input_type.value is ContractInput
        assert H(ea={"action": f"{MODULE}.contract_action"}).ea.input_type.value is BaseEntity

    def test_stage_entity_call_shapes(self):
        ea = EntityAction(action=annotated_action)
        stages = [
            StageEntity(ea),
            StageEntity(__root__=ea),
            StageEntity({"action": annotated_action}),
            StageInput.validate({"entity": {"action": f"{MODULE}.annotated_action"}}).__root__,
        ]
        # the previous line resolved `f"{MODULE}.annotated_action"`, registering this
        # module as an extra module; the next by-name lookup of the "entity" stage
        # would otherwise rescan it and report `StageIdentity`/`StageCompose`/etc.
        # (re-exported at module top level) as duplicates of pipelime's own.
        _clean_registry()
        stages.append(
            StageInput.validate({"entity": f"{MODULE}.annotated_action"}).__root__
        )
        for st in stages:
            assert isinstance(st, StageEntity), st
            y = st(_sample())
            assert y["meta"]() == {"name": "n!"}
        # the `stages.append(StageInput.validate({"entity": f"{MODULE}.annotated_action"}))`
        # call above resolved `f"{MODULE}...."` again, re-registering this module;
        # clean up before another by-name lookup.
        _clean_registry()
        # config written by pipelime 2.x carries the `__root__` envelope
        st = StageInput.validate(
            {"entity": {"__root__": {"action": f"{MODULE}.annotated_action"}}}
        ).__root__
        assert isinstance(st, StageEntity)

    def test_stage_entity_dump_roundtrip(self):
        si = StageInput.validate({"entity": {"action": f"{MODULE}.annotated_action"}})
        d = dump(si)
        assert list(d) == ["entity"]
        spec = d["entity"]
        # 2.x: the v1 top-level `.dict()` of the inner root model kept the envelope,
        # `{"__root__": {"action": ..., "input_type": ...}}`
        assert spec == {"action": f"{MODULE}.annotated_action", "input_type": f"{__name__}.ContractInput"}
        # `si` resolved `f"{MODULE}.annotated_action"` above, registering this module;
        # clean up before the "entity" by-name lookup triggered by validating `d`.
        _clean_registry()
        again = StageInput.validate(d)
        assert isinstance(again.__root__, StageEntity)


# --- sequences (spec §4.4) ------------------------------------------------------
# `test_callable_def` above imports `contract_identity` via `THIS_FILE:...`
# (file-path form), which makes `import_symbol` load *this same file* a second
# time under a bare-stem module name (see `import_module_from_file`); that
# re-executes every top-level statement, including this class body. Letting
# `@pls.piped_sequence` re-run would re-register "contract_pipe" and rebind
# `SamplesSequence.contract_pipe` to that second-generation class, whose `__module__`
# is the bare stem rather than the dotted package path. A spawned multiprocessing
# worker (fresh interpreter) can no longer `import` that bare module to reconstruct
# pickled instances, so `seq.run(num_workers=...)` hangs waiting for results that
# never come back. Reuse the already-registered class instead.
if "contract_pipe" in SamplesSequence._pipes:
    ContractPipe = SamplesSequence._pipes["contract_pipe"]
else:

    @pls.piped_sequence
    class ContractPipe(PipedSequenceBase, title="contract_pipe"):
        keys: t.Sequence[str] = Field(default_factory=list)
        stage: StageInput = Field(
            default_factory=lambda: StageInput.validate("identity")
        )

        def size(self) -> int:
            return self.source.size()

        def get_sample(self, idx: int) -> pls.Sample:
            return self.stage(self.source.get_sample(idx))


class TestSequences:
    def test_piped_sequence_registration_and_pickle(self):
        seq = SamplesSequence.toy_dataset(3).contract_pipe(keys=["image"], stage="identity")
        assert seq.name() == "contract_pipe" and len(seq) == 3
        seq2 = pickle.loads(pickle.dumps(seq))
        assert len(seq2) == 3 and seq2.keys == ["image"]
        # multiprocessing path
        seq.run(num_workers=2, prefetch=1, track_fn=False)

    # 2.x: `to_pipe` recursed into `str` values (RecursionError; spec §4.4 bug fix)
    def test_to_pipe_roundtrip(self):
        seq = SamplesSequence.toy_dataset(3).contract_pipe(keys=["image"])
        pipe = seq.to_pipe()
        # an operator defined outside `pipelime` is serialized with its module path
        # (`_add_operator_path`, unchanged since 2.x) so that `build_pipe` can import it
        assert pipe[-1] == {
            f"{__name__}:contract_pipe": {"keys": ["image"], "stage": {"identity": {}}}
        }
        assert len(build_pipe(pipe)) == 3
        assert SamplesSequence.toy_dataset(2).to_pipe()[0]["toy_dataset"]["length"] == 2


# --- validation interfaces & validator ordering (spec §4.1, §4.5) --------------
class TestValidationInterfaces:
    def test_dynamic_sample_schema(self):
        svi = plt.SampleValidationInterface(
            sample_schema={
                "image": plt.ItemValidationModel(class_path="ImageItem", is_shared=False),
                "meta": plt.ItemValidationModel(class_path="MetadataItem", is_optional=True),
            },
            ignore_extra_keys=False,
            lazy=True,
        )
        model = svi.schema_model
        model(**_sample().extract_keys("image", "meta"))
        with pytest.raises(pyd.ValidationError):
            model(**_sample())  # `other` is an extra key
        shared = _sample().extract_keys("image", "meta")
        shared = shared.set_item("image", shared["image"].make_new(shared["image"], shared=True))
        with pytest.raises(pyd.ValidationError):
            model(**shared)  # image must not be shared
        # `_type_to_string` only strips the `pipelime.items.` default class path when
        # doing so leaves no further dot; `ImageItem` lives in the `pipelime.items.
        # image_item` submodule, so the reduced name still has a dot and the full
        # dotted path is kept instead (same v1 fact pinned in
        # `test_type_def_and_item_type` above).
        assert dump(svi, by_alias=True)["sample_schema"]["image"] == {
            "class_path": "pipelime.items.image_item.ImageItem",
            "is_optional": True,
            "is_shared": False,
            "validator": None,
        }
        assert svi.as_pipe()["validate_samples"]["sample_schema"]["lazy"] is True

    def test_schema_from_class_and_path(self):
        class S(pyd.BaseModel, arbitrary_types_allowed=True):
            image: pli.ImageItem

        assert plt.SampleValidationInterface(sample_schema=S).schema_model is S
        svi = plt.SampleValidationInterface(sample_schema=f"{MODULE}.ContractMeta")
        assert svi.schema_model is ContractMeta

    def test_validator_ordering(self, tmp_path: Path):
        with pytest.raises(pyd.ValidationError, match="Either `folder` or `pipe`"):
            InputDatasetInterface()
        with pytest.raises(pyd.ValidationError, match="overwrite an existing dataset"):
            OutputDatasetInterface(folder=tmp_path)
        assert OutputDatasetInterface(folder=tmp_path, exists_ok=True).folder == tmp_path.resolve()
        assert OutputDatasetInterface(pipe=[{"identity": None}]).folder is None
        with pytest.raises(pyd.ValidationError, match="Invalid pipeline"):
            InputDatasetInterface(folder=tmp_path, pipe=3)
        with pytest.raises(pyd.ValidationError, match="either `filter_query` or `filter_fn`"):
            from pipelime.commands import FilterCommand

            FilterCommand(input=str(tmp_path), output=str(tmp_path / "o"))

    def test_new_path(self, tmp_path: Path):
        H = make_model("H", p=(plt.new_file_path(".txt"), ...))
        assert H(p=tmp_path / "x").p == tmp_path / "x.txt"
        assert H(p=str(tmp_path / "y.txt")).p == tmp_path / "y.txt"
        with pytest.raises(pyd.ValidationError):
            H(p=tmp_path)  # exists
        with pytest.raises(pyd.ValidationError):
            H(p=tmp_path / "z.png")  # wrong suffix
        from pipelime.piper.checkpoint import LocalCheckpoint

        assert LocalCheckpoint(folder=tmp_path / "new").folder == (tmp_path / "new").resolve()
        assert LocalCheckpoint(folder=tmp_path).folder == tmp_path.resolve()

    def test_alias_error_formatting(self):
        from pipelime.cli.utils import show_field_alias_valerr

        with pytest.raises(pyd.ValidationError) as ei:
            PortsCommand(i="notanint")
        from pipelime.cli.utils import format_validation_error

        text = format_validation_error(ei.value, PortsCommand)
        assert "inp / i" in text
        # the 2.x name is kept as an alias (2.x: it rewrote `ei.value` in place and
        # `str(ei.value)` showed the `name / alias` locations)
        assert show_field_alias_valerr(ei.value, PortsCommand) == text

    def test_transformation(self):
        import albumentations as A
        from pipelime.stages import StageAlbumentations
        from pipelime.stages.augmentations import Transformation

        tr = A.Compose([A.Resize(height=2, width=2)])
        st = StageAlbumentations(transform=tr, keys_to_targets={"image": "image"})
        assert isinstance(st.transform, Transformation)
        assert isinstance(st.transform.value, A.Compose)
        st2 = StageAlbumentations(transform=A.to_dict(tr), keys_to_targets={"image": "image"})
        assert dump(st2)["transform"] == A.to_dict(tr)
        y = st(_sample())
        assert y["image"]().shape[:2] == (2, 2)

    def test_color_field(self):
        """`pad_colors: Union[Color, Sequence[Color]]` parses names, hex and tuples."""
        from pipelime.stages import StageCropAndPad

        kw = dict(x=0, y=0, width=4, height=4, images="image")
        st = StageCropAndPad(pad_colors="red", **kw)
        assert st.pad_colors.as_rgb_tuple() == (255, 0, 0)
        st = StageCropAndPad(pad_colors=["#00ff00", (0, 0, 255)], **kw)
        assert [c.as_rgb_tuple() for c in st.pad_colors] == [(0, 255, 0), (0, 0, 255)]
        assert StageCropAndPad(**kw).pad_colors.as_rgb_tuple() == (0, 0, 0)


# --- help rendering (spec §4.6) -------------------------------------------------
HELP_SNAPSHOT = Path(__file__).parent.parent / "sample_data" / "contract" / "help_contract_ports.txt"


def _render_help(model_cls) -> str:
    from rich.console import Console

    import pipelime.cli.pretty_print as pp

    console = Console(width=160, record=True, force_terminal=False, color_system=None)
    # rich.print uses the global console; render through a local one instead
    original = pp.rprint
    try:
        pp.rprint = console.print
        pp.print_model_info(model_cls, show_class_path=False, show_piper_port=True)
    finally:
        pp.rprint = original
    return " ".join(console.export_text().split())


INT_DOC_PLACEHOLDER = "<inspect.getdoc(int) body>"


def _interpreter_independent(text: str) -> str:
    """The rows of the undocumented `int` fields fall back to `inspect.getdoc(int)`,
    whose wording depends on the Python version (3.12: "floating-point"): replace
    its body (every line after the first, which shares its row with the other
    columns) with a placeholder."""
    lines = inspect.getdoc(int).splitlines()
    body = " ".join(" │ │ ".join(lines[1:]).split())
    return text.replace(body, INT_DOC_PLACEHOLDER)


class TestHelpRendering:
    def test_help_rows_snapshot(self):
        text = _interpreter_independent(_render_help(PortsCommand))
        for token in ["inp / i", "out / o", "prm", "INPUT", "OUTPUT", "PARAMETER", "int"]:
            assert token in text
        assert text.count(INT_DOC_PLACEHOLDER) == 3  # the docstring fallback, once per field
        if os.environ.get("PIPELIME_CONTRACT_REGEN") == "1":  # opt-in (re)generation
            HELP_SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
            HELP_SNAPSHOT.write_text(text)
        assert HELP_SNAPSHOT.exists(), (
            "run once with PIPELIME_CONTRACT_REGEN=1 to (re)generate the help snapshot"
        )
        assert text == HELP_SNAPSHOT.read_text()

    def test_type_names(self):
        from pipelime.cli.pretty_print import _human_readable_type as hrt

        assert hrt(t.Tuple[int, ...]) == "(int, ellipsis)"  # a value arg: by its class
        assert hrt(t.TypeVar("T")) == "T"
        assert hrt(t.Callable[[int], str]) == "Callable[[int], str]"
        assert hrt(t.Literal["a", 1]) == "Literal['a', 1]"
        assert hrt(t.Optional[t.Sequence[int]]) == "[int, ...] | None"
        assert hrt(t.Mapping[str, t.Any]) == "{str: Any}"
        # v2 constrained types are `Annotated[X, <constraints>]`: shown as `X`
        # (2.x: by the v1 constrained type's name, e.g. `PositiveInt`)
        assert hrt(t.Union[bool, pyd.PositiveInt]) == "bool | int"
        assert hrt(t.Tuple[pyd.NonNegativeInt, pyd.NonNegativeInt]) == "(int, int)"
        assert hrt(t.Optional[t.Union[pyd.PositiveInt, str]]) == "int | str | None"


# --- modern type hints (spec §4.8) ----------------------------------------------
class ModernCommand(PipelimeCommand, title="contract-modern"):
    a: list[int] = Field([1], description="a list")
    b: dict[str, float] = Field(default_factory=dict, description="a dict")
    c: int | None = Field(None, description="optional int")
    d: tuple[int, str] = Field((1, "x"), description="a tuple")
    e: list[SampleStage] | None = Field(None, description="stages")
    f: int | str = Field(3, description="union")
    g: int | None
    h: StageInput | None = None

    def run(self) -> None:
        pass


class ModernStage(SampleStage, title="contract-modern-stage"):
    keys: list[str] = Field(default_factory=list)
    limit: int | None = None

    def __call__(self, x):
        return x


# See the `ContractPipe` guard above: `test_callable_def`'s
# `THIS_FILE:contract_identity` reimports this whole file a second time as a
# bare-stem module, and re-running `@pls.piped_sequence` would rebind
# `SamplesSequence.contract_modern_pipe` to that second-generation class (whose
# `__module__` a spawned multiprocessing worker cannot reimport).
if "contract_modern_pipe" in SamplesSequence._pipes:
    ModernPipe = SamplesSequence._pipes["contract_modern_pipe"]
else:

    @pls.piped_sequence
    class ModernPipe(PipedSequenceBase, title="contract_modern_pipe"):
        keys: list[str] = Field(default_factory=list)
        limit: int | None = None

        def size(self) -> int:
            return self.source.size()

        def get_sample(self, idx: int) -> pls.Sample:
            return self.source.get_sample(idx)


class ModernEntity(BaseEntity):
    image: pli.ImageItem
    label: pli.NumpyItem | None


class TestModernTypeHints:
    def test_validation(self):
        c = ModernCommand(a=["3"], f="5", e=[StageIdentity()], h="identity")
        # `int | str` given "5": v2 smart unions keep the exact type (an accepted
        # change, see the design spec's risks and the migration guide). 2.x: v1 went
        # left-to-right and coerced to `int` (`5`)
        assert c.a == [3] and c.f == "5" and c.g is None
        assert isinstance(c.h.__root__, StageIdentity)
        assert ModernStage(keys=("a",)).keys == ["a"]
        e = ModernEntity(**_sample())
        assert e.label is None
        seq = SamplesSequence.toy_dataset(2).contract_modern_pipe(keys=["image"], limit=1)
        assert seq.limit == 1

    def test_help(self):
        text = _render_help(ModernCommand)
        for token in ["[int, ...]", "{str: float}", "(int, str)", "int | str", "SampleStage"]:
            assert token in text, token

    # 2.x: the TUI raised AttributeError on `types.UnionType` fields (spec §4.8)
    def test_tui(self):
        from pipelime.cli.tui.utils import get_field_type, init_tui_field, is_tui_needed
        from pipelime.utils.pydantic_compat import iter_fields

        assert is_tui_needed(ModernCommand, {}) is False
        fields = list(iter_fields(ModernCommand))
        types = [get_field_type(f) for f in fields]
        assert all(isinstance(x, str) and x for x in types)
        assert init_tui_field(fields[2], {}).type_  # `c: int | None`

    def test_dag_node(self):
        nodes = NodesDefinition.create({"n": {f"{MODULE}.ModernCommand": {"a": [1, 2], "g": 4}}})
        assert nodes.value["n"].a == [1, 2]
        assert dump(nodes.value["n"])["g"] == 4
