"""Unit tests for pipelime.utils.pydantic_compat (design spec §3)."""
from __future__ import annotations

import inspect
import typing as t
import warnings

import annotated_types
import pydantic
import pytest

from pipelime.utils import pydantic_compat as pc


class TestOptionalSemantics:
    def test_optional_without_default_is_optional(self):
        class M(pc.PipelimeModel):
            a: t.Optional[int]
            b: int | None
            c: t.Union[int, str, None]
            d: int

        assert M.model_fields["a"].is_required() is False
        assert M.model_fields["b"].is_required() is False
        assert M.model_fields["c"].is_required() is False
        assert M.model_fields["d"].is_required() is True
        m = M(d=1)
        assert (m.a, m.b, m.c) == (None, None, None)

    def test_none_default_allows_none(self):
        class M(pc.PipelimeModel):
            a: int = None  # type: ignore[assignment]
            b: str = pydantic.Field(None, description="b")
            c: t.Optional[int] = pydantic.Field(None)

        m = M(a=None, b=None, c=None)
        assert (m.a, m.b, m.c) == (None, None, None)
        assert M.model_fields["b"].is_required() is False and M.model_fields["b"].description == "b"
        assert M.model_fields["c"].is_required() is False
        assert M(a=3).a == 3
        with pytest.raises(pydantic.ValidationError):
            M(a="x")

    def test_annotated_default_is_kept(self):
        class M(pc.PipelimeModel):
            x: t.Annotated[t.Optional[int], pydantic.Field(default=3)]
            y: t.Annotated[t.Optional[int], pydantic.Field(default_factory=lambda: 4)]
            z: t.Annotated[t.Optional[int], pydantic.Field(description="d")]

        assert M.model_fields["x"].is_required() is False
        assert (M().x, M().y, M().z) == (3, 4, None)

    def test_string_annotations(self):
        class M(pc.PipelimeModel):
            a: "t.Optional[int]"
            b: "int | None"
            c: "int"
            d: "list[int] | None"

        m = M(c=1)
        assert (m.a, m.b, m.d) == (None, None, None)
        assert M.model_fields["c"].is_required()

    def test_classvar_and_private_untouched(self):
        class M(pc.PipelimeModel):
            _priv: t.Optional[int] = pydantic.PrivateAttr(None)
            cv: t.ClassVar[t.Optional[int]] = 3
            x: int = 1

        assert M.model_fields.keys() == {"x"}
        assert M.cv == 3 and M()._priv is None

    def test_inherited_fields_keep_semantics(self):
        class Base(pc.PipelimeModel):
            a: t.Optional[int]

        class Child(Base):
            b: t.Optional[str]

        assert Child().a is None and Child().b is None

    def test_numbers_coerced_to_str(self):
        class M(pc.PipelimeModel):
            s: str

        assert M(s=5).s == "5"

    def test_bools_coerced_to_str(self):
        # v1 `str_validator` took the `int` path for bools: `True` -> "True"
        class M(pc.PipelimeModel):
            s: str
            opt: t.Optional[str] = None
            seq: tuple[str, ...] = ()
            kw: dict[str, str] = {}
            either: t.Union[bool, str] = False

        m = M(s=True, opt=False, seq=("a", 1, True), kw={"k": False}, either=True)
        assert m.s == "True" and m.opt == "False"
        assert m.seq == ("a", "1", "True") and m.kw == {"k": "False"}
        assert m.either is True  # the bool member of a union still wins
        assert M.model_json_schema()["properties"]["s"] == {"title": "S", "type": "string"}

        class R(pc.PipelimeRootModel[str]):
            pass

        assert R(True).root == "True"

    def test_bools_coerced_to_str_is_per_model(self):
        # nested plain pydantic models keep pydantic's rules, like `coerce_numbers_to_str`
        class Plain(pydantic.BaseModel):
            s: str

        class M(pc.PipelimeModel):
            inner: Plain

        with pytest.raises(pydantic.ValidationError):
            M(inner={"s": True})

    def test_bools_coerced_to_str_skips_strict(self):
        # v1's strict str rejected bools as well
        class M(pc.PipelimeModel):
            s: pydantic.StrictStr
            c: t.Annotated[str, pydantic.StringConstraints(strict=True)] = ""

        with pytest.raises(pydantic.ValidationError):
            M(s=True)
        with pytest.raises(pydantic.ValidationError):
            M(s="ok", c=False)
        assert M(s="ok", c="x").c == "x"

    def test_bools_coerced_to_str_leaves_plain_values_alone(self):
        # only nested *schemas* are walked: a default that looks like a core
        # schema fragment is a plain value
        class M(pc.PipelimeModel):
            cfg: dict = {"type": "str", "name": "x"}
            which: t.Literal["str", "int"] = "str"

        assert M().cfg == {"type": "str", "name": "x"}
        assert M.model_fields["cfg"].default == {"type": "str", "name": "x"}
        assert M().which == "str"


class TestV1Guard:
    def test_v1_field_rejected(self):
        v1 = pytest.importorskip("pydantic.v1")
        with pytest.raises(TypeError, match="pydantic.v1"):

            class M(pc.PipelimeModel):
                x: int = v1.Field(1)

    def test_v1_private_attr_rejected(self):
        v1 = pytest.importorskip("pydantic.v1")
        with pytest.raises(TypeError, match="pydantic.v1"):

            class M(pc.PipelimeModel):
                _p: int = v1.PrivateAttr(1)

    def test_v1_validator_rejected(self):
        v1 = pytest.importorskip("pydantic.v1")
        with pytest.raises(TypeError, match="pydantic.v1"):

            class M(pc.PipelimeModel):
                x: int = 1

                @v1.validator("x")
                def _v(cls, v):
                    return v

    def test_v2_objects_accepted(self):
        class M(pc.PipelimeModel):
            x: int = pydantic.Field(1)
            _p: int = pydantic.PrivateAttr(2)

            @pydantic.field_validator("x")
            @classmethod
            def _v(cls, v):
                return v

        assert M().x == 1


def _collect_class_config_warnings(make_cls):
    """Build a class with a `class Config` and return (the class, the other warnings).

    pydantic's own `class Config` deprecation is expected (and left as is); anything
    else — e.g. its "Valid config keys have changed in V2" UserWarning — is returned.
    """
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        cls = make_cls()
    others = [w for w in record if not issubclass(w.category, pydantic.PydanticDeprecatedSince20)]
    assert len(others) < len(record), "pydantic's class-Config deprecation expected"
    return cls, others


class TestV1ConfigKeys:
    """v1 config key names are translated to v2 (pydantic only warns and ignores them)."""

    @pytest.mark.parametrize(
        "v1_key,v2_key,value",
        [
            ("allow_population_by_field_name", "populate_by_name", True),
            ("anystr_lower", "str_to_lower", True),
            ("anystr_strip_whitespace", "str_strip_whitespace", True),
            ("anystr_upper", "str_to_upper", True),
            ("keep_untouched", "ignored_types", (property,)),
            ("max_anystr_length", "str_max_length", 5),
            ("min_anystr_length", "str_min_length", 2),
            ("orm_mode", "from_attributes", True),
            ("schema_extra", "json_schema_extra", {"examples": [1]}),
            ("validate_all", "validate_default", True),
        ],
    )
    def test_renamed_key_in_config_class(self, v1_key, v2_key, value):
        def make_cls():
            class M(pc.PipelimeModel):
                x: int = 1

                class Config:
                    pass

                setattr(Config, v1_key, value)  # `class Config: <v1_key> = <value>`

            return M

        M, others = _collect_class_config_warnings(make_cls)
        assert others == []  # no "has been renamed" warning: the key is applied
        assert M.model_config[v2_key] == value
        assert v1_key not in M.model_config

    def test_renamed_key_as_class_kwarg(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # no "renamed" UserWarning, no deprecation

            class M(pc.PipelimeModel, anystr_strip_whitespace=True, allow_mutation=False):
                s: str = ""

        assert M(s="  a  ").s == "a"
        assert M.model_config["frozen"] is True

    def test_settings_are_applied(self):
        def make_cls():
            class M(pc.PipelimeModel):
                s: str = ""
                n: int = pc.Field(0, alias="nn")
                d: int = 7

                @pydantic.field_validator("d")
                @classmethod
                def _d(cls, v):
                    return v * 2

                class Config:
                    anystr_strip_whitespace = True
                    anystr_upper = True
                    allow_population_by_field_name = True
                    allow_mutation = False
                    validate_all = True
                    orm_mode = True
                    schema_extra = {"examples": [{"s": "A"}]}

            return M

        M, others = _collect_class_config_warnings(make_cls)
        assert others == []
        m = M(s="  ab  ", n=3)  # `n` by field name: populate_by_name
        assert (m.s, m.n, m.d) == ("AB", 3, 14)  # stripped, upper-cased, default validated
        with pytest.raises(pydantic.ValidationError, match="frozen"):
            m.s = "x"

        class Obj:
            s, nn, d = "q", 5, 1

        assert M.model_validate(Obj()).n == 5  # orm_mode → from_attributes
        assert M.model_json_schema()["examples"] == [{"s": "A"}]

        class Sub(M):  # the translated config is inherited
            pass

        assert Sub(s=" z ").s == "Z"

    def test_string_length_limits(self):
        def make_cls():
            class M(pc.PipelimeModel):
                s: str = "abc"

                class Config:
                    min_anystr_length = 2
                    max_anystr_length = 3

            return M

        M, _ = _collect_class_config_warnings(make_cls)
        for bad in ("a", "abcd"):
            with pytest.raises(pydantic.ValidationError):
                M(s=bad)

    def test_keep_untouched(self):
        class Untouched:
            pass

        def make_cls():
            class M(pc.PipelimeModel):
                x: int = 1
                helper = Untouched()  # not a field: its type is ignored

                class Config:
                    keep_untouched = (Untouched,)

            return M

        M, others = _collect_class_config_warnings(make_cls)
        assert others == [] and "helper" not in M.model_fields
        assert isinstance(M.helper, Untouched)

    def test_allow_mutation_true_is_not_frozen(self):
        def make_cls():
            class M(pc.PipelimeModel):
                x: int = 1

                class Config:
                    allow_mutation = True

            return M

        M, others = _collect_class_config_warnings(make_cls)
        assert others == [] and M.model_config["frozen"] is False
        m = M()
        m.x = 2
        assert m.x == 2

    def test_v2_key_wins(self):
        def make_cls():
            class M(pc.PipelimeModel):
                s: str = ""

                class Config:
                    anystr_strip_whitespace = False
                    str_strip_whitespace = True
                    allow_mutation = False
                    frozen = False

            return M

        M, others = _collect_class_config_warnings(make_cls)
        assert others == []
        m = M(s=" a ")
        assert m.s == "a" and M.model_config["frozen"] is False
        m.s = "b"

    def test_inherited_config_class(self):
        class Base:
            anystr_lower = True

        def make_cls():
            class M(pc.PipelimeModel):
                s: str = ""

                class Config(Base):
                    pass

            return M

        M, others = _collect_class_config_warnings(make_cls)
        assert others == [] and M(s="AB").s == "ab"

    def test_root_model(self):
        def make_cls():
            class R(pc.PipelimeRootModel[str]):
                class Config:
                    anystr_strip_whitespace = True

            return R

        R, others = _collect_class_config_warnings(make_cls)
        assert others == [] and R("  a ").root == "a"

    def test_removed_key_left_to_pydantic(self):
        # no v2 equivalent: pydantic's warning stays and the key does nothing
        def make_cls():
            class M(pc.PipelimeModel):
                x: int = 1

                class Config:
                    smart_union = True

            return M

        M, others = _collect_class_config_warnings(make_cls)
        assert others  # pydantic itself may emit it more than once
        assert all("'smart_union' has been removed" in str(w.message) for w in others)


class _DeeperMeta(pc.PipelimeModelMeta):
    """A further metaclass layer, as downstream code may add."""

    def __new__(mcs, cls_name, bases, namespace, **kwargs):
        return super().__new__(mcs, cls_name, bases, namespace, **kwargs)


class TestLocalForwardReferences:
    """Models defined inside functions resolve annotations naming function-local
    classes exactly like plain pydantic models do (this module uses
    `from __future__ import annotations`, so every annotation is a string)."""

    def test_local_names_resolve(self):
        Alias = t.Optional[int]

        class Inner(pc.PipelimeModel):
            x: int = 1

        class M(pc.PipelimeModel):
            a: Inner | None = None
            b: "Alias"
            c: Inner = None  # type: ignore[assignment]

        assert M.__pydantic_complete__
        fa, fb, fc = (M.model_fields[k] for k in "abc")
        assert set(t.get_args(fa.annotation)) == {Inner, type(None)}
        assert type(None) in t.get_args(fb.annotation) and not fb.is_required()
        assert set(t.get_args(fc.annotation)) == {Inner, type(None)}
        m = M(a={"x": 2}, c=None)
        assert m.a == Inner(x=2) and m.b is None and m.c is None

    def test_extra_metaclass_layer(self):
        class Inner(pc.PipelimeModel):
            x: int = 1

        class M(pc.PipelimeModel, metaclass=_DeeperMeta):
            a: t.Optional[Inner]

        assert M.__pydantic_complete__ and M().a is None and M(a={}).a == Inner()

    def test_root_model_local_names(self):
        class Inner(pc.PipelimeModel):
            x: int = 1

        class R(pc.PipelimeRootModel[t.Optional[Inner]]):
            pass

        assert R.__pydantic_complete__ and R(None).root is None and R({"x": 3}).root == Inner(x=3)

    def test_module_level_has_no_parent_namespace(self):
        assert _PolyBase.__pydantic_parent_namespace__ is None


class _PolyBase(pc.PipelimeModel, extra="forbid"):
    pass


class _PolySub(_PolyBase):
    a: int = 1
    s: str = pydantic.Field("x", alias="ss")


class _PolyHost(pydantic.BaseModel):
    one: _PolyBase
    many: list[_PolyBase] = []
    maybe: t.Optional[_PolyBase] = None
    by_key: dict[str, _PolyBase] = {}


class TestPolymorphicSerialization:
    Base, Sub, Host = _PolyBase, _PolySub, _PolyHost

    def test_python_and_json(self):
        h = self.Host(one=self.Sub(a=2), many=[self.Sub()], by_key={"k": self.Sub(a=5)})
        assert h.model_dump() == {
            "one": {"a": 2, "s": "x"},
            "many": [{"a": 1, "s": "x"}],
            "maybe": None,
            "by_key": {"k": {"a": 5, "s": "x"}},
        }
        assert h.model_dump(by_alias=True)["one"] == {"a": 2, "ss": "x"}
        assert h.model_dump(exclude_defaults=True) == {"one": {"a": 2}, "many": [{}], "by_key": {"k": {"a": 5}}}
        assert '"one":{"a":2,"s":"x"}' in h.model_dump_json()

    def test_exact_type_and_top_level(self):
        assert self.Host(one=self.Base()).model_dump()["one"] == {}
        assert self.Sub(a=3).model_dump() == {"a": 3, "s": "x"}

    def test_no_warnings_and_json_schema(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            self.Host(one=self.Sub()).model_dump()
            assert "one" in self.Host.model_json_schema()["properties"]

    def test_identity_preserved_on_validation(self):
        s = self.Sub()
        assert self.Host(one=s).one is s

    def test_exclude_computed_fields_forwarded(self):
        if "exclude_computed_fields" not in inspect.signature(pydantic.BaseModel.model_dump).parameters:
            pytest.skip("pydantic < 2.12 has no `exclude_computed_fields`")

        class WithComputed(self.Base):
            a: int = 1

            @pydantic.computed_field
            @property
            def double(self) -> int:
                return self.a * 2

        h = self.Host(one=WithComputed())
        assert h.model_dump()["one"] == {"a": 1, "double": 2}
        assert h.model_dump(exclude_computed_fields=True)["one"] == {"a": 1}

    def test_non_model_value_falls_back_to_pydantic(self):
        # e.g. after `model_construct`: pydantic warns and dumps the value as-is
        h = self.Host.model_construct(one={"a": 1})
        # the detail line changed across pydantic minors (2.10: "Expected `X` but got
        # `dict`", 2.12: "PydanticSerializationUnexpectedValue(...)"); the header did not
        with pytest.warns(UserWarning, match="Pydantic serializer warnings"):
            assert h.model_dump()["one"] == {"a": 1}

    def test_model_serializer_kept(self):
        # a `@model_serializer` shares the schema slot the polymorphic hook uses:
        # it must still run (exact type, subclasses, nested, python and json modes)
        class Wrapped(pc.PipelimeModel):
            a: int = 1

            @pydantic.model_serializer(mode="wrap")
            def _ser(self, handler):
                return {"wrapped": handler(self)}

        class WrappedSub(Wrapped):
            b: int = 2

        class JsonOnly(pc.PipelimeModel):
            a: int = 1

            @pydantic.model_serializer(mode="plain", when_used="json")
            def _ser(self) -> str:
                return "json!"

        class JsonOnlySub(JsonOnly):
            b: int = 2

        class Host(pc.PipelimeModel):
            w: Wrapped
            j: JsonOnly
            ws: list[Wrapped] = []

        assert Wrapped().model_dump() == {"wrapped": {"a": 1}}
        assert WrappedSub().model_dump() == {"wrapped": {"a": 1, "b": 2}}
        h = Host(w=WrappedSub(), j=JsonOnlySub(), ws=[Wrapped(), WrappedSub()])
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            assert h.model_dump() == {
                "w": {"wrapped": {"a": 1, "b": 2}},
                "j": {"a": 1, "b": 2},
                "ws": [{"wrapped": {"a": 1}}, {"wrapped": {"a": 1, "b": 2}}],
            }
            assert h.model_dump(mode="json")["j"] == "json!"
            assert '"j":"json!"' in h.model_dump_json()


def _bool_to_str_layers(schema) -> list[int]:
    """Bool→str layers stacked on each non-strict ``str`` schema of a stored core schema."""
    layers = []

    def walk(node, depth):
        if isinstance(node, dict):
            if node.get("type") == "function-before" and node["function"]["function"] is pc._bool_to_str:
                walk(node["schema"], depth + 1)
            elif node.get("type") == "str":
                if not node.get("strict"):
                    layers.append(depth)
            else:
                for key, value in node.items():
                    if key not in ("metadata", "serialization"):
                        walk(value, 0)
        elif isinstance(node, (list, tuple)):
            for value in node:
                walk(value, 0)

    walk(schema, 0)
    return layers


def _pipelime_wrap_serializers(schema) -> int:
    """Length of the chain of pipelime polymorphic wrap serializers on a stored core schema."""
    if schema["type"] == "definitions":
        schema = schema["schema"]
    count, ser = 0, schema.get("serialization")
    while ser is not None and ser.get("type") == "function-wrap":
        fn = ser["function"]
        if not fn.__qualname__.startswith("_polymorphic_serialization."):
            break
        count += 1
        ser = inspect.getclosurevars(fn).nonlocals["custom"]
    return count


class TestHooksAppliedOncePerClass:
    """pydantic hands back the *stored* core schema of a built class on every
    reference from another model: the hooks must not stack on it."""

    def _references(self, tp, count):
        for i in range(count):
            base = pc.PipelimeModel if i % 2 else pydantic.BaseModel
            pydantic.create_model(
                f"Ref{i}", __base__=base, x=(tp, ...), y=(t.Optional[tp], None), z=(list[tp], [])
            )

    def test_model_referenced_by_several_models_keeps_one_layer(self):
        class Inner(pc.PipelimeModel):
            s: str
            d: dict[str, str] = {}

        class Wrapper(pc.PipelimeRootModel[str]):
            pass

        self._references(Inner, 250)
        self._references(Wrapper, 250)

        for cls, n_str in ((Inner, 3), (Wrapper, 1)):
            schema = cls.__pydantic_core_schema__
            assert schema["metadata"][pc._HOOKS_MARKER] is cls
            assert _bool_to_str_layers(schema) == [1] * n_str
            assert _pipelime_wrap_serializers(schema) == 1
        assert Inner.model_json_schema()["properties"]["s"] == {"title": "S", "type": "string"}
        assert Wrapper.model_json_schema() == {"title": "Wrapper", "type": "string"}
        last = pydantic.create_model("Last", x=(Inner, ...)).model_json_schema()
        assert last["$defs"]["Inner"]["properties"]["s"] == {"title": "S", "type": "string"}
        assert Inner(s=True, d={"k": False}).model_dump() == {"s": "True", "d": {"k": "False"}}
        assert Wrapper(True).root == "True"

    def test_subclass_gets_its_own_hooks(self):
        class Inner(pc.PipelimeModel):
            s: str

        self._references(Inner, 3)

        class Sub(Inner):
            more: str = "x"

        class Host(pydantic.BaseModel):
            one: Inner

        schema = Sub.__pydantic_core_schema__
        assert schema is not Inner.__pydantic_core_schema__
        assert schema["metadata"][pc._HOOKS_MARKER] is Sub
        assert _bool_to_str_layers(schema) == [1, 1]
        assert _pipelime_wrap_serializers(schema) == 1
        assert Sub(s=True, more=False).more == "False"
        assert Host(one=Sub(s="a")).model_dump() == {"one": {"s": "a", "more": "x"}}


class TestGenericBaseBeforeModel:
    """`SamplesSequence(SamplesSequenceBase, PipelimeModel)` with
    `SamplesSequenceBase(t.Sequence[Sample])`: `typing.Generic` precedes `BaseModel`
    in the MRO (the non-pydantic `__iter__` must win). pydantic's warning about it
    only matters for classes that can still be parametrized."""

    def test_fully_parametrized_base_is_silent(self):
        class SeqBase(t.Sequence[int]):
            def __getitem__(self, idx):
                if idx >= 3:
                    raise IndexError(idx)
                return idx

            def __len__(self):
                return 3

        with warnings.catch_warnings():
            warnings.simplefilter("error")

            class Seq(SeqBase, pc.PipelimeModel):
                x: int = 0

            class SubSeq(Seq):
                y: int = 1

        assert list(SubSeq()) == [0, 1, 2]  # `Sequence.__iter__`, not `BaseModel.__iter__`
        assert SubSeq().model_dump() == {"x": 0, "y": 1}

    def test_parametrizable_class_still_warns(self):
        T = t.TypeVar("T")

        class GenBase(t.Generic[T]):
            pass

        with pytest.warns(pydantic.warnings.GenericBeforeBaseModelWarning):

            class Gen(GenBase[T], pc.PipelimeModel):
                x: int = 0


class _Upper(pc.PipelimeRootModel[str]):
    @classmethod
    def _coerce(cls, value):
        if isinstance(value, bytes):
            value = value.decode()
        if not isinstance(value, str):
            raise ValueError("not a string")
        return value.upper()


class _UpperHost(pydantic.BaseModel):
    u: _Upper


class TestRootModel:
    Upper = _Upper

    def test_construction_forms(self):
        assert self.Upper("a").root == "A"
        assert self.Upper(__root__="b").root == "B"
        assert self.Upper.create(b"c").root == "C"
        assert self.Upper.validate("d").value == "D"
        assert self.Upper.model_validate("e").__root__ == "E"
        with pytest.raises(TypeError):
            self.Upper("a", __root__="b")
        with pytest.raises(TypeError):
            self.Upper("a", other=1)
        with pytest.raises(pydantic.ValidationError):
            self.Upper(3)
        with pytest.raises(pydantic.ValidationError):
            self.Upper()

    def test_instance_is_unwrapped(self):
        u = self.Upper("a")
        assert self.Upper(u).root == "A"
        assert self.Upper(__root__=u).root == "A"
        assert self.Upper.model_validate(u) is u  # identity pass-through kept

    def test_dumps(self):
        u = self.Upper("a")
        assert u.model_dump() == "A"
        assert u.model_dump_json() == '"A"'
        assert u.dict() == {"__root__": "A"}

    def test_nested(self):
        H = _UpperHost
        h = H(u="x")
        assert h.model_dump() == {"u": "X"}
        u = self.Upper("y")
        assert H(u=u).u is u  # identity pass-through
        assert H.model_validate({"u": "z"}).u.root == "Z"

    def test_generic(self):
        T = t.TypeVar("T")

        class Box(pc.PipelimeRootModel[list[T]], t.Generic[T]):
            pass

        class IntBox(Box[int]):
            pass

        assert IntBox(["1", 2]).root == [1, 2]
        assert IntBox.model_fields["root"].annotation == list[int]

    def test_mapping_root(self):
        # a dict input is the root *value*, never `cls(**dict)` (pydantic would
        # call the custom `__init__` that way unless it is flagged as base init)
        class Cfg(pc.PipelimeRootModel[dict[str, int]]):
            @classmethod
            def _coerce(cls, value):
                return dict(value)

        d = {"a": 1, "b": 2}
        for c in (Cfg(d), Cfg(__root__=d), Cfg.create(d), Cfg.model_validate(d), Cfg.create([("a", 1), ("b", 2)])):
            assert c.root == d
        assert Cfg.model_validate_json('{"a": 1, "b": 2}').root == d
        assert Cfg(d).dict() == {"__root__": d}

        class H(pc.PipelimeModel):
            c: Cfg

        assert H(c=d).c.root == d
        assert H.model_validate({"c": {"x": "3"}}).c.root == {"x": 3}
        with pytest.raises(pydantic.ValidationError):
            Cfg({"a": "not an int"})
        with pytest.raises(TypeError):
            Cfg(d, extra=1)


class TestFieldWrapper:
    def test_flags_go_to_json_schema_extra_without_warnings(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")

            class M(pydantic.BaseModel):
                a: int = pc.Field(1, description="a", alias="aa", piper_port="input", custom_flag=42)
                b: int = pc.Field(default_factory=lambda: 2, pipe_source=True, json_schema_extra={"k": "v"})
                c: int = pc.Field(3)
                d: int = pc.Field(..., is_required=False)

        fa, fb, fc, fd = (M.model_fields[k] for k in "abcd")
        assert fa.description == "a" and fa.alias == "aa" and fa.default == 1
        assert fa.json_schema_extra == {"piper_port": "input", "custom_flag": 42}
        assert fb.json_schema_extra == {"k": "v", "pipe_source": True}
        assert fc.json_schema_extra is None
        assert fd.is_required() and fd.json_schema_extra == {"is_required": False}
        assert pc.field_extra(fa, "piper_port") == "input"
        assert pc.field_extra(fc, "piper_port", "param") == "param"
        assert pc.field_extra(fb, "missing") is None

    def test_legacy_pydantic_kwargs_forwarded(self):
        # kwargs pydantic.Field still converts (or rejects) itself must reach it,
        # with pydantic's own deprecation warning, instead of being stashed as flags
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")

            class M(pydantic.BaseModel):
                a: list[int] = pc.Field([], min_items=2, piper_port="input")

        assert len(caught) == 1 and issubclass(caught[0].category, DeprecationWarning)
        assert "min_items" in str(caught[0].message)
        assert M.model_fields["a"].json_schema_extra == {"piper_port": "input"}
        assert M(a=[1, 2]).a == [1, 2]
        with pytest.raises(pydantic.ValidationError):
            M(a=[1])
        with pytest.raises(pydantic.PydanticUserError):
            pc.Field("x", const=True)

    def test_v1_regex_translated_to_pattern(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")

            class M(pydantic.BaseModel):
                a: str = pc.Field("a", regex=r"^[a-z]+$")

        assert M.model_fields["a"].json_schema_extra is None
        assert M(a="abc").a == "abc"
        with pytest.raises(pydantic.ValidationError):
            M(a="A1")

    def test_callable_json_schema_extra_preserved(self):
        def upd(schema):
            schema["x"] = 1

        class M(pydantic.BaseModel):
            a: int = pc.Field(1, json_schema_extra=upd, piper_port="output")

        schema = M.model_json_schema()["properties"]["a"]
        assert schema["x"] == 1 and schema["piper_port"] == "output"
        assert pc.field_extra(M.model_fields["a"], "piper_port") is None  # callables are opaque


class TestIntrospection:
    def test_type_info_both_spellings(self):
        for tp in (t.Optional[int], int | None, t.Union[None, int]):
            ti = pc.type_info(tp)
            assert ti.is_union and ti.is_optional and ti.inner is int
        ti = pc.type_info(t.Union[int, str, None])
        assert ti.is_optional and ti.inner == t.Union[int, str, None]  # v1 outer_type_ semantics
        ti = pc.type_info(int | str)
        assert ti.is_union and not ti.is_optional and ti.inner == int | str
        assert pc.type_info(list[int]).origin is list and pc.type_info(t.List[int]).origin is list
        assert pc.type_info(t.Annotated[t.Optional[int], "meta"]).inner is int
        assert pc.strip_optional(dict[str, int] | None) == dict[str, int]
        assert pc.type_info(int).inner is int and pc.type_info(int).args == ()

    def test_strip_annotated(self):
        pos = t.Annotated[int, annotated_types.Gt(0)]
        assert pc.strip_annotated(pos) is int
        assert pc.strip_annotated(t.Annotated[pos, "more"]) is int
        assert pc.strip_annotated(t.Union[bool, pos]) == t.Union[bool, int]
        assert pc.strip_annotated(bool | pos) == bool | int
        assert pc.strip_annotated(t.Tuple[pos, pos]) == t.Tuple[int, int]
        assert pc.strip_annotated(tuple[pos, ...]) == tuple[int, ...]
        assert pc.strip_annotated(t.Optional[t.List[pos]]) == t.Optional[t.List[int]]
        assert pc.strip_annotated(dict[str, list[pos]]) == dict[str, list[int]]
        assert pc.strip_annotated(t.Callable[[pos], pos]) == t.Callable[[int], int]
        # nothing to strip: the very same object (its printed form is unchanged)
        for tp in (int, t.List[int], t.Optional[int], int | None, t.Literal["a", 1],
                   t.Callable[..., int], t.Dict[str, t.Any], list[int]):
            assert pc.strip_annotated(tp) is tp

    def test_field_view(self):
        class Inner(pc.PipelimeModel):
            x: int = 1

        class R(pc.PipelimeRootModel[list[int]]):
            pass

        class M(pc.PipelimeModel, populate_by_name=True):
            a: int = pc.Field(1, alias="aa", description="A", piper_port="input")
            b: t.Optional[Inner]
            c: list[str] = pc.Field(default_factory=list, exclude=True)
            d: R | None = None
            e: str

        views = {v.name: v for v in pc.iter_fields(M)}
        assert list(views) == ["a", "b", "c", "d", "e"]
        a, b, c, d, e = (views[k] for k in "abcde")
        assert a.owner is M and a.populate_by_name is True
        assert (a.alias, a.effective_alias, a.has_alias) == ("aa", "aa", True)
        assert (e.alias, e.effective_alias, e.has_alias) == (None, "e", False)
        assert a.description == "A" and a.extra == {"piper_port": "input"} and not a.exclude
        assert a.default == 1 and c.default == [] and e.default is Ellipsis
        assert e.required and not a.required and not b.required
        assert b.inner_type is Inner and b.is_model and b.root_type is None
        assert d.inner_type is R and d.root_type == list[int]
        assert c.exclude is True and c.inner_type == list[str] and not c.is_model
        assert pc.get_field(M, "b") == b
        with pytest.raises(KeyError):
            pc.get_field(M, "nope")

    def test_model_title(self):
        class A(pc.PipelimeModel, title="the-title"):
            pass

        class B(pc.PipelimeModel):
            pass

        assert pc.model_title(A) == "the-title" and pc.model_title(B) == "B"


class TestS2aToolkitFixes:
    """Regression tests for the two toolkit fixes S2a needed: a `@model_serializer`
    composes with polymorphic dispatch on `PipelimeRootModel` too (not just
    `PipelimeModel`), and a non-pydantic `Generic` base with no free type parameters
    (`SamplesSequenceBase(t.Sequence[Sample])` before `PipelimeModel`) creates
    silently and keeps its own `__iter__`."""

    def test_root_model_plain_serializer_composes_with_polymorphic_dispatch(self):
        class CustomRoot(pc.PipelimeRootModel[dict]):
            @pydantic.model_serializer(mode="plain")
            def _ser(self):
                return {"custom": self.root}

        class Host(pydantic.BaseModel):
            r: pc.PipelimeRootModel[dict]

        h = Host(r=CustomRoot({"a": 1}))
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            assert h.model_dump() == {"r": {"custom": {"a": 1}}}

    def test_sequence_before_model_creates_silently_and_keeps_iter(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")

            class Seq(t.Sequence[int], pc.PipelimeModel):
                def __len__(self):
                    return 2

                def __getitem__(self, idx):
                    if idx >= 2:
                        raise IndexError(idx)
                    return idx * 10

        # `Sequence.__iter__`, not `BaseModel.__iter__` (which would yield `(field,
        # value)` pairs from `self.__dict__` and, since `Seq` has none, give `[]`)
        assert list(Seq()) == [0, 10]
