"""Unit tests for pipelime.utils.pydantic_compat (design spec §3)."""
from __future__ import annotations

import inspect
import typing as t
import warnings

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
        with pytest.warns(UserWarning, match="PydanticSerializationUnexpectedValue"):
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
