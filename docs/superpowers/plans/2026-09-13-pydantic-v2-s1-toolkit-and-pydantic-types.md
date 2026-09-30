# S1 — Compat toolkit and `utils/pydantic_types.py`

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `pipelime/utils/pydantic_compat.py` (spec §3) with unit tests, then convert the leaf module `pipelime/utils/pydantic_types.py` to native pydantic v2 on top of it.

**Architecture:** The toolkit is pure pydantic v2 (plus a `try`-guarded `pydantic.v1` import for the leftover guard). `pydantic_types.py` depends only on `pipelime.items` and the toolkit, so it is the first source module converted; after this subtask `import pipelime.stages`/`sequences`/`commands`/`cli` **fail** until the end of S2b (v1 models cannot hold v2 fields) — expected.

**Tech Stack:** pydantic 2.12 (target `>=2.10,<3`), pydantic-core `core_schema`, numpy.

**Spec:** `docs/superpowers/specs/2026-09-12-pydantic-v2-migration-design.md` §3, §4.1.

## Global Constraints

- Tests runnable in this subtask: `tests/pipelime/utils/`, `tests/pipelime/items/`, `tests/pipelime/choixe/`, and the contract classes `TestRootWrappers`, `TestValidationInterfaces::test_dynamic_sample_schema`, `test_schema_from_class_and_path`, `test_new_path` (the contract module imports `pipelime.commands`, so run it only after S2b; until then the listed classes are exercised through `tests/pipelime/utils/test_pydantic_types.py` and the new toolkit tests).
- `pipelime/utils/pydantic_compat.py` must not import anything from `pipelime` (it is imported by `pipelime.items`-level code).
- Keep every public name of `pydantic_types.py` (`NewPath`, `new_file_path`, `NumpyType`, `yaml_any_type`, `YamlInput`, `TypeDef`, `ItemType`, `CallableDef`, `ItemValidationModel`, `SampleValidationInterface`, and their `create`/`validate`/`value`/`wrapped_type`/`default_class_path`/signature helpers).

---

### Task S1-T1: `PipelimeModel` — metaclass rules and polymorphic serialization

**Files:**
- Create: `pipelime/utils/pydantic_compat.py`
- Create: `tests/pipelime/utils/test_pydantic_compat.py`

**Interfaces:**
- Produces: `PipelimeModelMeta`, `PipelimeModel` (config: `coerce_numbers_to_str=True`), `is_optional_annotation(resolved, raw)`, `_UNRESOLVED` sentinel.

- [ ] **Step 1: Write the failing tests**

```python
"""Unit tests for pipelime.utils.pydantic_compat (design spec §3)."""
from __future__ import annotations

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

        assert M(a=None, b=None, c=None).a is None
        assert M(a=3).a == 3
        with pytest.raises(pydantic.ValidationError):
            M(a="x")

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
```

Run: `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/utils/test_pydantic_compat.py`
Expected: FAIL with `ModuleNotFoundError: pipelime.utils.pydantic_compat`.

- [ ] **Step 2: Write the module (first part)**

```python
"""Pydantic v2 compatibility toolkit for pipelime (design spec §3).

pipelime 2.x was written against the ``pydantic.v1`` shim. pipelime 3 uses the
native pydantic v2 API but keeps the v1-era *semantics* that downstream code
relies on ("option A" of the migration design). Everything needed for that
lives here:

* :class:`PipelimeModel` — base class of every pipelime model. It restores
  polymorphic nested serialization (a field typed as a base class is dumped
  using the runtime subclass), v1 ``Optional`` semantics (``x: Optional[int]``
  without a default is optional; ``x: int = None`` accepts ``None``), coerces
  numbers to ``str`` fields (CLI values are parsed before validation) and
  refuses ``pydantic.v1`` objects in subclasses with an actionable error.
* :class:`PipelimeRootModel` — base of the "value wrapper" types; accepts the
  v1 ``__root__=`` construction, exposes ``.__root__``/``.value`` and keeps the
  ``{"__root__": ...}`` envelope on ``.dict()``.
* :func:`Field` — ``pydantic.Field`` accepting pipelime flags
  (``piper_port``, ``pipe_source``, ``expand_help``, ``is_required`` and any
  other unknown keyword) which are stored in ``json_schema_extra``.
* :class:`FieldView`, :func:`iter_fields`, :func:`get_field`,
  :func:`field_extra`, :func:`model_title`, :func:`type_info` — the
  introspection API replacing pydantic v1's ``ModelField``.
"""
from __future__ import annotations

import dataclasses
import inspect
import re
import sys
import types
import typing as t

import pydantic
from pydantic import BaseModel, ConfigDict, RootModel
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined, core_schema

try:  # pydantic 3 drops the v1 shim: the guard then becomes a no-op
    from pydantic.v1.fields import FieldInfo as _V1FieldInfo
    from pydantic.v1.fields import ModelPrivateAttr as _V1PrivateAttr
except Exception:  # pragma: no cover
    _V1FieldInfo = None  # type: ignore[assignment,misc]
    _V1PrivateAttr = None  # type: ignore[assignment,misc]

_ModelMetaclass = type(BaseModel)
_NONE_TYPE = type(None)
_UNRESOLVED = object()
_OPTIONAL_STR_RE = re.compile(r"\bOptional\[|\|\s*None\b|\bNone\s*\||\bUnion\[[^\]]*\bNone\b")
_CLASSVAR_STR_RE = re.compile(r"\bClassVar\b")


# --------------------------------------------------------------------------- #
# metaclass helpers
# --------------------------------------------------------------------------- #
def _resolve_annotation(raw: t.Any, globalns: dict, localns: dict) -> t.Any:
    """Evaluate a string annotation; return ``_UNRESOLVED`` when it cannot be.

    Loops because `from __future__ import annotations` turns an explicitly
    quoted annotation (``x: "int | None"``) into a string *containing* a string.
    """
    for _ in range(3):
        if not isinstance(raw, str):
            return raw
        try:
            raw = eval(raw, globalns, dict(localns))  # noqa: S307 (module-controlled input)
        except Exception:
            return _UNRESOLVED
    return _UNRESOLVED if isinstance(raw, str) else raw


def _unwrap_annotated(tp: t.Any) -> t.Any:
    while t.get_origin(tp) is t.Annotated:
        tp = t.get_args(tp)[0]
    return tp


def _is_classvar(resolved: t.Any, raw: t.Any) -> bool:
    if resolved is _UNRESOLVED:
        return isinstance(raw, str) and bool(_CLASSVAR_STR_RE.search(raw))
    resolved = _unwrap_annotated(resolved)
    return resolved is t.ClassVar or t.get_origin(resolved) is t.ClassVar


def is_optional_annotation(resolved: t.Any, raw: t.Any = None) -> bool:
    """True for ``Optional[X]``, ``X | None``, ``Union[..., None]`` (or their
    unresolvable string forms)."""
    if resolved is _UNRESOLVED:
        return isinstance(raw, str) and bool(_OPTIONAL_STR_RE.search(raw))
    resolved = _unwrap_annotated(resolved)
    if resolved is None or resolved is _NONE_TYPE:
        return True
    origin = t.get_origin(resolved)
    return origin in (t.Union, types.UnionType) and _NONE_TYPE in t.get_args(resolved)


def _apply_v1_optional_semantics(namespace: dict) -> None:
    """v1: ``Optional[X]`` without default → ``= None``; ``x: T = None`` → ``Optional[T]``."""
    anns = namespace.get("__annotations__")
    if not anns:
        return
    module = sys.modules.get(namespace.get("__module__", ""))
    globalns = dict(vars(module)) if module is not None else {}
    for name, raw in list(anns.items()):
        if name.startswith("_"):
            continue
        resolved = _resolve_annotation(raw, globalns, namespace)
        if _is_classvar(resolved, raw):
            continue
        if name not in namespace:
            if is_optional_annotation(resolved, raw):
                namespace[name] = None
            continue
        default = namespace[name]
        default_value = default.default if isinstance(default, FieldInfo) else default
        if default_value is None and not is_optional_annotation(resolved, raw):
            anns[name] = t.Optional[raw if resolved is _UNRESOLVED else resolved]


def _is_v1_validator(value: t.Any) -> bool:
    for obj in (value, getattr(value, "__func__", None)):
        if obj is not None and (
            hasattr(obj, "__validator_config__") or hasattr(obj, "__root_validator_config__")
        ):
            return True
    return False


def _check_v1_leftovers(cls_name: str, namespace: dict) -> None:
    if _V1FieldInfo is None:
        return
    for name, value in namespace.items():
        if isinstance(value, (_V1FieldInfo, _V1PrivateAttr)) or _is_v1_validator(value):
            raise TypeError(
                f"`{cls_name}.{name}` is a `pydantic.v1` object ({type(value).__name__}). "
                "pipelime 3 is built on native pydantic v2: replace `import pydantic.v1` "
                "with `import pydantic`, use `from pipelime.piper import Field` for fields "
                "with piper flags and `@pydantic.field_validator` instead of `@validator` "
                "(see docs/migration/pydantic_v2.md)."
            )


class PipelimeModelMeta(_ModelMetaclass):  # type: ignore[misc,valid-type]
    """pydantic's metaclass plus the v1-compat rules of the design spec §3.1."""

    def __new__(mcs, cls_name: str, bases: tuple, namespace: dict, **kwargs: t.Any):
        _check_v1_leftovers(cls_name, namespace)
        _apply_v1_optional_semantics(namespace)
        return super().__new__(mcs, cls_name, bases, namespace, **kwargs)


# --------------------------------------------------------------------------- #
# base models
# --------------------------------------------------------------------------- #
def _polymorphic_serialization(cls: type, schema: core_schema.CoreSchema) -> core_schema.CoreSchema:
    """Serialize instances of subclasses with *their* serializer (v1 behaviour)."""

    def _serialize(value: t.Any, nxt: t.Callable[[t.Any], t.Any], info: core_schema.SerializationInfo):
        if type(value) is cls:
            return nxt(value)
        return type(value).__pydantic_serializer__.to_python(
            value,
            mode=info.mode,
            include=info.include,
            exclude=info.exclude,
            by_alias=info.by_alias,
            exclude_unset=info.exclude_unset,
            exclude_defaults=info.exclude_defaults,
            exclude_none=info.exclude_none,
            round_trip=info.round_trip,
            serialize_as_any=info.serialize_as_any,
            context=info.context,
        )

    schema["serialization"] = core_schema.wrap_serializer_function_ser_schema(
        _serialize, info_arg=True
    )
    return schema


class PipelimeModel(BaseModel, metaclass=PipelimeModelMeta):
    """Base class of every pipelime model (see the module docstring)."""

    model_config = ConfigDict(coerce_numbers_to_str=True)

    @classmethod
    def __get_pydantic_core_schema__(cls, source: t.Any, handler: pydantic.GetCoreSchemaHandler):
        return _polymorphic_serialization(cls, handler(source))
```

- [ ] **Step 3: Run the tests**

Run: `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/utils/test_pydantic_compat.py`
Expected: all `TestOptionalSemantics`, `TestV1Guard`, `TestPolymorphicSerialization` pass. If `test_string_annotations` fails because `eval` cannot see `t` (the test module's globals are used: `t` is imported there), verify `namespace["__module__"]` resolves to the test module; the class body namespace is passed as `localns`.

- [ ] **Step 4: Commit**

```bash
git add pipelime/utils/pydantic_compat.py tests/pipelime/utils/test_pydantic_compat.py
git commit -m "feat(compat): PipelimeModel with v1 Optional semantics, v1 guard, polymorphic dumps"
```

---

### Task S1-T2: `PipelimeRootModel`

**Files:**
- Modify: `pipelime/utils/pydantic_compat.py` (append)
- Modify: `tests/pipelime/utils/test_pydantic_compat.py` (append)

**Interfaces:**
- Produces: `PipelimeRootModel[RootT]` with `_coerce(value)` hook, `create()`, `validate()`, `.value`, `.__root__`, `.dict()` envelope.

- [ ] **Step 1: Write the failing tests**

```python
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
```

Run: `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/utils/test_pydantic_compat.py -k RootModel`
Expected: FAIL (`PipelimeRootModel` missing).

- [ ] **Step 2: Append to the module**

```python
RootT = t.TypeVar("RootT")


class PipelimeRootModel(RootModel[RootT], t.Generic[RootT], metaclass=PipelimeModelMeta):
    """Base of pipelime's value wrappers (``NumpyType``, ``TypeDef``, ...).

    Subclasses implement :meth:`_coerce` (any accepted input → root value); the
    v1 surface (``cls(__root__=x)``, ``.__root__``, ``.value``, ``create``,
    ``validate``, ``.dict()`` envelope) is provided here.
    """

    def __init__(self, root: t.Any = PydanticUndefined, /, **data: t.Any) -> None:
        if "__root__" in data:
            if root is not PydanticUndefined:
                raise TypeError("pass the root value either positionally or as `__root__`")
            root = data.pop("__root__")
        if data:
            raise TypeError(
                f"{type(self).__name__} takes a single root value, "
                f"got unexpected keyword arguments: {sorted(data)}"
            )
        super().__init__(root)

    @classmethod
    def _coerce(cls, value: t.Any) -> t.Any:
        """Turn any accepted input into the root value. Override in subclasses."""
        return value

    @pydantic.model_validator(mode="wrap")
    @classmethod
    def _validate_root(cls, value: t.Any, handler: pydantic.ValidatorFunctionWrapHandler):
        if isinstance(value, cls):
            return value
        if value is PydanticUndefined:  # missing root → let pydantic raise
            return handler(value)
        return handler(cls._coerce(value))

    @classmethod
    def create(cls, value: t.Any):
        return cls.model_validate(value)

    @classmethod
    def validate(cls, value: t.Any):  # v1 name, kept for downstream code
        return cls.model_validate(value)

    @property
    def value(self) -> RootT:
        return self.root

    def dict(self, **kwargs: t.Any) -> t.Dict[str, t.Any]:  # type: ignore[override]
        """v1 envelope ``{"__root__": <serialized root>}``; ``model_dump()`` is bare."""
        return {"__root__": self.model_dump(**kwargs)}


# `__root__` cannot be declared inside a model body (pydantic rejects the name)
PipelimeRootModel.__root__ = property(lambda self: self.root)  # type: ignore[attr-defined]
```

- [ ] **Step 3: Run the tests**

Run: `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/utils/test_pydantic_compat.py -k RootModel`
Expected: 4 passed. If `self.Upper()` does not raise, check that `_validate_root` receives `PydanticUndefined` (not `None`) for a missing root and forwards it to `handler`.

- [ ] **Step 4: Commit**

```bash
git add pipelime/utils/pydantic_compat.py tests/pipelime/utils/test_pydantic_compat.py
git commit -m "feat(compat): PipelimeRootModel with v1 __root__ surface"
```

---

### Task S1-T3: `Field` wrapper and `field_extra`

**Files:**
- Modify: `pipelime/utils/pydantic_compat.py` (append)
- Modify: `tests/pipelime/utils/test_pydantic_compat.py` (append)

**Interfaces:**
- Produces: `Field(default=PydanticUndefined, **kwargs) -> Any`, `field_extra(field_info, key, default=None) -> Any`.

- [ ] **Step 1: Write the failing tests**

```python
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

    def test_callable_json_schema_extra_preserved(self):
        def upd(schema):
            schema["x"] = 1

        class M(pydantic.BaseModel):
            a: int = pc.Field(1, json_schema_extra=upd, piper_port="output")

        schema = M.model_json_schema()["properties"]["a"]
        assert schema["x"] == 1 and schema["piper_port"] == "output"
        assert pc.field_extra(M.model_fields["a"], "piper_port") is None  # callables are opaque
```

Run: `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/utils/test_pydantic_compat.py -k FieldWrapper`
Expected: FAIL (`pc.Field` missing).

- [ ] **Step 2: Append to the module**

```python
# --------------------------------------------------------------------------- #
# Field wrapper
# --------------------------------------------------------------------------- #
_PYDANTIC_FIELD_PARAMS = frozenset(
    name
    for name, prm in inspect.signature(pydantic.Field).parameters.items()
    if prm.kind not in (prm.VAR_KEYWORD, prm.VAR_POSITIONAL)
)


def Field(default: t.Any = PydanticUndefined, **kwargs: t.Any) -> t.Any:  # noqa: N802
    """``pydantic.Field`` that also accepts pipelime flags.

    ``piper_port``, ``pipe_source``, ``expand_help``, ``is_required`` and any
    other keyword unknown to ``pydantic.Field`` are stored in
    ``json_schema_extra`` (read back with :func:`field_extra`), which is where
    pydantic v2 puts extra ``Field`` kwargs — but without the deprecation
    warning pydantic emits for them.
    """
    extra = {k: kwargs.pop(k) for k in list(kwargs) if k not in _PYDANTIC_FIELD_PARAMS}
    if extra:
        current = kwargs.get("json_schema_extra")
        if current is None:
            kwargs["json_schema_extra"] = extra
        elif isinstance(current, dict):
            kwargs["json_schema_extra"] = {**current, **extra}
        else:  # callable(schema) -> None

            def _merged(schema: dict, _orig=current, _extra=extra) -> None:
                _orig(schema)
                schema.update(_extra)

            kwargs["json_schema_extra"] = _merged
    return pydantic.Field(default, **kwargs)


def field_extra(field_info: FieldInfo, key: str, default: t.Any = None) -> t.Any:
    """Read a pipelime flag stored by :func:`Field` (or by a raw
    ``pydantic.Field(**flags)``) from ``json_schema_extra``."""
    extra = field_info.json_schema_extra
    if isinstance(extra, dict):
        return extra.get(key, default)
    return default
```

- [ ] **Step 3: Run the tests**

Run: `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/utils/test_pydantic_compat.py -k FieldWrapper`
Expected: 2 passed.

- [ ] **Step 4: Commit**

```bash
git add pipelime/utils/pydantic_compat.py tests/pipelime/utils/test_pydantic_compat.py
git commit -m "feat(compat): pipelime Field wrapper and field_extra"
```

---

### Task S1-T4: Introspection — `type_info`, `FieldView`, `iter_fields`, `get_field`, `model_title`

**Files:**
- Modify: `pipelime/utils/pydantic_compat.py` (append)
- Modify: `tests/pipelime/utils/test_pydantic_compat.py` (append)

**Interfaces:**
- Produces: `TypeInfo(origin, args, is_union, is_optional, inner)`, `type_info(tp)`, `strip_optional(tp)`, `FieldView` (fields `owner, name, field_info, annotation, inner_type, alias, required, description, exclude, extra`; properties `effective_alias, has_alias, populate_by_name, default, is_model, root_type`), `iter_fields(model_cls)`, `get_field(model_cls, name)`, `model_title(cls)`.

- [ ] **Step 1: Write the failing tests**

```python
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
```

Run: `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/utils/test_pydantic_compat.py -k Introspection`
Expected: FAIL.

- [ ] **Step 2: Append to the module**

```python
# --------------------------------------------------------------------------- #
# introspection
# --------------------------------------------------------------------------- #
@dataclasses.dataclass(frozen=True)
class TypeInfo:
    origin: t.Any
    args: tuple
    is_union: bool
    is_optional: bool
    inner: t.Any
    """The type with ``None`` stripped when exactly one other member remains
    (pydantic v1's ``ModelField.outer_type_``); otherwise the type itself."""


def type_info(tp: t.Any) -> TypeInfo:
    """Uniform view over ``typing`` aliases, builtin generics and both union spellings."""
    tp = _unwrap_annotated(tp)
    origin = t.get_origin(tp)
    args = t.get_args(tp)
    is_union = origin in (t.Union, types.UnionType)
    is_optional = is_union and _NONE_TYPE in args
    inner = tp
    if is_optional:
        rest = tuple(a for a in args if a is not _NONE_TYPE)
        if len(rest) == 1:
            inner = rest[0]
    return TypeInfo(origin, args, is_union, is_optional, inner)


def strip_optional(tp: t.Any) -> t.Any:
    return type_info(tp).inner


def model_title(cls: t.Type[BaseModel]) -> str:
    """``model_config["title"]`` or the class name (v1 ``__config__.title``)."""
    return cls.model_config.get("title") or cls.__name__


@dataclasses.dataclass(frozen=True)
class FieldView:
    """What pipelime needs to know about a model field (replaces v1 ``ModelField``)."""

    owner: t.Type[BaseModel]
    name: str
    field_info: FieldInfo
    annotation: t.Any
    inner_type: t.Any
    alias: t.Optional[str]
    required: bool
    description: t.Optional[str]
    exclude: bool
    extra: t.Dict[str, t.Any]

    @property
    def effective_alias(self) -> str:
        return self.alias or self.name

    @property
    def has_alias(self) -> bool:
        return bool(self.alias) and self.alias != self.name

    @property
    def populate_by_name(self) -> bool:
        return bool(self.owner.model_config.get("populate_by_name", False))

    @property
    def default(self) -> t.Any:
        """The default value (factories are called); ``Ellipsis`` when required."""
        if self.required:
            return Ellipsis
        return self.field_info.get_default(call_default_factory=True)

    @property
    def is_model(self) -> bool:
        return inspect.isclass(self.inner_type) and issubclass(self.inner_type, BaseModel)

    @property
    def root_type(self) -> t.Any:
        """The root annotation when the (inner) type is a ``RootModel``, else ``None``."""
        if inspect.isclass(self.inner_type) and issubclass(self.inner_type, RootModel):
            return self.inner_type.model_fields["root"].annotation
        return None


def _field_view(owner: t.Type[BaseModel], name: str, field_info: FieldInfo) -> FieldView:
    extra = field_info.json_schema_extra
    return FieldView(
        owner=owner,
        name=name,
        field_info=field_info,
        annotation=field_info.annotation,
        inner_type=strip_optional(field_info.annotation),
        alias=field_info.alias,
        required=field_info.is_required(),
        description=field_info.description,
        exclude=bool(field_info.exclude),
        extra=dict(extra) if isinstance(extra, dict) else {},
    )


def iter_fields(model_cls: t.Type[BaseModel]) -> t.Iterator[FieldView]:
    """Fields of ``model_cls`` in declaration order."""
    for name, field_info in model_cls.model_fields.items():
        yield _field_view(model_cls, name, field_info)


def get_field(model_cls: t.Type[BaseModel], name: str) -> FieldView:
    return _field_view(model_cls, name, model_cls.model_fields[name])
```

- [ ] **Step 3: Run the whole toolkit test file**

Run: `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/utils/test_pydantic_compat.py`
Expected: all passed.

- [ ] **Step 4: Commit**

```bash
git add pipelime/utils/pydantic_compat.py tests/pipelime/utils/test_pydantic_compat.py
git commit -m "feat(compat): FieldView/iter_fields/type_info/model_title introspection"
```

---

### Task S1-T5: `pydantic_types.py` — imports, `NewPath`

**Files:**
- Modify: `pipelime/utils/pydantic_types.py:1-64`
- Test: `tests/pipelime/utils/test_pydantic_types.py::TestNewPath`

- [ ] **Step 1: Update the test file's imports** (v1 API form in test code; record in `tests/TEST_CHANGES.md`)

In `tests/pipelime/utils/test_pydantic_types.py` replace `import pydantic.v1 as pyd` with `import pydantic as pyd`.

- [ ] **Step 2: Replace the module imports and `NewPath`**

```python
from __future__ import annotations

import inspect
import typing as t
from pathlib import Path

import numpy as np
import pydantic
from pydantic import ConfigDict, PrivateAttr
from pydantic_core import core_schema

from pipelime.items import Item
from pipelime.utils.pydantic_compat import Field, PipelimeModel, PipelimeRootModel

if t.TYPE_CHECKING:
    from numpy.typing import ArrayLike

    from pipelime.sequences import SamplesSequence


class NewPath(Path):
    """A path that does not exist yet."""

    extension: t.ClassVar[t.Optional[str]] = None

    @classmethod
    def __get_pydantic_core_schema__(cls, source, handler):
        return core_schema.no_info_after_validator_function(cls.validate, handler(Path))

    @classmethod
    def __get_pydantic_json_schema__(cls, schema, handler):
        json_schema = handler(schema)
        json_schema.update(exists=False)
        if cls.extension is not None:
            json_schema.update(extension=cls.extension)
        return json_schema

    @classmethod
    def validate(cls, value: Path) -> Path:
        if value.exists():
            raise ValueError(f"Path `{value}` already exists")

        if cls.extension is not None:
            vsuffix = value.suffix
            if not vsuffix and value.name.endswith("."):
                vsuffix = "."

            if not vsuffix:
                value = value.with_name(value.name + cls.extension)
            elif vsuffix != cls.extension:
                raise ValueError(f"Path `{value}` must have suffix `{cls.extension}`")
        return value
```

`new_file_path()` stays as is.

- [ ] **Step 3: Run the NewPath tests** (the rest of the module still fails to import until T6–T8 are done, so run only after T8 if the module does not import; otherwise now)

Run: `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/utils/test_pydantic_types.py -k NewPath`
Expected: passed (or `ImportError` from the still-v1 classes below — continue with T6).

- [ ] **Step 4: Commit** (may be combined with T6 if the module does not import yet)

```bash
git add pipelime/utils/pydantic_types.py tests/pipelime/utils/test_pydantic_types.py
git commit -m "refactor(pydantic_types): v2 imports and NewPath core schema"
```

---

### Task S1-T6: `NumpyType` and `YamlInput`

**Files:**
- Modify: `pipelime/utils/pydantic_types.py` (classes `NumpyType`, `YamlInput`)
- Test: `tests/pipelime/utils/test_pydantic_types.py::TestNumpyType`, `::TestYamlInput`

- [ ] **Step 1: Edit the tests' v1 forms** (record each in `tests/TEST_CHANGES.md`)

- `pyd.parse_raw_as(plt.NumpyType, nt.json())` → `plt.NumpyType.model_validate_json(nt.model_dump_json())`
- `pyd.parse_obj_as(plt.NumpyType, nt.dict()["__root__"])` → `plt.NumpyType.model_validate(nt.dict()["__root__"])` (the `.dict()` envelope is a kept contract)
- same two replacements for `YamlInput`, `ItemType`, `CallableDef` (`pyd.parse_raw_as(plt.X, x.json())` → `plt.X.model_validate_json(x.model_dump_json())`, `pyd.parse_obj_as(plt.X, x.dict()["__root__"])` → `plt.X.model_validate(x.dict()["__root__"])`).
- `plt.NumpyType(__root__=...)`, `plt.YamlInput(__root__=...)`, `plt.ItemType(__root__=...)`, `plt.CallableDef(__root__=...)` stay unchanged (supported).

- [ ] **Step 2: Replace the two classes**

```python
class NumpyType(PipelimeRootModel[np.ndarray], arbitrary_types_allowed=True):
    """Numpy array type for stages, commands and any other pydantic model.
    Any argument accepted by `numpy.array()` is a valid value. Also, any mapping
    will be treated as keyword arguments for `numpy.array()`.

    Examples:
        Create a new NumpyType instance::

            npt = NumpyType.create(numpy.array([1,2,3]))
            npt = NumpyType.create([[1, 2, 3], [4, 5, 6]])
            npt = NumpyType.create(
                {
                    "object": [[1, 2, 3], [4, 5, 6]],
                    "dtype": "float32",
                    "order": "F",
                }
            )

        Access the numpy array::

            npt.value  # numpy array

        Serialize to dict or json::

            npt_dict = npt.model_dump()      # {"object": [...], "dtype": "..."}
            npt_json_str = npt.model_dump_json()
            npt.dict()                       # {"__root__": {...}} (pipelime 2.x shape)

        Get the object back from dict or json::

            npt_again = NumpyType.model_validate(npt_dict)
            npt_again = NumpyType.model_validate_json(npt_json_str)

        Use this type within another model::

            class MyModel(pydantic.BaseModel):
                tensor: NumpyType = pydantic.Field(
                    default_factory=lambda: NumpyType.create([1, 2, 3])
                )

        Everything still works::

            mm = MyModel()
            mm = MyModel(tensor=np.array([1,2,3]))
            mm = MyModel.model_validate({"tensor": [1, 2, 3]})
            mm = MyModel.model_validate({"tensor": {"object": [1,2,3], "dtype": "float32"}})
            mm_again = MyModel.model_validate(mm.model_dump())
    """

    @classmethod
    def create(
        cls, value: t.Union[NumpyType, "ArrayLike", t.Mapping[str, t.Any]]
    ) -> NumpyType:
        return cls.model_validate(value)

    @classmethod
    def _coerce(cls, value):
        try:
            return np.array(**value) if isinstance(value, t.Mapping) else np.array(value)
        except Exception as e:
            raise ValueError(f"Invalid numpy input: {value}") from e

    @pydantic.model_serializer(mode="plain")
    def _serialize(self) -> t.Dict[str, t.Any]:
        v = self.root
        v_order = {} if v.flags["C_CONTIGUOUS"] else {"order": "F"}
        return {"object": v.tolist(), "dtype": v.dtype.name, **v_order}

    def __str__(self) -> str:
        return str(self.root)

    def __repr__(self) -> str:
        return self.__piper_repr__()

    def __piper_repr__(self) -> str:
        return repr(self.root)


yaml_any_type = t.Union[
    None,
    pydantic.StrictBool,
    pydantic.StrictInt,
    pydantic.StrictFloat,
    pydantic.StrictStr,
    t.Mapping[str, t.Any],
    t.Sequence,
]


class YamlInput(PipelimeRootModel[yaml_any_type]):
    """General yaml/json data (str, number, mapping, list...) optionally loaded from
    a yaml/json file, possibly with key path (format <filepath>[:<key>]).

    (keep the existing Examples block, replacing `parse_obj_as`/`parse_raw_as`/
    `parse_obj`/`.dict()`/`.json()` with `model_validate`/`model_validate_json`/
    `model_dump`/`model_dump_json` as in NumpyType above)
    """

    @classmethod
    def create(cls, value: t.Union[YamlInput, yaml_any_type]) -> YamlInput:
        return cls.model_validate(value)

    @classmethod
    def _coerce(cls, value):
        if isinstance(value, (str, Path)):
            pval = Path(value)
            filepath, _, root_key = pval.name.partition(":")
            filepath = Path(pval.parent / filepath)
            if filepath.exists():
                import pydash as py_
                import yaml

                with filepath.open() as f:
                    value = yaml.safe_load(f)
                    if root_key:
                        value = py_.get(value, root_key, default=None)
            return value
        if cls._check_any_type(value):
            return value
        raise ValueError(f"Invalid yaml data input: {value}")

    @classmethod
    def _check_any_type(cls, value):
        if isinstance(value, (str, int, float, bool, t.Sequence)) or value is None:
            return True
        if isinstance(value, t.Mapping):
            return all(isinstance(k, str) for k in value)

    def __str__(self) -> str:
        return str(self.root)

    def __repr__(self) -> str:
        return self.__piper_repr__()

    def __piper_repr__(self) -> str:
        return repr(self.root)
```

- [ ] **Step 3: Run** (module still may not import until T7/T8; if so, continue and run at T8)

Run: `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/utils/test_pydantic_types.py -k "NumpyType or YamlInput or NewPath"`
Expected: passed.

- [ ] **Step 4: Commit**

```bash
git add pipelime/utils/pydantic_types.py tests/pipelime/utils/test_pydantic_types.py
git commit -m "refactor(pydantic_types): NumpyType and YamlInput as PipelimeRootModel"
```

---

### Task S1-T7: `TypeDef`, `ItemType`, `CallableDef`

**Files:**
- Modify: `pipelime/utils/pydantic_types.py` (classes `TypeDef`, `ItemType`, `CallableDef`)
- Test: `tests/pipelime/utils/test_pydantic_types.py::TestItemType`, `::TestCallableDef`

- [ ] **Step 1: Replace the classes**

```python
TRoot = t.TypeVar("TRoot")


class TypeDef(PipelimeRootModel[t.Type[TRoot]], t.Generic[TRoot], frozen=True):
    """Generic type definition. It accepts both type names and string.
    (keep the existing docstring; update the (de)serialization examples as in NumpyType)
    """

    @classmethod
    def default_class_path(cls) -> str:
        return "__main__."

    @classmethod
    def wrapped_type(cls) -> t.Type[TRoot]:
        return t.get_args(cls.model_fields["root"].annotation)[0]

    @classmethod
    def create(cls, value: t.Union[TypeDef, t.Type[TRoot], str]) -> TypeDef:
        return cls.model_validate(value)

    @classmethod
    def _coerce(cls, value):
        if isinstance(value, str):
            value = cls._string_to_type(value)
        if inspect.isclass(value) and issubclass(value, cls.wrapped_type()):
            return value
        raise ValueError(f"Type `{value}` is not a subclass of `{cls.wrapped_type()}`")

    @pydantic.model_serializer(mode="plain")
    def _serialize(self) -> str:
        return self._type_to_string(self.root)

    @classmethod
    def _type_to_string(cls, type_: t.Type[TRoot]) -> str:
        (unchanged)

    @classmethod
    def _string_to_type(cls, type_str: str) -> t.Type[TRoot]:
        (unchanged)

    def __call__(self, *args, **kwargs) -> TRoot:
        return self.root(*args, **kwargs)

    def __hash__(self) -> int:
        return hash(self.root)

    def __str__(self) -> str:
        return self._type_to_string(self.root)

    def __repr__(self) -> str:
        return self.__piper_repr__()

    def __piper_repr__(self) -> str:
        return repr(self.root)


class ItemType(TypeDef[Item]):
    """Item type definition. It accepts both type names and string.
    The default class path is `pipelime.items`.
    """

    @classmethod
    def default_class_path(cls) -> str:
        return "pipelime.items."


class CallableDef(PipelimeRootModel[t.Callable], frozen=True):
    """Generic callable definition. (keep the existing docstring, updating the
    (de)serialization examples as in NumpyType)
    """

    @classmethod
    def default_class_path(cls) -> str:
        return "__main__."

    @classmethod
    def create(cls, value: t.Union[CallableDef, t.Callable, str]) -> CallableDef:
        return cls.model_validate(value)

    # full_signature / args / has_var_positional / has_var_keyword: unchanged,
    # reading `self.root` instead of `self.__root__`. `args_type` / `return_type`
    # now resolve string (forward-reference) annotations — modules using
    # `from __future__ import annotations` are common downstream and
    # `EntityAction` inference crashed on them (`issubclass(x, "Name")`):

    def _resolved_annotations(self) -> t.Dict[str, t.Any]:
        try:
            return t.get_type_hints(self.root)
        except Exception:  # unresolvable forward refs: fall back to raw annotations
            return {}

    @property
    def args_type(self) -> t.Sequence[t.Optional[t.Type]]:
        hints = self._resolved_annotations()
        return [
            None
            if p.annotation is inspect.Signature.empty
            else hints.get(p.name, p.annotation)
            for p in self.full_signature.parameters.values()
        ]

    @property
    def return_type(self) -> t.Optional[t.Type]:
        rt = self.full_signature.return_annotation
        if rt is inspect.Signature.empty:
            return None
        return self._resolved_annotations().get("return", rt)

    @pydantic.model_serializer(mode="plain")
    def _serialize(self) -> str:
        return self._callable_to_string(self.root)

    @classmethod
    def _callable_to_string(cls, clb: t.Callable) -> str:
        (unchanged)

    @classmethod
    def _string_to_callable(cls, clb_str: str) -> t.Callable:
        (unchanged)

    @classmethod
    def _coerce(cls, value):
        try:
            if isinstance(value, str):
                value = cls._string_to_callable(value)
            elif isinstance(value, t.Mapping):
                clb_path, clb_args = next(iter(value.items()))
                clb = (
                    cls._string_to_callable(clb_path)
                    if isinstance(clb_path, str)
                    else clb_path
                )
                if not isinstance(clb, t.Callable):
                    raise ValueError(f"Invalid callable: {clb_path}")
                if isinstance(clb_args, t.Mapping):
                    value = clb(**clb_args)
                elif isinstance(clb_args, t.Sequence) and not isinstance(
                    clb_args, (str, bytes)
                ):
                    value = clb(*clb_args)
                else:
                    value = clb(clb_args)
        except Exception as e:
            raise ValueError(f"Invalid callable: {value}") from e

        if isinstance(value, t.Callable):
            return value
        raise ValueError(f"Invalid callable: {value}")

    def __call__(self, *args, **kwargs):
        return self.root(*args, **kwargs)

    def __hash__(self) -> int:
        return hash(self.root)

    def __str__(self) -> str:
        return self._callable_to_string(self.root)

    def __repr__(self) -> str:
        return self.__piper_repr__()

    def __piper_repr__(self) -> str:
        return repr(self.root)
```

`"(unchanged)"` means: keep the existing method body verbatim, only replacing `self.__root__` with `self.root`.

- [ ] **Step 2: Add a unit test for string annotations** to `tests/pipelime/utils/test_pydantic_types.py::TestCallableDef`:

```python
    def test_string_annotations_resolved(self):
        def fn(x: "int", y: "t.Optional[str]" = None) -> "float":
            return 1.0

        cd = plt.CallableDef.create(fn)
        assert cd.args_type == [int, t.Optional[str]]
        assert cd.return_type is float
```
(`t` must be importable in that test module — it already does `import typing as t`.)

- [ ] **Step 3: Run**

Run: `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/utils/test_pydantic_types.py -k "ItemType or CallableDef"`
Expected: passed (module imports once T8 is also done if `ItemValidationModel` still references `pyd`).

- [ ] **Step 4: Commit**

```bash
git add pipelime/utils/pydantic_types.py tests/pipelime/utils/test_pydantic_types.py
git commit -m "refactor(pydantic_types): TypeDef/ItemType/CallableDef as PipelimeRootModel"
```

---

### Task S1-T8: `ItemValidationModel` and `SampleValidationInterface`

**Files:**
- Modify: `pipelime/utils/pydantic_types.py` (last two classes)

- [ ] **Step 1: Replace the classes**

```python
# This is defined here to make it picklable
def _identity_fn_helper(x):
    return x


class ItemValidationModel(PipelimeModel, extra="forbid"):
    """Item schema validation."""

    class_path: ItemType = Field(
        ...,
        description=(
            "The item class path. The default package `pipelime.item` can be omitted"
        ),
    )
    is_optional: bool = Field(True, description="Whether the item is required or optional.")
    is_shared: bool = Field(False, description="Whether the item is shared or not.")
    validator_: t.Optional[str] = Field(
        None,
        description=(
            "A class path to a callable accepting the item value and either returning "
            "a validated value or raising an exception in case of error."
        ),
        alias="validator",
    )

    _validator_callable = PrivateAttr()

    def __init__(self, **data):
        from pipelime.choixe.utils.imports import import_symbol

        super().__init__(**data)
        self._validator_callable = (
            import_symbol(self.validator_) if self.validator_ else _identity_fn_helper
        )

    def make_field(self, key_name: str):
        return (
            self.class_path.value,
            (
                pydantic.Field(default_factory=self.class_path.value, alias=key_name)
                if self.is_optional
                else pydantic.Field(..., alias=key_name)
            ),
        )

    def make_validator_method(self, field_name: str):
        import uuid

        # we need random names and dynamic function creation
        # to avoid reusing the same function name for validators
        rnd_name = uuid.uuid1().hex

        _validator_wrapper = (
            "def validate_{}_fn(cls, v):\n".format(rnd_name)
            + "    if v.is_shared != {}:\n".format(self.is_shared)
            + "        raise ValueError(\n"
            + "            'Item must{}be shared.'\n".format(
                " not " if not self.is_shared else " "
            )
            + "        )\n"
            + "    return user_validator_{}(v)\n".format(rnd_name)
        )

        local_scope = {
            **globals(),
            f"user_validator_{rnd_name}": self._validator_callable,
        }
        exec(_validator_wrapper, local_scope)
        fn_helper = local_scope[f"validate_{rnd_name}_fn"]
        return pydantic.field_validator(field_name)(fn_helper)


class SampleValidationInterface(PipelimeModel, extra="forbid"):
    """Sample schema validation."""

    sample_schema: t.Union[
        t.Type[pydantic.BaseModel], str, t.Mapping[str, ItemValidationModel]
    ] = Field(..., description=(... unchanged ...))
    ignore_extra_keys: bool = Field(True, description=(... unchanged ...))
    lazy: bool = Field(True, description="If True, samples will be validated only when accessed.")
    max_samples: int = Field(1, description=(... unchanged ...))

    _schema_model: t.Optional[t.Type[pydantic.BaseModel]] = PrivateAttr(None)

    def _import_schema(self, schema_path: str):
        (unchanged)

    def _make_schema(self, schema_def: t.Mapping[str, ItemValidationModel]):
        def _safe_name(k):
            return f"{k}___"

        _item_map = {_safe_name(k): v.make_field(k) for k, v in schema_def.items()}
        _validators = {
            f"validate_{k}": v.make_validator_method(_safe_name(k))
            for k, v in schema_def.items()
        }

        return pydantic.create_model(
            "SampleSchema",
            __config__=ConfigDict(
                arbitrary_types_allowed=True,
                extra="ignore" if self.ignore_extra_keys else "forbid",
            ),
            __validators__=_validators,
            **_item_map,
        )

    @property
    def schema_model(self) -> t.Type[pydantic.BaseModel]:
        (unchanged, `issubclass(sm, pydantic.BaseModel)`)

    def append_validator(self, sequence: "SamplesSequence") -> "SamplesSequence":
        return sequence.validate_samples(sample_schema=self)

    def as_pipe(self):
        return {"validate_samples": {"sample_schema": self.model_dump(by_alias=True)}}
```

- [ ] **Step 2: Run the module tests and the toolkit tests**

Run: `.venv/bin/python -m pytest -q -o addopts="" tests/pipelime/utils tests/pipelime/items tests/pipelime/choixe`
Expected: all passed. Then: `grep -n "pydantic.v1\|__root__\|_iter\|__get_validators__" pipelime/utils/pydantic_types.py` → no matches except in docstrings mentioning `.dict()`.

- [ ] **Step 3: Commit and update ledgers**

```bash
git add pipelime/utils/pydantic_types.py tests/TEST_CHANGES.md
git commit -m "refactor(pydantic_types): validation interfaces on native pydantic v2"
```

Update `tests/TEST_CHANGES.md` (import + `parse_*` replacements in `test_pydantic_types.py`) and the progress ledger (S1 done; gate: the Step 2 command; note that `pipelime.stages`/`sequences`/`commands`/`cli` do not import until S2b is complete).

Then open `2026-09-13-pydantic-v2-s2a-stages-and-sequences.md`.
