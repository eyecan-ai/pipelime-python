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
