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

try:  # pydantic internals used by PipelimeModelMeta to capture the class-statement frame
    from pydantic._internal._model_construction import build_lenient_weakvaluedict as _weak_valued
    from pydantic._internal._typing_extra import parent_frame_namespace as _parent_frame_namespace
except Exception:  # pragma: no cover - internals moved: pydantic's own (shallower) capture is used
    _weak_valued = None  # type: ignore[assignment]
    _parent_frame_namespace = None  # type: ignore[assignment]

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


def _annotated_has_default(resolved: t.Any) -> bool:
    """True when ``Annotated[...]`` metadata carries a ``FieldInfo`` with a default
    (``x: Annotated[Optional[int], Field(default=3)]``): pydantic owns the default then."""
    while t.get_origin(resolved) is t.Annotated:
        resolved, *metadata = t.get_args(resolved)
        for meta in metadata:
            if isinstance(meta, FieldInfo) and (
                meta.default is not PydanticUndefined or meta.default_factory is not None
            ):
                return True
    return False


def _apply_v1_optional_semantics(namespace: dict, parent_namespace: t.Optional[dict] = None) -> None:
    """v1: ``Optional[X]`` without default → ``= None``; ``x: T = None`` → ``Optional[T]``.

    String annotations are evaluated against the defining module's globals, the
    locals of the frame executing the ``class`` statement (``parent_namespace``,
    the same pydantic uses to resolve forward references) and the class body.
    """
    anns = namespace.get("__annotations__")
    if not anns:
        return
    module = sys.modules.get(namespace.get("__module__", ""))
    globalns = dict(vars(module)) if module is not None else {}
    localns = {**(parent_namespace or {}), **namespace}
    for name, raw in list(anns.items()):
        if name.startswith("_"):
            continue
        resolved = _resolve_annotation(raw, globalns, localns)
        if _is_classvar(resolved, raw):
            continue
        if name not in namespace:
            if is_optional_annotation(resolved, raw) and not _annotated_has_default(resolved):
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


def _class_statement_namespace(mcs: type) -> t.Optional[dict]:
    """Locals of the frame executing the ``class`` statement (``None`` at module level).

    pydantic's ``ModelMetaclass.__new__`` captures them at a fixed stack depth to
    resolve forward references to function-local names (``__pydantic_parent_namespace__``).
    Every metaclass ``__new__`` layered above it — ours, or a downstream subclass of it —
    shifts that depth onto the metaclass frame, so the capture is done here instead,
    skipping every ``__new__`` found in the metaclass MRO.
    """
    if _parent_frame_namespace is None:  # pragma: no cover
        return None
    new_codes = set()
    for klass in mcs.__mro__:
        new = klass.__dict__.get("__new__")
        code = getattr(getattr(new, "__func__", new), "__code__", None)
        if code is not None:
            new_codes.add(code)
    depth = 1  # frame 1 (from here) is our caller: `PipelimeModelMeta.__new__`
    frame: t.Optional[types.FrameType] = sys._getframe(1)
    while frame is not None and frame.f_code in new_codes:
        depth += 1
        frame = frame.f_back
    return _parent_frame_namespace(parent_depth=depth + 1)  # +1: its own frame


class PipelimeModelMeta(_ModelMetaclass):  # type: ignore[misc,valid-type]
    """pydantic's metaclass plus the v1-compat rules of the design spec §3.1."""

    def __new__(mcs, cls_name: str, bases: tuple, namespace: dict, **kwargs: t.Any):
        _check_v1_leftovers(cls_name, namespace)
        parent_namespace = None
        if _weak_valued is not None and kwargs.get("__pydantic_reset_parent_namespace__", True):
            # pydantic passes `False` when it parametrizes generics (the origin's
            # namespace is inherited): only replace *its* capture with ours.
            parent_namespace = _class_statement_namespace(mcs)
            namespace["__pydantic_parent_namespace__"] = _weak_valued(parent_namespace)
            kwargs["__pydantic_reset_parent_namespace__"] = False
        _apply_v1_optional_semantics(namespace, parent_namespace)
        return super().__new__(mcs, cls_name, bases, namespace, **kwargs)


# --------------------------------------------------------------------------- #
# base models
# --------------------------------------------------------------------------- #
def _polymorphic_serialization(cls: type, schema: core_schema.CoreSchema) -> core_schema.CoreSchema:
    """Serialize instances of subclasses with *their* serializer (v1 behaviour)."""

    def _serialize(value: t.Any, nxt: t.Callable[[t.Any], t.Any], info: core_schema.SerializationInfo):
        serializer = getattr(type(value), "__pydantic_serializer__", None)
        if type(value) is cls or serializer is None:
            # exact type, or not a model at all (e.g. after `model_construct`):
            # pydantic's own path, which warns on unexpected values instead of failing
            return nxt(value)
        extra_flags = {}
        exclude_computed_fields = getattr(info, "exclude_computed_fields", None)
        if exclude_computed_fields is not None:  # pydantic >= 2.12
            extra_flags["exclude_computed_fields"] = exclude_computed_fields
        return serializer.to_python(
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
            **extra_flags,
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
        if isinstance(root, type(self)):
            # `_validate_root` would return that instance untouched (identity
            # pass-through), leaving *this* one uninitialised: re-wrap its value
            root = root.root
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


# --------------------------------------------------------------------------- #
# Field wrapper
# --------------------------------------------------------------------------- #
# v1-era keywords that `pydantic.Field` still consumes out of its `**extra` (converting
# `min_items`/`max_items`/`allow_mutation` with a deprecation warning, raising for
# `const`/`unique_items`/`regex`, ignoring `include`): they must reach pydantic.
_PYDANTIC_LEGACY_FIELD_KWARGS = frozenset(
    {"const", "min_items", "max_items", "unique_items", "allow_mutation", "regex", "include"}
)
_PYDANTIC_FIELD_PARAMS = (
    frozenset(
        name
        for name, prm in inspect.signature(pydantic.Field).parameters.items()
        if prm.kind not in (prm.VAR_KEYWORD, prm.VAR_POSITIONAL)
    )
    | _PYDANTIC_LEGACY_FIELD_KWARGS
)


def Field(default: t.Any = PydanticUndefined, **kwargs: t.Any) -> t.Any:  # noqa: N802
    """``pydantic.Field`` that also accepts pipelime flags.

    ``piper_port``, ``pipe_source``, ``expand_help``, ``is_required`` and any
    other keyword unknown to ``pydantic.Field`` are stored in
    ``json_schema_extra`` (read back with :func:`field_extra`), which is where
    pydantic v2 puts extra ``Field`` kwargs — but without the deprecation
    warning pydantic emits for them. The v1-era keywords pydantic still handles
    itself (``min_items``, ``regex``, ... see ``_PYDANTIC_LEGACY_FIELD_KWARGS``)
    are forwarded, so pydantic's own conversion, warning or error applies.
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
