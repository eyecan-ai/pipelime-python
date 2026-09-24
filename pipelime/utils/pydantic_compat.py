"""Pydantic v2 compatibility toolkit for pipelime (design spec §3).

pipelime 2.x was written against the ``pydantic.v1`` shim. pipelime 3 uses the
native pydantic v2 API but keeps the v1-era *semantics* that downstream code
relies on ("option A" of the migration design). Everything needed for that
lives here:

* :class:`PipelimeModel` — base class of every pipelime model. It restores
  polymorphic nested serialization (a field typed as a base class is dumped
  using the runtime subclass), v1 ``Optional`` semantics (``x: Optional[int]``
  without a default is optional; ``x: int = None`` accepts ``None``), coerces
  numbers — ``bool`` included — to ``str`` fields (CLI values are parsed before
  validation; ``StrictStr``/``Field(strict=True)`` reject both, a model-level
  ``ConfigDict(strict=True)`` rejects numbers only), translates the v1 config
  key names of a ``class Config`` or of the class keywords to their v2 names
  (``anystr_strip_whitespace`` → ``str_strip_whitespace``, ...; ``allow_mutation``
  combined with ``frozen`` as v1 did; pydantic v2 would only warn and ignore them),
  compares models by their v1 ``.dict()``, gives JSON schemas that skip what v1
  skipped (:class:`V1JsonSchema`) and refuses ``pydantic.v1`` objects in subclasses
  with an actionable error.
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
import functools
import inspect
import operator
import re
import sys
import types
import typing as t
import warnings

import pydantic
import pydantic.json_schema
import pydantic.warnings
from pydantic import BaseModel, ConfigDict, RootModel
from pydantic.fields import FieldInfo
from pydantic_core import PydanticOmit, PydanticUndefined, core_schema

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

    "Without default" includes a :func:`Field` call that omits ``default`` (and
    ``default_factory``), e.g. ``x: Optional[int] = Field(description="...")``;
    ``Field(...)`` stays required, as in v1. A raw ``pydantic.Field(description=...)``
    cannot be told apart from ``pydantic.Field(...)`` and stays required.

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
        omitted = _DEFAULT_OMITTED.get(id(default)) if isinstance(default, FieldInfo) else None
        if omitted is not None and omitted[0] is default:
            # `Optional[X] = Field(description=...)`: v1 gave the missing default `None`
            if is_optional_annotation(resolved, raw):
                namespace[name] = pydantic.Field(None, **omitted[1])
            continue
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


# pydantic v1 config keys renamed in v2 (pipelime's own table: pydantic keeps its
# list private). v1 `allow_mutation` is handled apart (`_apply_v1_mutability`): it
# combines with `frozen` rather than renaming it. The v1 keys removed in v2 without an
# equivalent (`fields`, `smart_union`, `getter_dict`, ...) are left alone in a
# `class Config` (pydantic warns about them); as class keywords they are dropped with
# a warning (below).
_V1_RENAMED_CONFIG_KEYS = {
    "allow_population_by_field_name": "populate_by_name",
    "anystr_lower": "str_to_lower",
    "anystr_strip_whitespace": "str_strip_whitespace",
    "anystr_upper": "str_to_upper",
    "keep_untouched": "ignored_types",
    "max_anystr_length": "str_max_length",
    "min_anystr_length": "str_min_length",
    "orm_mode": "from_attributes",
    "schema_extra": "json_schema_extra",
    "validate_all": "validate_default",
}


def _translate_v1_config_keys(
    config: t.Mapping[str, t.Any], depth: t.Optional[t.Callable[[str], int]] = None
) -> t.Dict[str, t.Any]:
    """``config`` with the v1 key names replaced by their v2 names; the v1 key is
    dropped either way, so pydantic does not warn about a key that is applied.
    ``allow_mutation`` is dropped too (see :func:`_apply_v1_mutability`).

    When both spellings of a setting are given, the one defined nearest wins:
    ``depth(key)`` is the position, along the config class MRO, of the class that
    defines ``key`` (a child ``Config`` overrides what it inherits, whatever the
    spelling); within the same class — or in a plain mapping, where ``depth`` is
    ``None`` — the v2 key wins.
    """
    out = {
        k: v
        for k, v in config.items()
        if k not in _V1_RENAMED_CONFIG_KEYS and k != "allow_mutation"
    }
    for v1_key, v2_key in _V1_RENAMED_CONFIG_KEYS.items():
        if v1_key not in config:
            continue
        if v2_key in config and (depth is None or depth(v2_key) <= depth(v1_key)):
            continue
        out[v2_key] = config[v1_key]
    return out


# Class attribute of a pipelime model, and of a `Config` class rebuilt by
# `_translate_v1_config`, holding the v1 mutability settings *as defined* (the
# nearest `frozen` and `allow_mutation`, each only when defined somewhere), before
# `_apply_v1_mutability` combined them into the single v2 `frozen`.
_V1_MUTABILITY_ATTR = "__pipelime_v1_mutability__"
_UNSET = object()


def _config_class_lookup(config_cls: type, key: str) -> t.Any:
    """The nearest definition of ``frozen``/``allow_mutation`` along the MRO of a
    ``Config`` class (``_UNSET`` when none). A rebuilt ``Config`` stands for the
    whole MRO of the class it was built from: its v1 settings are read from
    :data:`_V1_MUTABILITY_ATTR` (it has no ``allow_mutation`` attribute)."""
    for klass in config_cls.__mro__:
        attrs = vars(klass)
        stash = attrs.get(_V1_MUTABILITY_ATTR)
        if stash is not None and key in stash:
            return stash[key]
        if key in attrs:
            return attrs[key]
    return _UNSET


def _v1_mutability_of_bases(bases: tuple) -> t.Dict[str, t.Any]:
    """The v1 mutability settings inherited from the model bases (pydantic merges
    the configs of the bases in order: a later base wins)."""
    out: t.Dict[str, t.Any] = {}
    for base in bases:
        stash = getattr(base, _V1_MUTABILITY_ATTR, None)
        if stash is not None:
            out.update(stash)
        else:
            config = getattr(base, "model_config", None)
            if isinstance(config, dict) and "frozen" in config:
                out["frozen"] = config["frozen"]
    return out


def _apply_v1_mutability(
    bases: tuple, namespace: dict, kwargs: dict, config_cls: t.Optional[type]
) -> None:
    """v1: a model is immutable when the nearest ``frozen`` is true *or* the nearest
    ``allow_mutation`` is false — two settings, not two spellings of one. Each is
    resolved on its own (class keywords, then the ``Config`` MRO or the
    ``model_config`` of the class body, then the model bases) and, when
    ``allow_mutation`` is defined anywhere, the combination is passed to pydantic as
    the ``frozen`` class keyword (which takes precedence over every other source).
    The settings are recorded on the class for its subclasses."""
    settings = _v1_mutability_of_bases(bases)
    own_config = namespace.get("model_config")
    for key in ("frozen", "allow_mutation"):
        value = kwargs.pop(key, _UNSET) if key == "allow_mutation" else kwargs.get(key, _UNSET)
        if value is _UNSET and config_cls is not None:
            value = _config_class_lookup(config_cls, key)
        if value is _UNSET and isinstance(own_config, dict):
            value = own_config.get(key, _UNSET)
        if value is not _UNSET:
            settings[key] = value
    if "allow_mutation" not in settings:
        return  # `frozen` alone: pydantic's own resolution is the v1 one
    kwargs["frozen"] = bool(settings.get("frozen", False)) or not settings["allow_mutation"]
    namespace[_V1_MUTABILITY_ATTR] = settings


# pydantic v1 config keys removed in v2 without an equivalent. In a `class Config`
# pydantic warns and ignores them; given as class keywords, pydantic would pass them
# on to `__init_subclass__`, which raises a `TypeError` that does not name the key.
_V1_REMOVED_CONFIG_KEYS = (
    "copy_on_model_validation",
    "error_msg_templates",
    "fields",
    "getter_dict",
    "json_dumps",
    "json_loads",
    "post_init_call",
    "smart_union",
    "underscore_attrs_are_private",
)


def _drop_v1_removed_class_kwargs(cls_name: str, kwargs: dict) -> None:
    for key in _V1_REMOVED_CONFIG_KEYS:
        if key in kwargs:
            del kwargs[key]
            warnings.warn(
                f"`{cls_name}`: the pydantic v1 config key `{key}` (given as a class "
                "keyword) has been removed in pydantic v2 and is ignored; remove it "
                "(see docs/migration/pydantic_v2.md).",
                UserWarning,
                stacklevel=4,  # the `class` statement, through the metaclass `__new__`
            )


def _has_v1_config_keys(config: t.Iterable[str]) -> bool:
    return any(k in _V1_RENAMED_CONFIG_KEYS or k == "allow_mutation" for k in config)


def _translate_v1_config(cls_name: str, bases: tuple, namespace: dict, kwargs: dict) -> None:
    """Rename the v1 config keys of a ``class Config`` and of the class keyword
    arguments (in place) before pydantic reads them: pydantic v2 only warns about
    a v1 key and does not apply it, silently changing the model's behaviour. The
    v1 keys removed in v2 are dropped from the class keywords, with a warning;
    ``frozen``/``allow_mutation`` are combined by :func:`_apply_v1_mutability`."""
    _drop_v1_removed_class_kwargs(cls_name, kwargs)
    config_cls = namespace.get("Config")
    if not isinstance(config_cls, type):
        config_cls = None
    if config_cls is not None:
        # pydantic reads a config class through `dir()` (inherited attributes included)
        attrs = {k: getattr(config_cls, k) for k in dir(config_cls) if not k.startswith("__")}
        if _has_v1_config_keys(attrs):

            def depth(key: str) -> int:
                mro = config_cls.__mro__
                return next((i for i, klass in enumerate(mro) if key in vars(klass)), len(mro))

            rebuilt_ns = {
                # pydantic recognises a nested `Config` by its module and qualname
                "__module__": config_cls.__module__,
                "__qualname__": config_cls.__qualname__,
                **_translate_v1_config_keys(attrs, depth),
            }
            allow_mutation = _config_class_lookup(config_cls, "allow_mutation")
            if allow_mutation is not _UNSET:
                rebuilt_ns[_V1_MUTABILITY_ATTR] = {"allow_mutation": allow_mutation}
            namespace["Config"] = type(config_cls.__name__, (), rebuilt_ns)
    if _has_v1_config_keys(kwargs):
        translated = _translate_v1_config_keys(kwargs)
        if "allow_mutation" in kwargs:  # combined below
            translated["allow_mutation"] = kwargs["allow_mutation"]
        kwargs.clear()
        kwargs.update(translated)
    # the original `Config` (the rebuilt one has no `allow_mutation` of its own)
    _apply_v1_mutability(bases, namespace, kwargs, config_cls)


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


def _has_type_parameters(bases: tuple, namespace: dict) -> bool:
    """Whether the class being created will have free type variables
    (``__parameters__``), computed as ``typing.Generic.__init_subclass__`` does:
    from the subscripted bases of the class statement when there are any, else
    inherited from the bases."""
    orig_bases = namespace.get("__orig_bases__", bases)
    return any(getattr(base, "__parameters__", ()) for base in orig_bases)


class PipelimeModelMeta(_ModelMetaclass):  # type: ignore[misc,valid-type]
    """pydantic's metaclass plus the v1-compat rules of the design spec §3.1."""

    def __new__(mcs, cls_name: str, bases: tuple, namespace: dict, **kwargs: t.Any):
        _check_v1_leftovers(cls_name, namespace)
        _translate_v1_config(cls_name, bases, namespace, kwargs)
        parent_namespace = None
        if _weak_valued is not None and kwargs.get("__pydantic_reset_parent_namespace__", True):
            # pydantic passes `False` when it parametrizes generics (the origin's
            # namespace is inherited): only replace *its* capture with ours.
            parent_namespace = _class_statement_namespace(mcs)
            namespace["__pydantic_parent_namespace__"] = _weak_valued(parent_namespace)
            kwargs["__pydantic_reset_parent_namespace__"] = False
        _apply_v1_optional_semantics(namespace, parent_namespace)
        if _has_type_parameters(bases, namespace):
            return super().__new__(mcs, cls_name, bases, namespace, **kwargs)
        # A non-pydantic generic base (`SamplesSequence(SamplesSequenceBase, ...)`
        # with `SamplesSequenceBase(t.Sequence[Sample])`) puts `typing.Generic`
        # before `BaseModel` in the MRO: pydantic warns because *its*
        # `__class_getitem__` would be shadowed, which only matters for a class
        # that can be parametrized. Without type parameters the warning is moot.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", pydantic.warnings.GenericBeforeBaseModelWarning)
            return super().__new__(mcs, cls_name, bases, namespace, **kwargs)


# --------------------------------------------------------------------------- #
# base models
# --------------------------------------------------------------------------- #
def _custom_serializer_applies(custom: dict, value: t.Any, info: core_schema.SerializationInfo) -> bool:
    """pydantic's ``when_used`` rule for a ``function-plain``/``function-wrap`` ser schema."""
    when_used = custom.get("when_used", "always")
    if when_used == "always":
        return True
    if when_used == "unless-none":
        return value is not None
    if when_used == "json":
        return info.mode == "json"
    return info.mode == "json" and value is not None  # "json-unless-none"


def _polymorphic_serialization(cls: type, schema: core_schema.CoreSchema) -> core_schema.CoreSchema:
    """Serialize instances of subclasses with *their* serializer (v1 behaviour).

    A ``@model_serializer`` declared on the model (or inherited) lands in the
    same ``serialization`` slot of the schema: it is kept and called on the
    exact-type path, so that custom dumps and polymorphism compose.

    The subclass dump receives every option ``SerializationInfo`` exposes (mode,
    include/exclude, by_alias, exclude_*, round_trip, serialize_as_any, context,
    exclude_computed_fields). It does *not* receive ``warnings=`` nor
    ``fallback=`` of the outer ``model_dump()``: pydantic does not expose them
    to a serializer function, so a nested subclass dump warns and falls back
    with pydantic's defaults.
    """
    custom = schema.get("serialization")
    if custom is not None and custom.get("type") not in ("function-wrap", "function-plain"):
        return schema  # not a user serializer: nothing pipelime knows how to compose with

    def _serialize(value: t.Any, nxt: t.Callable[[t.Any], t.Any], info: core_schema.SerializationInfo):
        serializer = getattr(type(value), "__pydantic_serializer__", None)
        if type(value) is cls or serializer is None:
            # exact type, or not a model at all (e.g. after `model_construct`):
            # pydantic's own path, which warns on unexpected values instead of failing
            if custom is None or not _custom_serializer_applies(custom, value, info):
                return nxt(value)
            fn, with_info = custom["function"], custom.get("info_arg", False)
            if custom["type"] == "function-wrap":
                return fn(value, nxt, info) if with_info else fn(value, nxt)
            return fn(value, info) if with_info else fn(value)
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

    # `return_schema` binds the *whole* wrapping function's output, but `_serialize`
    # only funnels every call through the custom serializer when it is unconditional
    # (`when_used="always"`, pydantic's default): otherwise some calls take the
    # `nxt(value)`/default-dump branch above, whose shape does not match a narrower
    # declared return type (e.g. a `when_used="json"` serializer returning `str`),
    # which would make pydantic warn (or, under `-W error`, raise) on every dump that
    # does not use the custom path.
    return_schema = None
    if custom is not None and custom.get("when_used", "always") == "always":
        return_schema = custom.get("return_schema")
    schema["serialization"] = core_schema.wrap_serializer_function_ser_schema(
        _serialize,
        info_arg=True,
        return_schema=return_schema,
    )
    return schema


def _bool_to_str(value: t.Any) -> t.Any:
    return str(value) if isinstance(value, bool) else value


# the keys of a core schema holding other schemas (a schema, a list of schemas or a
# name → schema mapping): the walk below follows these only, never the plain values
# (`default`, `expected`, `metadata`, `serialization`, ...)
_NESTED_SCHEMA_KEYS = (
    "schema",
    "fields",
    "items_schema",
    "keys_schema",
    "values_schema",
    "choices",
    "lax_schema",
    "strict_schema",
    "json_schema",
    "python_schema",
    "definitions",
    "steps",
)


def _coerce_bools_to_str(cls: type, schema: t.Any) -> None:
    """v1 coerced ``bool`` into ``str`` fields like any other number (``True`` →
    ``"True"``; CLI values such as ``+name true`` are YAML-parsed before validation);
    ``coerce_numbers_to_str`` leaves bools out, so every ``str`` schema among the
    fields of ``cls`` gets a before-validator (in place). Nested models are left
    to their own hook: as for ``coerce_numbers_to_str``, the rule is per model.
    Strict ``str`` schemas (``StrictStr``, ``Field(strict=True)``) are skipped: v1's
    strict str rejected bools too. A model-level ``ConfigDict(strict=True)`` is not
    visible here (the ``str`` schema carries no ``strict`` flag, pydantic applies
    the config at validation time), so such a model rejects numbers but still
    turns a bool into ``"True"``; use ``StrictStr`` to reject bools as well.
    A ``str`` already wrapped is left alone.
    """
    if isinstance(schema, (list, tuple)):  # a union choice may be a `(schema, label)` pair
        for item in schema:
            _coerce_bools_to_str(cls, item)
        return
    if not isinstance(schema, dict):
        return
    stype = schema.get("type")
    if not isinstance(stype, str):  # a name → schema mapping (`fields`, tagged-union `choices`)
        for value in schema.values():
            _coerce_bools_to_str(cls, value)
        return
    if stype == "model":
        if schema.get("cls") is cls:
            _coerce_bools_to_str(cls, schema.get("schema"))
        return
    if stype == "str":
        if not schema.get("strict"):
            inner = dict(schema)
            schema.clear()
            schema.update(core_schema.no_info_before_validator_function(_bool_to_str, inner))
        return
    if stype == "function-before" and schema["function"]["function"] is _bool_to_str:
        return
    for key in _NESTED_SCHEMA_KEYS:
        _coerce_bools_to_str(cls, schema.get(key))


# `metadata` key marking a schema whose hooks were applied, valued with the class
_HOOKS_MARKER = "pipelime_hooks_applied_for"


def _apply_v1_hooks(cls: type, schema: core_schema.CoreSchema) -> core_schema.CoreSchema:
    """Both schema hooks, applied once per class.

    pydantic hands back the *stored* schema of an already-built class every time
    the class is referenced as a field type, and the hooks mutate it in place: a
    marker in ``metadata`` keeps a re-reference from stacking another layer. The
    marker holds the class itself because a subclass gets its own, freshly
    generated schema (and its own hooks).
    """
    metadata = schema.get("metadata")
    if metadata is None:
        metadata = schema["metadata"] = {}
    if metadata.get(_HOOKS_MARKER) is cls:
        return schema
    metadata[_HOOKS_MARKER] = cls
    _coerce_bools_to_str(cls, schema)
    return _polymorphic_serialization(cls, schema)


class V1JsonSchema(pydantic.json_schema.GenerateJsonSchema):
    """JSON schema generator used by default by the pipelime models.

    pydantic.v1 left out of the schema what it could not describe (a ``Callable``
    field) where pydantic v2 raises ``PydanticInvalidForJsonSchema``: a ``Callable``
    is omitted (a field, or a union member; ``{}`` when it is the whole value), an
    arbitrary type (``arbitrary_types_allowed``) is described as any value (``{}``).
    A plain pydantic model or a ``TypeAdapter`` holding a pipelime model uses
    pydantic's generator: pass ``schema_generator=V1JsonSchema`` there.
    """

    def generate(self, schema: t.Any, mode: t.Any = "validation") -> t.Any:
        try:
            return super().generate(schema, mode=mode)
        except PydanticOmit:  # the whole value is a `Callable` (e.g. a root model)
            return {}

    def callable_schema(self, schema: core_schema.CallableSchema) -> t.Any:
        raise PydanticOmit

    def is_instance_schema(self, schema: core_schema.IsInstanceSchema) -> t.Any:
        return {}


def _v1_dict(model: BaseModel) -> t.Any:
    """What v1 ``model.dict()`` returns: an overridden ``dict()`` (the root wrappers'
    ``{"__root__": ...}`` envelope, ``StageInput``'s shape, a downstream override),
    else ``model_dump()`` (what pydantic's deprecated ``dict()`` returns, without
    its warning)."""
    if type(model).dict is BaseModel.dict:
        return model.model_dump()
    return model.dict()


def _v1_eq(self: BaseModel, other: t.Any) -> bool:
    """v1 ``BaseModel.__eq__``: the ``.dict()`` of both models, or of ``self``
    against any other value (``model == {"x": 1}``). pydantic v2 compares the
    type, the fields set and the private attributes as well. ``!=`` follows
    (``object.__ne__`` inverts ``__eq__``). Defining ``__eq__`` sets ``__hash__``
    to ``None`` in the class body, as ``BaseModel`` already has it: frozen
    subclasses still get pydantic's generated ``__hash__``."""
    if isinstance(other, BaseModel):
        return _v1_dict(self) == _v1_dict(other)
    return _v1_dict(self) == other


class PipelimeModel(BaseModel, metaclass=PipelimeModelMeta):
    """Base class of every pipelime model (see the module docstring)."""

    model_config = ConfigDict(coerce_numbers_to_str=True)

    __eq__ = _v1_eq

    @classmethod
    def __get_pydantic_core_schema__(cls, source: t.Any, handler: pydantic.GetCoreSchemaHandler):
        return _apply_v1_hooks(cls, handler(source))

    @classmethod
    def model_json_schema(  # type: ignore[override]
        cls,
        by_alias: bool = True,
        ref_template: str = pydantic.json_schema.DEFAULT_REF_TEMPLATE,
        schema_generator: t.Type[pydantic.json_schema.GenerateJsonSchema] = V1JsonSchema,
        mode: t.Literal["validation", "serialization"] = "validation",
        **kwargs: t.Any,
    ) -> t.Dict[str, t.Any]:
        """pydantic's, with :class:`V1JsonSchema` as the default generator."""
        return super().model_json_schema(by_alias, ref_template, schema_generator, mode, **kwargs)


RootT = t.TypeVar("RootT")


class PipelimeRootModel(RootModel[RootT], t.Generic[RootT], metaclass=PipelimeModelMeta):
    """Base of pipelime's value wrappers (``NumpyType``, ``TypeDef``, ...).

    Subclasses implement :meth:`_coerce` (any accepted input → root value); the
    v1 surface (``cls(__root__=x)``, ``.__root__``, ``.value``, ``create``,
    ``validate``, ``.dict()`` envelope, v1 equality) is provided here.
    """

    __eq__ = _v1_eq

    @classmethod
    def __get_pydantic_core_schema__(cls, source: t.Any, handler: pydantic.GetCoreSchemaHandler):
        # same polymorphic dispatch as `PipelimeModel` (e.g. `StageInput.root:
        # SampleStage` or `NumpyType`/`TypeDef` referenced through a base-typed
        # field): keeps a subclass's own `@model_serializer` instead of always
        # dumping the declared (base) root type.
        return _apply_v1_hooks(cls, handler(source))

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

    # Like `RootModel.__init__`: tell pydantic this `__init__` adds nothing to
    # validation, so `model_validate(<dict>)` (and `handler(<dict>)` from
    # `_validate_root`) validates the dict *as the root value* instead of
    # calling `cls(**dict)` — which a custom `__init__` would trigger and which
    # breaks every wrapper whose root is a mapping (`YamlInput`, `NodesDefinition`).
    __init__.__pydantic_base_init__ = True  # type: ignore[attr-defined]

    @classmethod
    def _coerce(cls, value: t.Any) -> t.Any:
        """Turn any accepted input into the root value. Override in subclasses."""
        return value

    @classmethod
    def model_json_schema(  # type: ignore[override]
        cls,
        by_alias: bool = True,
        ref_template: str = pydantic.json_schema.DEFAULT_REF_TEMPLATE,
        schema_generator: t.Type[pydantic.json_schema.GenerateJsonSchema] = V1JsonSchema,
        mode: t.Literal["validation", "serialization"] = "validation",
        **kwargs: t.Any,
    ) -> t.Dict[str, t.Any]:
        """pydantic's, with :class:`V1JsonSchema` as the default generator."""
        return super().model_json_schema(by_alias, ref_template, schema_generator, mode, **kwargs)

    @pydantic.model_validator(mode="wrap")
    @classmethod
    def _validate_root(cls, value: t.Any, handler: pydantic.ValidatorFunctionWrapHandler):
        if isinstance(value, cls):
            return value
        if value is PydanticUndefined:  # missing root → let pydantic raise
            return handler(value)
        if isinstance(value, t.Mapping) and value.keys() == {"__root__"}:
            # the v1 envelope (`.dict()`, 2.x configs): v1 `_enforce_dict_if_root`
            value = value["__root__"]
            if isinstance(value, cls):
                return value
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
# `const`/`unique_items`/`regex`, popping `include` after its own deprecation warning):
# they are forwarded so that pydantic's own handling (and warning) applies. `regex` is
# translated into v2's `pattern` by :func:`Field` before forwarding.
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


# attribute of the `json_schema_extra` callable built by `Field` holding the flags
_FLAGS_ATTR = "__pipelime_field_flags__"


# `Field()` calls that omitted both `default` and `default_factory`, for the v1
# Optional rule of `PipelimeModelMeta`: id(FieldInfo) → (FieldInfo, kwargs to rebuild
# it with `default=None`). `FieldInfo` is slotted and not weak-referenceable, so the
# marker cannot live on the object; holding it here also keeps its id from being
# reused. Entries are few (one per such call, typically in class bodies).
_DEFAULT_OMITTED: t.Dict[int, t.Tuple[FieldInfo, t.Dict[str, t.Any]]] = {}
_OMITTED = object()


def Field(default: t.Any = _OMITTED, **kwargs: t.Any) -> t.Any:  # noqa: N802
    """``pydantic.Field`` that also accepts pipelime flags.

    ``piper_port``, ``pipe_source``, ``expand_help``, ``is_required`` and any
    other keyword unknown to ``pydantic.Field`` are stored in
    ``json_schema_extra`` (read back with :func:`field_extra`), which is where
    pydantic v2 puts extra ``Field`` kwargs — but without the deprecation
    warning pydantic emits for them. The v1 ``regex=`` keyword is translated
    into v2's ``pattern=`` (pydantic v2 rejects ``regex``); the other v1-era
    keywords pydantic still handles itself (``min_items``, ``const``, ... see
    ``_PYDANTIC_LEGACY_FIELD_KWARGS``) are forwarded, so pydantic's own
    conversion, warning or error applies.

    Like v1, a field annotated ``Optional[X]`` whose ``Field`` gives no default (nor
    ``default_factory``) defaults to ``None`` on pipelime models; ``Field(...)`` keeps
    it required.
    """
    if "regex" in kwargs and "pattern" not in kwargs:
        kwargs["pattern"] = kwargs.pop("regex")
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

            # read back by `field_extra`/`FieldView.extra` (a callable is opaque)
            setattr(_merged, _FLAGS_ATTR, extra)
            kwargs["json_schema_extra"] = _merged
    if default is not _OMITTED:
        return pydantic.Field(default, **kwargs)
    field_info = pydantic.Field(**kwargs)
    if "default_factory" not in kwargs:
        _DEFAULT_OMITTED[id(field_info)] = (field_info, kwargs)
    return field_info


def _extra_flags(field_info: FieldInfo) -> t.Dict[str, t.Any]:
    """The pipelime flags of a field: its ``json_schema_extra`` mapping, or the flags
    :func:`Field` merged into a callable ``json_schema_extra``."""
    extra = field_info.json_schema_extra
    if isinstance(extra, dict):
        return extra
    return getattr(extra, _FLAGS_ATTR, None) or {}


def field_extra(field_info: FieldInfo, key: str, default: t.Any = None) -> t.Any:
    """Read a pipelime flag stored by :func:`Field` (or by a raw
    ``pydantic.Field(**flags)``) from ``json_schema_extra``."""
    return _extra_flags(field_info).get(key, default)


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


def strip_annotated(tp: t.Any) -> t.Any:
    """``tp`` without ``Annotated`` metadata at any depth (``Union[bool, PositiveInt]``
    → ``Union[bool, int]``), for display. ``tp`` itself when there is nothing to
    strip; a generic form it does not know how to rebuild is returned as is."""
    tp = _unwrap_annotated(tp)
    args = getattr(tp, "__args__", None)
    if t.get_origin(tp) is None or not isinstance(args, tuple):
        return tp
    new_args = tuple(strip_annotated(a) for a in args)
    if all(n is a for n, a in zip(new_args, args)):
        return tp
    origin = t.get_origin(tp)
    if origin is t.Union:
        return t.Union[new_args]
    if origin is types.UnionType:
        return functools.reduce(operator.or_, new_args)
    if isinstance(tp, types.GenericAlias):  # `list[int]`, `tuple[int, ...]`
        return types.GenericAlias(origin, new_args)
    copy_with = getattr(tp, "copy_with", None)  # `typing` aliases, incl. `Callable`
    return copy_with(new_args) if copy_with is not None else tp


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
        extra=dict(_extra_flags(field_info)),
    )


def iter_fields(model_cls: t.Type[BaseModel]) -> t.Iterator[FieldView]:
    """Fields of ``model_cls`` in declaration order."""
    for name, field_info in model_cls.model_fields.items():
        yield _field_view(model_cls, name, field_info)


def get_field(model_cls: t.Type[BaseModel], name: str) -> FieldView:
    return _field_view(model_cls, name, model_cls.model_fields[name])
