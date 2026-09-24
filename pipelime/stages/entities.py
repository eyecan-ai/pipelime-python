import inspect
import typing as t

import pydantic
from pydantic import ConfigDict

from pipelime.items import Item
from pipelime.stages import SampleStage
from pipelime.utils.pydantic_compat import Field, PipelimeModel, get_field
from pipelime.utils.pydantic_types import CallableDef, TypeDef

if t.TYPE_CHECKING:
    from pipelime.sequences import Sample

ActionTp = t.TypeVar("ActionTp", bound=t.Callable)


@t.overload
def register_action(  # noqa: E704
    *, title: t.Optional[str] = None, description: t.Optional[str] = None
) -> t.Callable[[ActionTp], ActionTp]: ...


@t.overload
def register_action(__action: ActionTp) -> ActionTp: ...  # noqa: E704


def register_action(
    __action=None, *, title: t.Optional[str] = None, description: t.Optional[str] = None
):
    """Register an action (class or function)

    Args:
        __action (_type_, optional): _description_. Defaults to None.
        title (t.Optional[str], optional): _description_. Defaults to None.
        description (t.Optional[str], optional): _description_. Defaults to None.
    """

    def _decorator(func):
        from pathlib import Path

        from pipelime.cli.utils import ActionInfo, PipelimeSymbolsHelper

        name = title or func.__name__
        try:
            source_path = inspect.getfile(func)
            class_path = Path(source_path).resolve().as_posix()

            if (func.__module__.replace(".", "/") + ".py") in class_path:
                class_path = func.__module__ + "." + func.__name__
            else:
                class_path = class_path + ":" + func.__name__
        except (TypeError, OSError):
            # this happens when the class does not come from a file
            class_path = "__main__." + func.__name__

        docs = description
        if docs is None:
            docs = inspect.getdoc(func) or ""

        PipelimeSymbolsHelper.register_action(
            name,
            ActionInfo(
                action=func,
                name=name,
                description=docs,
                classpath=class_path,
            ),
        )
        return func

    if __action is None:
        return _decorator
    return _decorator(__action)


ItTp = t.TypeVar("ItTp", bound=Item)
ValTp = t.TypeVar("ValTp")


class ParsedItem(
    PipelimeModel,
    t.Generic[ItTp, ValTp],
    extra="forbid",
    arbitrary_types_allowed=True,
):
    raw_item: ItTp
    parsed_value: ValTp

    def __call__(self) -> ValTp:
        return self.parsed_value

    @property
    def value(self) -> ValTp:
        return self.parsed_value

    @classmethod
    def raw_item_type(cls) -> t.Type[ItTp]:
        return cls.model_fields["raw_item"].annotation  # type: ignore[return-value]

    @classmethod
    def parsed_value_type(cls) -> t.Type[ValTp]:
        return cls.model_fields["parsed_value"].annotation  # type: ignore[return-value]

    @classmethod
    def value_to_item_data(cls, value) -> t.Any:
        if hasattr(value, "__to_item_data__"):
            return value.__to_item_data__()
        elif isinstance(value, pydantic.BaseModel):
            return value.model_dump()
        return value

    @classmethod
    def make_raw_item(cls, value) -> ItTp:
        return cls.raw_item_type().make_new(cls.value_to_item_data(value))

    @classmethod
    def make_parsed_value(cls, value) -> ValTp:
        pvtp = cls.parsed_value_type()
        if inspect.isclass(pvtp) and issubclass(pvtp, pydantic.BaseModel):
            return pydantic.TypeAdapter(pvtp).validate_python(value)  # type: ignore
        return pvtp(value)  # type: ignore

    @classmethod
    def make_new(cls, value) -> ItTp:
        return cls.make_raw_item(value)

    @classmethod
    def _coerce(cls, value) -> t.Dict[str, t.Any]:
        """Any accepted input -> the `{"raw_item", "parsed_value"}` field mapping."""
        if isinstance(value, Item):
            if isinstance(value, cls.raw_item_type()):
                return {
                    "raw_item": value,
                    "parsed_value": cls.make_parsed_value(value()),
                }
        elif isinstance(value, cls.parsed_value_type()):
            return {"raw_item": cls.make_raw_item(value), "parsed_value": value}
        else:
            try:
                value = cls.make_parsed_value(value)
            except Exception:
                pass
            else:
                return {"raw_item": cls.make_raw_item(value), "parsed_value": value}
        # NB: v1 turned TypeError into a ValidationError, v2 only converts ValueError
        raise ValueError(
            f"{value} is neither `{cls.raw_item_type()}` nor `{cls.parsed_value_type()}`"
        )

    @pydantic.model_validator(mode="wrap")
    @classmethod
    def _validate_input(cls, value, handler):
        if isinstance(value, cls):
            return value
        # direct construction `cls(raw_item=..., parsed_value=...)` passes the field mapping
        if isinstance(value, t.Mapping) and set(value.keys()) == {
            "raw_item",
            "parsed_value",
        }:
            return handler(value)
        return handler(cls._coerce(value))

    @classmethod
    def validate(cls, value):  # v1 name kept for downstream code
        return cls.model_validate(value)


class ParsedData(ParsedItem[Item, ValTp], t.Generic[ValTp]):
    pass


class ModelDynamicKey:
    __slots__ = ("owner", "item_tp", "default", "default_factory", "extra_kwargs")

    def __init__(
        self,
        item_tp: t.Union[t.Type[Item], t.Type[ParsedItem]],
        default: t.Any = ...,
        default_factory: t.Optional[t.Callable[[], t.Any]] = None,
        extra_kwargs: t.Mapping[str, t.Any] = {},
    ):
        self.item_tp = item_tp
        self.default = default
        self.default_factory = default_factory
        self.extra_kwargs = extra_kwargs

    def validate(self, key) -> t.Union[t.Type[Item], t.Type[ParsedItem]]:
        parser_model = pydantic.create_model(
            "DynamicKeyParser",
            __config__=ConfigDict(arbitrary_types_allowed=True),
            **{
                key: (
                    self.item_tp,
                    Field(
                        self.default,
                        default_factory=self.default_factory,
                        **self.extra_kwargs,
                    ),
                )
            },
        )
        parsed_values = parser_model.model_validate(
            {key: getattr(self.owner, key)} if hasattr(self.owner, key) else {}
        )
        return getattr(parsed_values, key)


def DynamicKey(
    item_tp: t.Union[t.Type[Item], t.Type[ParsedItem]],
    default: t.Any = ...,
    *,
    default_factory: t.Optional[t.Callable[[], t.Any]] = None,
    **field_kwargs,
) -> t.Any:
    if default is not ... and default_factory is not None:
        raise ValueError("cannot specify both default and default_factory")

    return pydantic.PrivateAttr(
        ModelDynamicKey(item_tp, default, default_factory, field_kwargs)
    )


DerivedEntityTp = t.TypeVar("DerivedEntityTp", bound="BaseEntity")


class BaseEntity(PipelimeModel, extra="allow", arbitrary_types_allowed=True):
    """The base class for all input/output entity models."""

    def __init__(self, **data):
        # create an item field from raw values
        # NB: if the field is optional, it can be None
        cls = type(self)
        for k, v in data.items():
            if k in cls.model_fields and not isinstance(v, Item):
                k_field = get_field(cls, k)
                item_cls = k_field.inner_type
                if (
                    inspect.isclass(item_cls)
                    and issubclass(item_cls, Item)
                    and (k_field.required or v is not None)
                ):
                    data[k] = item_cls.make_new(v)
        super().__init__(**data)

        # assign self as the owner of dynamic key fields
        for k in self.__private_attributes__:
            v = getattr(self, k)
            if isinstance(v, ModelDynamicKey):
                v.owner = self

    @pydantic.model_serializer(mode="wrap")
    def _serialize(self, handler, info: pydantic.SerializationInfo) -> t.Dict[str, t.Any]:
        # skip None fields and bypass ParsedItem (v1 `_iter` override); a ParsedItem
        # is recognised on the instance attribute, not by the keys of its dump
        names = {}
        if info.by_alias:
            for name, field_info in type(self).model_fields.items():
                alias = field_info.serialization_alias or field_info.alias
                if alias:
                    names[alias] = name
        out = {}
        for k, v in handler(self).items():
            if v is None:
                continue
            if isinstance(getattr(self, names.get(k, k), None), ParsedItem):
                raw_item = v.get("raw_item") if isinstance(v, t.Mapping) else None
                if raw_item is not None:
                    out[k] = raw_item
            else:
                out[k] = v
        return out

    @classmethod
    def merge(
        cls: t.Type[DerivedEntityTp], __other__: "BaseEntity", /, **kwargs
    ) -> DerivedEntityTp:
        """Creates a new entity by merging `other` entity with extra `kwargs` fields."""
        other_dict = __other__.model_dump()
        for k, v in kwargs.items():
            if not isinstance(v, Item):
                # NB: None is a valid value for optional fields
                my_k_field = get_field(cls, k) if k in cls.model_fields else None
                if not my_k_field or my_k_field.required or v is not None:
                    # user has no preference on the actual Item class,
                    # so keep the original item type if it is a subclass
                    # of the declared output item type
                    if k in other_dict:
                        other_item_obj = other_dict[k]
                        if other_item_obj is not None:
                            my_item_cls = Item
                            if my_k_field:
                                my_item_cls = my_k_field.inner_type
                                if inspect.isclass(my_item_cls) and issubclass(
                                    my_item_cls, ParsedItem
                                ):
                                    my_item_cls = my_item_cls.raw_item_type()
                            if isinstance(other_item_obj, my_item_cls):
                                kwargs[k] = other_item_obj.make_new(
                                    ParsedItem.value_to_item_data(v)
                                )
        return cls(**{**other_dict, **kwargs})


class ActionDef(CallableDef):
    """Action definition, can be a registered action title, a callable
    (cfr. Choixe `$call`/`$symbol`), a `class.path.to.a.function`,
    a `path/to/a/file.py:function`, a `function:::<code>`
    or a mapping where the key is like above, while the value is
    the list of `__init__` arguments (mapping, sequence or single value).
    """

    @classmethod
    def _coerce(cls, value):
        from pipelime.cli.utils import PipelimeSymbolsHelper

        if isinstance(value, str):
            # action is function
            act = PipelimeSymbolsHelper.get_action(value)
            if act is not None:
                value = act.action
        elif isinstance(value, t.Mapping):
            # action is a callable instance
            name, args = next(iter(value.items()))
            if isinstance(name, str):
                act = PipelimeSymbolsHelper.get_actions().get(name, None)
                if act is not None:
                    value = {act.action: args}
        return super()._coerce(value)

    @classmethod
    def validate_action(cls, value) -> "ActionDef":  # v1 name kept for downstream code
        return cls.model_validate(value)


class BaseEntityType(TypeDef[BaseEntity]):
    """An entity type. It accepts both type names and string."""


class EntityAction(PipelimeModel, extra="forbid"):
    """An action and its associated input entity model."""

    action: ActionDef = Field(
        ...,
        description=(
            "The action callable to run (can be a class path). The expected annotation "
            "is `(BaseEntitySubClass) -> BaseEntityLike`. Return type annotation "
            "is not mandatory."
        ),
    )
    input_type: BaseEntityType = Field(
        BaseEntity,
        validate_default=True,
        description=(
            "The input type of the action (can be a string). If None, "
            "it is inferred from the callable's annotations or set to BaseEntity."
        ),
    )

    @pydantic.field_validator("action")
    @classmethod
    def validate_action(cls, v):
        params = v.full_signature.parameters
        if not params:
            raise ValueError(
                "The action must have at least one argument, ie, the input model entity."
            )
        first_param = next(iter(params.values()))
        if (
            first_param.kind == inspect.Parameter.KEYWORD_ONLY
            or first_param.kind == inspect.Parameter.VAR_KEYWORD
        ):
            raise ValueError(
                "The action must have at least one positional argument, "
                "ie, the input model entity."
            )
        return v

    @pydantic.field_validator("input_type")
    @classmethod
    def validate_input_type(cls, v, info: pydantic.ValidationInfo):
        if "action" in info.data:  # if not True, an error should have been raised yet
            action_t = info.data["action"].args_type[0]
            if v is None:
                v = BaseEntityType.create(action_t or BaseEntity)
            else:
                if action_t is not None and not issubclass(v.value, action_t):
                    if not issubclass(action_t, v.value):
                        raise ValueError(
                            "Action annotation is incompatible with the input type."
                        )
                    v = BaseEntityType.create(action_t)  # use the more specific type

        return v

    @pydantic.model_validator(mode="wrap")
    @classmethod
    def _validate_input(cls, value, handler):
        if isinstance(value, cls):
            return value
        if not isinstance(value, t.Mapping):
            value = {"action": value}
        if "action" not in value:
            value = {"action": value}
        return handler(value)

    @classmethod
    def validate(cls, value):  # v1 name kept for downstream code
        return cls.model_validate(value)


_MISSING = object()


class StageEntity(SampleStage, title="entity"):
    """Runs an entity action, ie, `(BaseEntitySubClass) -> BaseEntityLike`,
    on each sample."""

    entity_action: EntityAction = Field(..., description="The entity action to run.")

    @staticmethod
    def _normalize(value: t.Any) -> t.Any:
        """Any pipelime 2.x input shape -> the EntityAction spec.

        Shapes: a bare spec (callable / str / `{action: ..., input_type: ...}`),
        the `{"__root__": spec}` envelope written by 2.x `to_pipe()`, and the
        `{"entity_action": spec}` field mapping. Envelopes may be nested
        (`{"entity_action": {"__root__": spec}}`), so they are peeled in turn.
        """
        while isinstance(value, t.Mapping):
            keys = set(value.keys())
            if keys == {"__root__"}:
                value = value["__root__"]
            elif keys == {"entity_action"}:
                value = value["entity_action"]
            else:
                break
        return value

    def __init__(self, __root__: t.Any = _MISSING, /, **data):
        # positional spec, `__root__=` keyword, the 2.x envelope, or the bare
        # `{action: ..., input_type: ...}` kwargs form
        if __root__ is not _MISSING:
            if data:
                raise TypeError(
                    "StageEntity takes a single entity action, "
                    f"got extra arguments {sorted(data)}"
                )
            spec = __root__
        else:
            spec = data
        super().__init__(entity_action=self._normalize(spec))  # type: ignore

    @pydantic.model_validator(mode="wrap")
    @classmethod
    def _validate_input(cls, value, handler):
        # `model_validate(spec)` / `TypeAdapter(StageEntity)` / nested validation
        # receive the raw spec (v1 accepted it through `_enforce_dict_if_root`)
        if isinstance(value, cls):
            return value
        return handler({"entity_action": cls._normalize(value)})

    @pydantic.model_serializer(mode="wrap")
    def _serialize(self, handler) -> t.Dict[str, t.Any]:
        # `{}` when `include`/`exclude` leave the action out (2.x: `.dict()` of a
        # root model without its `__root__`)
        return handler(self).get("entity_action", {})

    def __call__(self, x: "Sample") -> "Sample":
        from pipelime.sequences import Sample

        ea = self.entity_action
        return Sample(ea.action(ea.input_type.value(**x)).model_dump())


# v1 exposed the action as `__root__`; pydantic rejects the name inside a class body
StageEntity.__root__ = property(lambda self: self.entity_action)  # type: ignore[attr-defined]
