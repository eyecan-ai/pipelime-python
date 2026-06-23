import inspect
import typing as t

import pydantic as pyd

from pipelime.items import Item
from pipelime.stages import SampleStage
from pipelime.utils.pydantic_types import CallableDef, TypeDef

if t.TYPE_CHECKING:
    from pipelime.sequences import Sample

ActionTp = t.TypeVar("ActionTp", bound=t.Callable)


@t.overload
def register_action(
    *, title: t.Optional[str] = None, description: t.Optional[str] = None
) -> t.Callable[[ActionTp], ActionTp]: ...


@t.overload
def register_action(__action: ActionTp) -> ActionTp: ...


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
    pyd.BaseModel,
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
        return cls.model_fields["raw_item"].annotation

    @classmethod
    def parsed_value_type(cls) -> t.Type[ValTp]:
        return cls.model_fields["parsed_value"].annotation

    @classmethod
    def value_to_item_data(cls, value) -> t.Any:
        if hasattr(value, "__to_item_data__"):
            return value.__to_item_data__()
        elif isinstance(value, pyd.BaseModel):
            return value.model_dump()
        return value

    @classmethod
    def make_raw_item(cls, value) -> ItTp:
        return cls.raw_item_type().make_new(cls.value_to_item_data(value))

    @classmethod
    def make_parsed_value(cls, value) -> ValTp:
        pvtp = cls.parsed_value_type()
        return (  # type: ignore
            pyd.TypeAdapter(pvtp).validate_python(value)
            if isinstance(pvtp, type) and issubclass(pvtp, pyd.BaseModel)
            else pvtp(value)
        )

    @classmethod
    def make_new(cls, value) -> ItTp:
        return cls.make_raw_item(value)

    @pyd.model_validator(mode="before")
    @classmethod
    def _coerce(cls, value):
        if isinstance(value, cls):
            return {"raw_item": value.raw_item, "parsed_value": value.parsed_value}
        elif isinstance(value, Item):
            if isinstance(value, cls.raw_item_type()):
                return {
                    "raw_item": value,
                    "parsed_value": cls.make_parsed_value(value()),
                }
        elif isinstance(value, cls.parsed_value_type()):
            return {
                "raw_item": cls.make_raw_item(value),
                "parsed_value": value,
            }
        else:
            try:
                value = cls.make_parsed_value(value)
            except Exception:
                pass
            else:
                return {
                    "raw_item": cls.make_raw_item(value),
                    "parsed_value": value,
                }
        raise TypeError(
            f"{value} is neither `{cls.raw_item_type()}` nor `{cls.parsed_value_type()}`"
        )


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
        field_kwargs = dict(self.extra_kwargs)
        if self.default_factory is not None:
            field_kwargs["default_factory"] = self.default_factory
            field_info = pyd.Field(**field_kwargs)
        else:
            field_info = pyd.Field(self.default, **field_kwargs)

        parser_model = pyd.create_model(
            "DynamicKeyParser",
            __config__=pyd.ConfigDict(arbitrary_types_allowed=True),
            **{key: (self.item_tp, field_info)},
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

    return pyd.PrivateAttr(
        ModelDynamicKey(item_tp, default, default_factory, field_kwargs)
    )


DerivedEntityTp = t.TypeVar("DerivedEntityTp", bound="BaseEntity")


def _field_core_type(ann) -> t.Optional[type]:
    """Strip Optional/Union[..., None] and return the underlying class, if any.

    For pydantic generic models (e.g. ``ParsedItem[X, Y]``) the annotation is
    already a concrete class, so it is returned as-is.
    """
    if t.get_origin(ann) is t.Union:
        args = [a for a in t.get_args(ann) if a is not type(None)]
        if len(args) == 1:
            ann = args[0]
    return ann if isinstance(ann, type) else None


class BaseEntity(
    pyd.BaseModel,
    extra="allow",
    arbitrary_types_allowed=True,
):
    """The base class for all input/output entity models."""

    def __init__(self, **data):
        # create an item field from raw values
        # NB: if the field is optional, it can be None
        cls_fields = type(self).model_fields
        for k, v in data.items():
            if k in cls_fields and not isinstance(v, Item):
                k_field = cls_fields[k]
                core = _field_core_type(k_field.annotation)
                if (
                    core is not None
                    and issubclass(core, Item)
                    and (k_field.is_required() or v is not None)
                ):
                    data[k] = core.make_new(v)
        super().__init__(**data)

        # assign self as the owner of dynamic key fields
        for k in self.__private_attributes__:
            v = getattr(self, k)
            if isinstance(v, ModelDynamicKey):
                v.owner = self

    @pyd.model_serializer(mode="wrap")
    def _serialize(self, handler):
        # skip None fields and bypass ParsedItem
        out = {}
        for k, v in handler(self).items():
            if v is not None:
                if isinstance(v, t.Mapping) and "raw_item" in v:
                    if v["raw_item"] is not None:
                        out[k] = v["raw_item"]
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
                my_k_field = cls.model_fields.get(k, None)
                if not my_k_field or my_k_field.is_required() or v is not None:
                    # user has no preference on the actual Item class,
                    # so keep the original item type if it is a subclass
                    # of the declared output item type
                    if k in other_dict:
                        other_item_obj = other_dict[k]
                        if other_item_obj is not None:
                            my_item_cls = Item
                            if my_k_field:
                                core = _field_core_type(my_k_field.annotation)
                                if core is not None:
                                    my_item_cls = core
                                    if issubclass(my_item_cls, ParsedItem):
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

    @pyd.model_validator(mode="before")
    @classmethod
    def _coerce(
        cls,
        value: t.Union[
            CallableDef, t.Callable, str, t.Mapping[t.Union[str, t.Callable], t.Any]
        ],
    ):
        from pipelime.cli.utils import PipelimeSymbolsHelper

        if isinstance(value, cls):
            return value.root
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
        return cls._to_callable(value)


class BaseEntityType(TypeDef[BaseEntity]):
    """An entity type. It accepts both type names and string."""


class EntityAction(pyd.BaseModel, extra="forbid"):
    """An action and its associated input entity model."""

    action: ActionDef = pyd.Field(
        ...,
        description=(
            "The action callable to run (can be a class path). The expected annotation "
            "is `(BaseEntitySubClass) -> BaseEntityLike`. Return type annotation "
            "is not mandatory."
        ),
    )
    input_type: BaseEntityType = pyd.Field(
        BaseEntity,
        validate_default=True,
        description=(
            "The input type of the action (can be a string). If None, "
            "it is inferred from the callable's annotations or set to BaseEntity."
        ),
    )

    @pyd.field_validator("action")
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

    @pyd.field_validator("input_type")
    @classmethod
    def validate_input_type(cls, v, info: pyd.ValidationInfo):
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

    @pyd.model_validator(mode="before")
    @classmethod
    def _coerce(cls, value):
        if isinstance(value, cls):
            return value
        if not isinstance(value, t.Mapping):
            value = {"action": value}
        if "action" not in value:
            value = {"action": value}
        return value


_STAGE_ENTITY_UNSET = object()


class StageEntity(SampleStage, title="entity"):
    action: EntityAction = pyd.Field(..., description="The entity action to run.")

    def __init__(self, __root__=_STAGE_ENTITY_UNSET, **data):
        if __root__ is not _STAGE_ENTITY_UNSET:
            super().__init__(action=__root__, **data)  # type: ignore
        else:
            super().__init__(**data)  # type: ignore

    @pyd.model_validator(mode="before")
    @classmethod
    def _coerce(cls, value):
        # Accept a bare EntityAction spec (callable, str, or
        # `{"action": ..., "input_type": ...}` mapping) as the whole stage.
        if isinstance(value, t.Mapping):
            if "action" in value and set(value.keys()) <= {"action"}:
                return value  # already a {"action": <spec>} field mapping
            if "action" in value or "input_type" in value:
                return {"action": value}
            return value
        return {"action": value}

    @pyd.model_serializer(mode="plain")
    def _serialize(self, info: pyd.SerializationInfo):
        return self.action.model_dump(
            by_alias=bool(info.by_alias),
            exclude_none=bool(info.exclude_none),
            exclude_defaults=bool(info.exclude_defaults),
        )

    def __call__(self, x: "Sample") -> "Sample":
        from pipelime.sequences import Sample

        return Sample(
            self.action.action(self.action.input_type.value(**x)).model_dump()
        )
