import inspect
import typing as t
import pydantic as pyd
from pydantic.generics import GenericModel
from pipelime.utils.pydantic_types import TypeDef, CallableDef
from pipelime.stages import SampleStage
from pipelime.items import Item

if t.TYPE_CHECKING:
    from pipelime.sequences import Sample

ActionTp = t.TypeVar("ActionTp", bound=t.Callable)


@t.overload
def register_action(
    *, title: t.Optional[str] = None, description: t.Optional[str] = None
) -> t.Callable[[ActionTp], ActionTp]:
    ...


@t.overload
def register_action(__action: ActionTp) -> ActionTp:
    ...


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
    GenericModel,
    t.Generic[ItTp, ValTp],
    extra="forbid",
    copy_on_model_validation="none",
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
        return cls.__fields__["raw_item"].outer_type_

    @classmethod
    def parsed_value_type(cls) -> t.Type[ValTp]:
        return cls.__fields__["parsed_value"].outer_type_

    @classmethod
    def value_to_item_data(cls, value) -> t.Any:
        if hasattr(value, "__to_item_data__"):
            return value.__to_item_data__()
        elif isinstance(value, pyd.BaseModel):
            return value.dict()
        return value

    @classmethod
    def make_raw_item(cls, value) -> ItTp:
        return cls.raw_item_type().make_new(cls.value_to_item_data(value))

    @classmethod
    def make_parsed_value(cls, value) -> ValTp:
        pvtp = cls.parsed_value_type()
        return (  # type: ignore
            pyd.parse_obj_as(pvtp, value)
            if issubclass(pvtp, pyd.BaseModel)
            else pvtp(value)
        )

    @classmethod
    def make_new(cls, value) -> ItTp:
        return cls.make_raw_item(value)

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value):
        if isinstance(value, cls):
            return value
        elif isinstance(value, Item):
            if isinstance(value, cls.raw_item_type()):
                return cls(
                    raw_item=value,
                    parsed_value=cls.make_parsed_value(value()),
                )
        elif isinstance(value, cls.parsed_value_type()):
            return cls(
                raw_item=cls.make_raw_item(value),
                parsed_value=value,
            )
        else:
            try:
                value = cls.make_parsed_value(value)
            except Exception:
                pass
            else:
                return cls(
                    raw_item=cls.make_raw_item(value),
                    parsed_value=value,
                )
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
        class Config(pyd.BaseConfig):
            arbitrary_types_allowed = True

        parser_model = pyd.create_model(
            "DynamicKeyParser",
            __config__=Config,
            **{
                key: (
                    self.item_tp,
                    pyd.Field(
                        self.default,
                        default_factory=self.default_factory,
                        **self.extra_kwargs,
                    ),
                )
            },
        )
        parsed_values = parser_model.parse_obj(
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


class BaseEntity(
    pyd.BaseModel,
    extra="allow",
    copy_on_model_validation="none",
    arbitrary_types_allowed=True,
):
    """The base class for all input/output entity models."""

    def __init__(self, **data):
        # create an item field from raw values
        # NB: if the field is optional, it can be None
        for k, v in data.items():
            if k in self.__fields__ and not isinstance(v, Item):
                k_field = self.__fields__[k]
                if issubclass(k_field.outer_type_, Item) and (
                    k_field.required or v is not None
                ):
                    data[k] = self.__fields__[k].outer_type_.make_new(v)
        super().__init__(**data)

        # assign self as the owner of dynamic key fields
        for k in self.__private_attributes__:
            v = getattr(self, k)
            if isinstance(v, ModelDynamicKey):
                v.owner = self

    def _iter(self, *args, **kwargs):
        # skip None fields and bypass ParsedItem
        for k, v in super()._iter(*args, **kwargs):
            if v is not None:
                if isinstance(v, t.Mapping) and "raw_item" in v:
                    if v["raw_item"] is not None:
                        yield k, v["raw_item"]
                else:
                    yield k, v

    @classmethod
    def merge(
        cls: t.Type[DerivedEntityTp], __other__: "BaseEntity", /, **kwargs
    ) -> DerivedEntityTp:
        """Creates a new entity by merging `other` entity with extra `kwargs` fields."""
        other_dict = __other__.dict()
        for k, v in kwargs.items():
            if not isinstance(v, Item):
                # NB: None is a valid value for optional fields
                my_k_field = cls.__fields__.get(k, None)
                if not my_k_field or my_k_field.required or v is not None:
                    # user has no preference on the actual Item class,
                    # so keep the original item type if it is a subclass
                    # of the declared output item type
                    if k in other_dict:
                        other_item_obj = other_dict[k]
                        if other_item_obj is not None:
                            my_item_cls = Item
                            if my_k_field:
                                my_item_cls = my_k_field.outer_type_
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

    @classmethod
    def __get_validators__(cls):
        yield cls.validate_action

    @classmethod
    def validate_action(
        cls,
        value: t.Union[
            CallableDef, t.Callable, str, t.Mapping[t.Union[str, t.Callable], t.Any]
        ],
    ) -> "ActionDef":
        from pipelime.cli.utils import PipelimeSymbolsHelper

        if isinstance(value, cls):
            return value
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
        return cls.validate(value)


class BaseEntityType(TypeDef[BaseEntity]):
    """An entity type. It accepts both type names and string."""


class EntityAction(pyd.BaseModel, extra="forbid", copy_on_model_validation="none"):
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
        description=(
            "The input type of the action (can be a string). If None, "
            "it is inferred from the callable's annotations or set to BaseEntity."
        ),
    )

    @pyd.validator("action")
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

    @pyd.validator("input_type", always=True)
    def validate_input_type(cls, v, values):
        if "action" in values:  # if not True, an error should have been raised yet
            action_t = values["action"].args_type[0]
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

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value):
        if isinstance(value, cls):
            return value
        if not isinstance(value, t.Mapping):
            value = {"action": value}
        if "action" not in value:
            value = {"action": value}
        return cls(**value)


class StageEntity(SampleStage, title="entity"):
    __root__: EntityAction = pyd.Field(..., description="The entity action to run.")

    def __init__(self, __root__: EntityAction, **data):
        super().__init__(__root__=__root__, **data)  # type: ignore

    def __call__(self, x: "Sample") -> "Sample":
        from pipelime.sequences import Sample

        return Sample(self.__root__.action(self.__root__.input_type.value(**x)).dict())
