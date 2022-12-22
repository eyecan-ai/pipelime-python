import inspect
import typing as t
import pydantic as pyd
from pydantic.generics import GenericModel
from pipelime.utils.pydantic_types import TypeDef, CallableDef
from pipelime.stages import SampleStage
from pipelime.items import Item

if t.TYPE_CHECKING:
    from pipelime.sequences import Sample


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

    def __call__(self):
        return self.parsed_value

    @classmethod
    def raw_item_type(cls) -> t.Type[ItTp]:
        return cls.__fields__["raw_item"].outer_type_

    @classmethod
    def parsed_value_type(cls) -> t.Type[ValTp]:
        return cls.__fields__["parsed_value"].outer_type_

    @classmethod
    def value_to_item_data(cls, value) -> t.Any:
        if hasattr(value, "to_item_data"):
            return value.to_item_data()
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


DerivedEntityTp = t.TypeVar("DerivedEntityTp", bound="BaseEntity")


class BaseEntity(
    pyd.BaseModel,
    extra="allow",
    copy_on_model_validation="none",
    arbitrary_types_allowed=True,
):
    """The base class for all input/output entity models."""

    def __init__(self, **data):
        for k, v in data.items():
            # create an item field from raw values
            if (
                k in self.__fields__
                and not isinstance(v, Item)
                and issubclass(self.__fields__[k].outer_type_, Item)
            ):
                data[k] = self.__fields__[k].outer_type_.make_new(v)
        super().__init__(**data)

    def _iter(self, *args, **kwargs):
        for k, v in super()._iter(*args, **kwargs):
            if isinstance(v, t.Mapping):
                yield k, v["raw_item"]
            else:
                yield k, v

    @classmethod
    def merge(
        cls: t.Type[DerivedEntityTp], other: "BaseEntity", **kwargs
    ) -> DerivedEntityTp:
        """Creates a new entity by merging `other` entity with extra `kwargs` fields."""
        other_dict = other.dict()
        for k, v in kwargs.items():
            if not isinstance(v, Item) and k in other_dict:
                # v is a raw or parsed value and k was in `other`,
                # so keep the same type
                other_item = other_dict[k]
                v = ParsedItem.value_to_item_data(v)
                kwargs[k] = other_item.make_new(v)

        return cls(**{**other_dict, **kwargs})


class BaseEntityType(TypeDef[BaseEntity]):
    """An entity type. It accepts both type names and string."""


class EntityAction(pyd.BaseModel, extra="forbid", copy_on_model_validation="none"):
    """An action and its associated input entity model."""

    action: CallableDef = pyd.Field(
        ...,
        description=(
            "The action callable to run (can be a class path). The expected signature "
            "is (BaseEntity) -> BaseEntityLike (ie, the return type should provide "
            "a compatible interface). If properly annotated, input_type "
            "definition could be skipped."
        ),
    )
    input_type: BaseEntityType = pyd.Field(
        None,
        description=(
            "The input type of the action (can be a string). If None, "
            "it is inferred from the callable's annotations."
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
        if "action" in values:  # if not True, an error has been raised yet
            action = values["action"]
            if v is None:
                if action.args_type[0] is None:
                    raise ValueError(
                        "Cannot infer input model type because no annotation "
                        "has been found for the first argument of the action."
                    )
                v = BaseEntityType.create(action.args_type[0])
            else:
                first_tp = action.args_type[0]
                if first_tp is not None and not issubclass(first_tp, v.value):
                    raise ValueError(
                        "The first argument of the action is not compatible "
                        "with the given input model type"
                    )
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
        return cls(**value)


class StageEntity(SampleStage, title="entity"):
    __root__: EntityAction = pyd.Field(..., description="The entity action to run.")

    def __init__(self, __root__: EntityAction, **data):
        super().__init__(__root__=__root__, **data)  # type: ignore

    def __call__(self, x: "Sample") -> "Sample":
        from pipelime.sequences import Sample

        return Sample(self.__root__.action(self.__root__.input_type.value(**x)).dict())
