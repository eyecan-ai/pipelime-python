import inspect
import typing as t
import pydantic as pyd
from pipelime.utils.pydantic_types import TypeDef, CallableDef
from pipelime.stages import SampleStage

if t.TYPE_CHECKING:
    from pipelime.sequences import Sample


class BaseEntity(
    pyd.BaseModel,
    extra="allow",
    copy_on_model_validation="none",
    arbitrary_types_allowed=True,
):
    """The base class for all input/output entity models."""

    @classmethod
    def merge_with(cls, other: "BaseEntity", **kwargs) -> "BaseEntity":
        """Creates a new entity by merging `other` entity with extra `kwargs` fields."""
        return cls(**{**other.dict(), **kwargs})


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

    def __init__(self, __root__, **data):
        super().__init__(__root__=__root__, **data)  # type: ignore

    def __call__(self, x: "Sample") -> "Sample":
        from pipelime.sequences import Sample

        return Sample(self.__root__.action(self.__root__.input_type(**x)).dict())
