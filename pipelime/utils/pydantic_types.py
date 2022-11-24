from __future__ import annotations
import typing as t
from pathlib import Path
import pydantic as pyd
import numpy as np

from pipelime.items import Item

if t.TYPE_CHECKING:
    from pipelime.sequences import SamplesSequence
    from numpy.typing import ArrayLike


class NumpyType(
    pyd.BaseModel,
    extra="forbid",
    copy_on_model_validation="none",
    arbitrary_types_allowed=True,
):
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

            npt_dict = npt.dict()
            npt_json_str = npt.json()

        Get the object back from dict or json::

            npt_again = pydantic.parse_obj_as(NumpyType, npt_dict["__root__"])
            npt_again = pydantic.parse_raw_as(NumpyType, npt_json_str)

        Use this type within another model::

            class MyModel(pydantic.BaseModel):
                tensor: NumpyType = pydantic.Field(
                    default_factory=lambda: NumpyType.create([1, 2, 3])
                )

        Everything still works::

            mm = MyModel()
            mm = MyModel(tensor=np.array([1,2,3]))
            mm = MyModel.parse_obj({"tensor": [1, 2, 3]})
            mm = MyModel.parse_obj({"tensor": {"object": [1,2,3], "dtype": "float32"}})
            mm_again = pydantic.parse_obj_as(MyModel, mm.dict())
    """

    __root__: np.ndarray

    @classmethod
    def create(
        cls, value: t.Union[NumpyType, "ArrayLike", t.Mapping[str, t.Any]]
    ) -> NumpyType:
        return cls.validate(value)

    @property
    def value(self):
        return self.__root__

    def _iter(self, *args, **kwargs):
        for k, v in super()._iter(*args, **kwargs):
            assert k == "__root__"
            assert isinstance(v, np.ndarray)
            v_order = {} if v.flags["C_CONTIGUOUS"] else {"order": "F"}
            yield k, {"object": v.tolist(), "dtype": v.dtype.name, **v_order}

    def __str__(self) -> str:
        return str(self.__root__)

    def __repr__(self) -> str:
        return self.__piper_repr__()

    def __piper_repr__(self) -> str:
        return repr(self.__root__)

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value):
        if isinstance(value, NumpyType):
            return value
        try:
            return NumpyType(
                __root__=(
                    np.array(**value)
                    if isinstance(value, t.Mapping)
                    else np.array(value)
                )
            )
        except Exception as e:
            raise ValueError(f"Invalid numpy input: {value}") from e


yaml_any_type = t.Union[
    None,
    pyd.StrictBool,
    pyd.StrictInt,
    pyd.StrictFloat,
    pyd.StrictStr,
    t.Mapping[str, t.Any],
    t.Sequence,
]


class YamlInput(pyd.BaseModel, extra="forbid", copy_on_model_validation="none"):
    """General yaml/json data (str, number, mapping, list...) optionally loaded from
    a yaml/json file, possibly with key path (format <filepath>[:<key>]).

    Examples:
        Create a new YamlInput from local data::

            yml = YamlInput.create(None)
            yml = YamlInput.create("some string")
            yml = YamlInput.create(4.2)
            yml = YamlInput.create([1, True, "string"])
            yml = YamlInput.create({"a": 1, "b": {"c": [1, 2, 3]}})

        Create a new YamlInput from a file::

            yml = YamlInput.create("path/to/file.yaml")
            yml = YamlInput.create("path/to/file.json")

        Create a new YamlInput from a pydash-like address within a file::

            yml = YamlInput.create("path/to/file.yaml:my.key.path")
            yml = YamlInput.create("path/to/file.json:my.key.path")

        Access the data::

            yml.value

        Serialize to dict or json::

            yml_dict = yml.dict()
            yml_json_str = yml.json()

        Get the object back from dict or json::

            yml_again = pydantic.parse_obj_as(YamlInput, yml_dict["__root__"])
            yml_again = pydantic.parse_raw_as(YamlInput, yml_json_str)

        Use this type within another model::

            class MyModel(pydantic.BaseModel):
                config: YamlInput = pydantic.Field(
                    default_factory=lambda: YamlInput.create([1, 2, 3])
                )

        Everything still works::

            mm = MyModel()
            mm = MyModel(config=[4, 5, 6])
            mm = MyModel.parse_obj({"config": [4, 5, 6]})
            mm_again = pydantic.parse_obj_as(MyModel, mm.dict())
    """

    __root__: yaml_any_type

    @classmethod
    def create(cls, value: t.Union[YamlInput, yaml_any_type]) -> YamlInput:
        return cls.validate(value)

    @property
    def value(self):
        return self.__root__

    def __str__(self) -> str:
        return str(self.__root__)

    def __repr__(self) -> str:
        return self.__piper_repr__()

    def __piper_repr__(self) -> str:
        return repr(self.__root__)

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value):
        if isinstance(value, YamlInput):
            return value
        if isinstance(value, str):
            pval = Path(value)
            filepath, _, root_key = pval.name.partition(":")
            filepath = Path(pval.parent / filepath)
            if filepath.exists():
                import yaml
                import pydash as py_

                with filepath.open() as f:
                    value = yaml.safe_load(f)
                    if root_key:
                        value = py_.get(value, root_key, default=None)
            return YamlInput(__root__=value)  # type: ignore
        if cls._check_any_type(value):
            return YamlInput(__root__=value)
        raise ValueError(f"Invalid yaml data input: {value}")

    @classmethod
    def _check_any_type(cls, value):
        if isinstance(value, (str, int, float, bool, t.Sequence)) or value is None:
            return True
        if isinstance(value, t.Mapping):
            return all(isinstance(k, str) for k in value)


class ItemType(
    pyd.BaseModel, extra="forbid", copy_on_model_validation="none", allow_mutation=False
):
    """Item type definition. It accepts both type names and string.
    When a string is given, it can be a class path (`pipelime.items` can be omitted)
    or a `path/to/file.py:ClassName`.

    Examples:
        Create a new ItemType instance::

            it = ItemType.create(pipelime.items.ImageItem)
            it = ItemType.create("ImageItem")

        Access the internal type::

            it.value  # item type

        Serialize to dict or json::

            it_dict = it.dict()
            it_json_str = it.json()

        Get the object back from dict or json::

            it_again = pydantic.parse_obj_as(ItemType, it_dict["__root__"])
            it_again = pydantic.parse_raw_as(ItemType, it_json_str)

        Use this type within another model::

            class MyModel(pydantic.BaseModel):
                item_type: ItemType = pydantic.Field(
                    default_factory=lambda: ItemType.create("ImageItem")
                )

        Everything still works::

            mm = MyModel()
            mm = MyModel.parse_obj({"item_type": pipelime.items.MetadataItem})
            mm = MyModel.parse_obj({"item_type": "MetadataItem"})
            mm_again = pydantic.parse_obj_as(MyModel, mm.dict())
    """

    __root__: t.Type[Item]

    @classmethod
    def create(cls, value: t.Union[ItemType, t.Type[Item], str]) -> ItemType:
        return cls.validate(value)

    @property
    def value(self) -> t.Type[Item]:
        return self.__root__

    def _iter(self, *args, **kwargs):
        for k, v in super()._iter(*args, **kwargs):
            assert k == "__root__"
            assert issubclass(v, Item)
            yield k, ItemType._item_type_to_string(v)

    @staticmethod
    def _item_type_to_string(item_type: t.Type[Item]) -> str:
        return (
            item_type.__name__
            if item_type.__module__.startswith("pipelime.items")
            else item_type.__module__ + "." + item_type.__qualname__
        )

    @staticmethod
    def _string_to_item_type(item_type_str: str) -> t.Type[Item]:
        from pipelime.choixe.utils.imports import import_symbol

        if "." not in item_type_str:
            item_type_str = "pipelime.items." + item_type_str
        return import_symbol(item_type_str)

    def __call__(self, *args, **kwargs) -> Item:
        return self.__root__(*args, **kwargs)

    def __hash__(self) -> int:
        return hash(self.__root__)

    def __str__(self) -> str:
        return ItemType._item_type_to_string(self.__root__)

    def __repr__(self) -> str:
        return self.__piper_repr__()

    def __piper_repr__(self) -> str:
        return repr(self.__root__)

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value):
        import inspect

        if isinstance(value, ItemType):
            return value
        if inspect.isclass(value) and issubclass(value, Item):
            return ItemType(__root__=value)
        if isinstance(value, str):
            return ItemType(__root__=ItemType._string_to_item_type(value))
        raise ValueError(f"Invalid item type: {value}")


# This is defined here to make it picklable
def _identity_fn_helper(x):
    return x


class ItemValidationModel(
    pyd.BaseModel, extra="forbid", copy_on_model_validation="none"
):
    """Item schema validation."""

    class_path: ItemType = pyd.Field(
        ...,
        description=(
            "The item class path. The default package `pipelime.item` can be omitted"
        ),
    )
    is_optional: bool = pyd.Field(
        True, description="Whether the item is required or optional."
    )
    is_shared: bool = pyd.Field(False, description="Whether the item is shared or not.")
    validator_: t.Optional[str] = pyd.Field(
        None,
        description=(
            "A class path to a callable accepting the item value and either returning "
            "a validated value or raising an exception in case of error."
        ),
        alias="validator",
    )

    _validator_callable = pyd.PrivateAttr()

    def __init__(self, **data):
        from pipelime.choixe.utils.imports import import_symbol

        super().__init__(**data)
        self._validator_callable = (
            import_symbol(self.validator_) if self.validator_ else _identity_fn_helper
        )

    def make_field(self, key_name: str):
        return (
            self.class_path.value,
            pyd.Field(default_factory=self.class_path.value, alias=key_name)
            if self.is_optional
            else pyd.Field(..., alias=key_name),
        )

    def make_validator_method(self, field_name: str):
        import uuid

        # we need random names and dynamic function creation
        # to avoid reusing the same function name for validators
        # (yes, pydantic is really pedantic...)
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
        return pyd.validator(field_name)(fn_helper)


class SampleValidationInterface(
    pyd.BaseModel, extra="forbid", copy_on_model_validation="none"
):
    """Sample schema validation."""

    sample_schema: t.Union[
        t.Type[pyd.BaseModel], str, t.Mapping[str, ItemValidationModel]
    ] = pyd.Field(
        ...,
        description=(
            "The sample schema to validate, ie, a mapping from sample keys to expected "
            "item types.\nThe schema can be a pydantic Model or a class path to a "
            "pydantic model to import, where fields' names are the sample keys, "
            "while fields' values are the item types. Otherwise, an explicit "
            "`key-name: ItemValidationModel` mapping must be provided."
        ),
    )
    ignore_extra_keys: bool = pyd.Field(
        True,
        description=(
            "When `sample_schema` is an explicit mapping, if `ignore_extra_keys` is "
            "True, unexpected keys are ignored. Otherwise an error is raised."
        ),
    )
    lazy: bool = pyd.Field(
        True, description="If True, samples will be validated only when accessed."
    )
    max_samples: int = pyd.Field(
        1,
        description=(
            "When the validation is NOT lazy, "
            "only the slice `[0:max_samples]` is checked. "
            "Set to 0 to check all the samples."
        ),
    )

    _schema_model: t.Optional[t.Type[pyd.BaseModel]] = pyd.PrivateAttr(None)

    def _import_schema(self, schema_path: str):
        from pipelime.choixe.utils.imports import import_symbol

        imported_schema = import_symbol(schema_path)
        return imported_schema

    def _make_schema(self, schema_def: t.Mapping[str, ItemValidationModel]):
        class Config(pyd.BaseConfig):
            arbitrary_types_allowed = True
            extra = pyd.Extra.ignore if self.ignore_extra_keys else pyd.Extra.forbid

        def _safe_name(k):
            return f"{k}___"

        _item_map = {_safe_name(k): v.make_field(k) for k, v in schema_def.items()}
        _validators = {
            f"validate_{k}": v.make_validator_method(_safe_name(k))
            for k, v in schema_def.items()
        }

        return pyd.create_model(
            "SampleSchema",
            __config__=Config,
            __validators__=_validators,
            **_item_map,
        )

    @property
    def schema_model(self) -> t.Type[pyd.BaseModel]:
        sm = self._schema_model
        if sm is None:
            if isinstance(self.sample_schema, str):
                sm = self._import_schema(self.sample_schema)
            elif isinstance(self.sample_schema, t.Mapping):
                sm = self._make_schema(self.sample_schema)
            else:
                sm = self.sample_schema

            if not issubclass(sm, pyd.BaseModel):
                raise ValueError(f"`{self.sample_schema}` is not a pydantic model.")

            # cache the model for later use
            if self.lazy:
                self._schema_model = sm
        return sm

    def append_validator(self, sequence: "SamplesSequence") -> "SamplesSequence":
        return sequence.validate_samples(sample_schema=self)

    def as_pipe(self):
        return {"validate_samples": {"sample_schema": self.dict(by_alias=True)}}
