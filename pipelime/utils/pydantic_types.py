from __future__ import annotations
import typing as t
import inspect
from pathlib import Path
import pydantic as pyd
import pydantic.generics as pydg
import numpy as np

from pipelime.items import Item

if t.TYPE_CHECKING:
    from pipelime.sequences import SamplesSequence
    from numpy.typing import ArrayLike


class NewPath(Path):
    """A path that does not exist yet."""

    @classmethod
    def __modify_schema__(cls, field_schema: t.Dict[str, t.Any]) -> None:
        field_schema.update(format="new-path")

    @classmethod
    def __get_validators__(cls):
        from pydantic.validators import path_validator

        yield path_validator
        yield cls.validate

    @classmethod
    def validate(cls, value: Path) -> Path:
        if value.exists():
            raise ValueError(f"Path {value} already exists")
        return value


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
            # assert k == "__root__"
            # assert isinstance(v, np.ndarray)
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
        if isinstance(value, cls):
            return value
        try:
            return cls(
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
        if isinstance(value, cls):
            return value
        if isinstance(value, (str, Path)):
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
            return cls(__root__=value)  # type: ignore
        if cls._check_any_type(value):
            return cls(__root__=value)
        raise ValueError(f"Invalid yaml data input: {value}")

    @classmethod
    def _check_any_type(cls, value):
        if isinstance(value, (str, int, float, bool, t.Sequence)) or value is None:
            return True
        if isinstance(value, t.Mapping):
            return all(isinstance(k, str) for k in value)


TRoot = t.TypeVar("TRoot")


class TypeDef(
    pydg.GenericModel,
    t.Generic[TRoot],
    extra="forbid",
    copy_on_model_validation="none",
    allow_mutation=False,
):
    """Generic type definition. It accepts both type names and string.
    You should derive from this class to define your own type definitions and,
    possibly, re-implement the `default_class_path` class method
    (NB: it must end with `.`). When a string is given, it can be a class path
    (the default class path can be omitted) or a `path/to/file.py:ClassName`.

    Examples:
        Create a new FooType definition::

            class FooType(TypeDef[Foo]):
                @classmethod
                def default_class_path(cls) -> str:
                    return "my_package.types."

        Now you can instantiate it::

            it = FooType.create(a.class.path.DerivedFromFoo)
            it = FooType.create("FooSubclassInMyPackage")

        Access the internal type and instantiate a new object::

            it = FooType.create(a.class.path.DerivedFromFoo)
            it.value  # DerivedFromFoo
            _ = it()  # new instance of DerivedFromFoo

        Serialize to dict or json::

            it_dict = it.dict()
            it_json_str = it.json()

        Get the object back from dict or json::

            it_again = pydantic.parse_obj_as(FooType, it_dict["__root__"])
            it_again = pydantic.parse_raw_as(FooType, it_json_str)

        Use this type within another model::

            class MyModel(pydantic.BaseModel):
                foo_type: FooType = pydantic.Field(
                    default_factory=lambda: FooType.create("DefaultFooSubclass")
                )

        Everything still works::

            mm = MyModel()
            mm = MyModel.parse_obj({"foo_type": a.class.path.DerivedFromFoo})
            mm = MyModel.parse_obj({"foo_type": "FooSubclassInMyPackage"})
            mm_again = pydantic.parse_obj_as(MyModel, mm.dict())
    """

    __root__: t.Type[TRoot]

    @classmethod
    def default_class_path(cls) -> str:
        return "__main__."

    @classmethod
    def wrapped_type(cls) -> t.Type[TRoot]:
        return t.get_args(cls.__fields__["__root__"].outer_type_)[0]

    @classmethod
    def create(cls, value: t.Union[TypeDef, t.Type[TRoot], str]) -> TypeDef:
        return cls.validate(value)

    @property
    def value(self) -> t.Type[TRoot]:
        return self.__root__

    def _iter(self, *args, **kwargs):
        for k, v in super()._iter(*args, **kwargs):
            # assert k == "__root__"
            # assert issubclass(v, self.wrapped_type())
            yield k, self._type_to_string(v)

    @classmethod
    def _type_to_string(cls, type_: t.Type[TRoot]) -> str:
        full_name = type_.__module__ + "." + type_.__qualname__
        redux_name = (
            full_name[len(cls.default_class_path()) :]
            if full_name.startswith(cls.default_class_path())
            else full_name
        )
        return full_name if "." in redux_name else redux_name

    @classmethod
    def _string_to_type(cls, type_str: str) -> t.Type[TRoot]:
        from pipelime.choixe.utils.imports import import_symbol

        type_str = type_str.strip("\"'")
        if "." not in type_str:
            type_str = cls.default_class_path() + type_str
        return import_symbol(type_str)

    def __call__(self, *args, **kwargs) -> TRoot:
        return self.__root__(*args, **kwargs)

    def __hash__(self) -> int:
        return hash(self.__root__)

    def __str__(self) -> str:
        return self._type_to_string(self.__root__)

    def __repr__(self) -> str:
        return self.__piper_repr__()

    def __piper_repr__(self) -> str:
        return repr(self.__root__)

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value: t.Union[TypeDef, t.Type[TRoot], str]) -> TypeDef:
        import inspect

        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            value = cls._string_to_type(value)
        if inspect.isclass(value) and issubclass(value, cls.wrapped_type()):
            return cls(__root__=value)
        raise ValueError(f"Type `{value}` is not a subclass of `{cls.wrapped_type()}`")


class ItemType(TypeDef[Item]):
    """Item type definition. It accepts both type names and string.
    The default class path is `pipelime.items`.
    """

    @classmethod
    def default_class_path(cls) -> str:
        return "pipelime.items."


class CallableDef(
    pyd.BaseModel, extra="forbid", copy_on_model_validation="none", allow_mutation=False
):
    """Generic callable definition. It accepts functions and callable classes.
    You may derive from this class to re-implement the `default_class_path` class method
    (NB: it must end with `.`).

    Can be created from a symbol, an instance, a class/file path to a function or
    a mapping where the key is the class/file path to a class and the value is the
    list of __init__ arguments (mapping, sequence or single value).

    To ease the inspection of the callable, the methods `full_signature`, `args_type`,
    `return_type`, `has_var_positional` and `has_var_keyword` are provided.

    Examples:
        Create a new CallableDef::

            cdef = CallableDef.create(a.class.path.to.callable)
            cdef = CallableDef.create("CallableInMain")

        Access the internal value and call it::

            cdef = CallableDef.create(a.class.path.to.callable)
            cdef.value  # callable
            _ = cdef()  # call the callable

        Serialize to dict or json::

            cdef_dict = cdef.dict()
            cdef_json_str = cdef.json()

        Get the object back from dict or json::

            cdef_again = pydantic.parse_obj_as(CallableDef, cdef_dict["__root__"])
            cdef_again = pydantic.parse_raw_as(CallableDef, cdef_json_str)

        Use this type within another model::

            class MyModel(pydantic.BaseModel):
                fn: CallableDef = pydantic.Field(
                    default_factory=lambda: CallableDef.create("CallableInMain")
                )

        Everything still works::

            mm = MyModel()
            mm = MyModel.parse_obj({"fn": a.class.path.to.callable})
            mm = MyModel.parse_obj({"fn": "CallableInMain"})
            mm_again = pydantic.parse_obj_as(MyModel, mm.dict())
    """

    __root__: t.Callable

    @classmethod
    def default_class_path(cls) -> str:
        return "__main__."

    @classmethod
    def create(cls, value: t.Union[CallableDef, t.Callable, str]) -> CallableDef:
        return cls.validate(value)

    @property
    def value(self) -> t.Callable:
        return self.__root__

    @property
    def full_signature(self) -> inspect.Signature:
        return inspect.signature(self.__root__)

    @property
    def args(self) -> t.Sequence[inspect.Parameter]:
        return list(self.full_signature.parameters.values())

    @property
    def args_type(self) -> t.Sequence[t.Optional[t.Type]]:
        return [
            None if p.annotation is inspect.Signature.empty else p.annotation
            for p in self.full_signature.parameters.values()
        ]

    @property
    def has_var_positional(self) -> bool:
        return any(
            p.kind is p.VAR_POSITIONAL for p in self.full_signature.parameters.values()
        )

    @property
    def has_var_keyword(self) -> bool:
        return any(
            p.kind is p.VAR_KEYWORD for p in self.full_signature.parameters.values()
        )

    @property
    def return_type(self) -> t.Optional[t.Type]:
        rt = self.full_signature.return_annotation
        return None if rt is inspect.Signature.empty else rt

    def _iter(self, *args, **kwargs):
        for k, v in super()._iter(*args, **kwargs):
            # assert k == "__root__"
            yield k, self._callable_to_string(v)

    @classmethod
    def _callable_to_string(cls, clb: t.Callable) -> str:
        full_name = clb.__module__ + "." + clb.__qualname__
        redux_name = (
            full_name[len(cls.default_class_path()) :]
            if full_name.startswith(cls.default_class_path())
            else full_name
        )
        return full_name if "." in redux_name else redux_name

    @classmethod
    def _string_to_callable(cls, clb_str: str) -> t.Callable:
        from pipelime.choixe.utils.imports import import_symbol

        clb_str = clb_str.strip("\"'")
        if "." not in clb_str:
            clb_str = cls.default_class_path() + clb_str
        return import_symbol(clb_str)

    def __call__(self, *args, **kwargs):
        return self.__root__(*args, **kwargs)

    def __hash__(self) -> int:
        return hash(self.__root__)

    def __str__(self) -> str:
        return self._callable_to_string(self.__root__)

    def __repr__(self) -> str:
        return self.__piper_repr__()

    def __piper_repr__(self) -> str:
        return repr(self.__root__)

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(
        cls,
        value: t.Union[
            CallableDef, t.Callable, str, t.Mapping[t.Union[str, t.Callable], t.Any]
        ],
    ):
        if isinstance(value, cls):
            return value
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
            return cls(__root__=value)
        raise ValueError(f"Invalid callable: {value}")


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
