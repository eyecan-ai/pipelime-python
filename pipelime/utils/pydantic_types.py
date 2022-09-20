from __future__ import annotations
import typing as t
from pathlib import Path
import pydantic as pyd

from pipelime.items import Item


yaml_any_type = t.Union[None, str, int, float, bool, t.Mapping[str, t.Any], t.Sequence]


class YamlInput(pyd.BaseModel, extra="forbid", copy_on_model_validation="none"):
    """General yaml/json data (str, number, mapping, list...) optionally loaded from
    a yaml/json file, possibly with key path (format <filepath>[:<key>])."""

    __root__: yaml_any_type

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
        if isinstance(value, (str, bytes)):
            value = str(value)
            filepath, _, root_key = (
                value.rpartition(":") if ":" in value else (value, None, None)
            )
            filepath = Path(filepath)
            if filepath.exists():
                import yaml
                import pydash as py_

                with filepath.open() as f:
                    value = yaml.safe_load(f)
                    if root_key is not None:
                        value = py_.get(value, root_key, default=None)
            return YamlInput(__root__=value)  # type: ignore
        if cls._check_any_type(value):
            return YamlInput(__root__=value)
        raise ValueError(f"Invalid yaml data input: {value}")

    # @classmethod
    # def _check_mapping(cls, value):
    #     for k, v in value.items():
    #         if not isinstance(k, str):
    #             return False
    #         if not cls._check_any_type(v):
    #             return False
    #     return True
    #
    # @classmethod
    # def _check_sequence(cls, value):
    #     for v in value:
    #         if not cls._check_any_type(v):
    #             return False
    #     return True

    @classmethod
    def _check_any_type(cls, value):
        if isinstance(value, (str, int, float, bool, t.Sequence)) or value is None:
            return True
        if isinstance(value, t.Mapping):
            return all(isinstance(k, str) for k in value)


class ItemType(pyd.BaseModel, extra="forbid", copy_on_model_validation="none"):
    """Item type definition."""

    __root__: t.Type[Item]

    @classmethod
    def make_default(cls, itype: t.Any) -> ItemType:
        return cls.validate(itype)

    @property
    def itype(self) -> t.Type[Item]:
        return self.__root__

    def __call__(self, *args, **kwargs) -> Item:
        return self.__root__(*args, **kwargs)

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
        import inspect
        from pipelime.choixe.utils.imports import import_symbol

        if isinstance(value, ItemType):
            return value
        if inspect.isclass(value) and issubclass(value, Item):
            return ItemType(__root__=value)
        if isinstance(value, (str, bytes)):
            value = str(value)
            if "." not in value:
                value = "pipelime.items." + value
            return ItemType(__root__=import_symbol(value))
        raise ValueError(f"Invalid item type: {value}")
