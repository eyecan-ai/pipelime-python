import json
import yaml
import toml
from io import TextIOWrapper

import typing as t

from pipelime.items import Item
from pipelime.items.base import deferred_classattr

_metadata_type = t.Union[str, int, float, bool, None, t.Sequence, t.Mapping]


class MetadataItem(Item[_metadata_type]):
    """A common base for items dealing with metadata."""

    @deferred_classattr
    def default_concrete(cls):
        return YamlMetadataItem

    @classmethod
    def validate(cls, raw_data: t.Any) -> _metadata_type:
        if not isinstance(
            raw_data, (str, int, float, bool, type(None), t.Sequence, t.Mapping)
        ):
            raise ValueError(  # pragma: no cover
                f"{cls}: raw data must be one of"
                " (str, int, float, bool, None, Sequence, Mapping)."
            )
        return raw_data

    @classmethod
    def pl_pretty_data(cls, value: _metadata_type) -> t.Any:
        from rich.pretty import Pretty

        return Pretty(value, indent_guides=True, expand_all=True)


class JsonMetadataItem(MetadataItem):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".json",)

    @classmethod
    def decode(cls, fp: t.BinaryIO) -> _metadata_type:
        return json.load(TextIOWrapper(fp))

    @classmethod
    def encode(cls, value: _metadata_type, fp: t.BinaryIO):
        json.dump(value, TextIOWrapper(fp))


class YamlMetadataItem(MetadataItem):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".yaml", ".yml")

    @classmethod
    def decode(cls, fp: t.BinaryIO) -> _metadata_type:
        return yaml.safe_load(TextIOWrapper(fp))

    @classmethod
    def encode(cls, value: _metadata_type, fp: t.BinaryIO):
        yaml.safe_dump(value, TextIOWrapper(fp), sort_keys=False)


class TomlMetadataItem(MetadataItem):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return (".toml", ".tml")

    @classmethod
    def decode(cls, fp: t.BinaryIO) -> t.Mapping:
        return toml.load(TextIOWrapper(fp))

    @classmethod
    def encode(cls, value: t.Mapping, fp: t.BinaryIO):
        toml.dump(value, TextIOWrapper(fp))

    @classmethod
    def validate(cls, raw_data: t.Any) -> _metadata_type:
        if not isinstance(raw_data, t.Mapping):
            raise ValueError(f"{cls}: raw data must be a Mapping")  # pragma: no cover
        return raw_data
