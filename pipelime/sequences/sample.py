from __future__ import annotations

import copy
import re
import typing as t

from pipelime.items import Item


class SamplePathRegex:
    KEY_PATH_REGEX = re.compile(r"(?<!\\)(?:\\\\)*\.|(\[\d+\])")

    @classmethod
    def split(cls, key_path: str) -> t.Tuple[str, str]:
        key = cls.KEY_PATH_REGEX.split(key_path)[0]
        path = key_path[len(key) :]  # noqa: E203
        key = key.replace(r"\\", "\\").replace(r"\.", r".")
        return key, path


class sample_schema:
    def __init__(self, data: t.Mapping[str, t.Mapping[str, t.Any]]):
        self._schema = data

    def __call__(self, sample: Sample) -> bool:
        return self._schema == sample.to_schema()._schema

    def __repr__(self) -> str:
        return f"<sample_schema {repr(self._schema)}>"

    def __str__(self) -> str:
        return repr(self)


class Sample(t.Mapping[str, Item]):
    """A Sample is a mapping from keys to Items. Any modification creates a new instance
    which is a shallow copy of the original Sample, ie, Item instances are shared, but
    the internal data mapping is not.
    """

    _data: t.Mapping[str, Item]

    def __init__(self, data: t.Optional[t.Mapping[str, Item]]):
        super().__init__()
        self._data = data if data is not None else {}

    def to_schema(self) -> sample_schema:
        return sample_schema({k: v.to_schema() for k, v in self._data.items()})

    def to_dict(self) -> t.Dict[str, t.Any]:
        return {k: v() for k, v in self.items()}

    def shallow_copy(self) -> Sample:
        return Sample(copy.copy(self._data))

    def deep_copy(self) -> Sample:
        return Sample(copy.deepcopy(self._data))

    def set_item(self, key: str, value: Item) -> Sample:
        new_data = dict(self._data)
        new_data[key] = value
        return Sample(new_data)

    def set_value_as(
        self,
        target_key: str,
        reference_key: str,
        value: t.Any,
        shared_item: t.Optional[bool] = None,
    ) -> Sample:
        ref_item = self._data[reference_key]
        new_data = dict(self._data)
        new_data[target_key] = ref_item.make_new(
            value, shared=ref_item.is_shared if shared_item is None else shared_item
        )
        return Sample(new_data)

    def set_value(self, key: str, value: t.Any) -> Sample:
        return self.set_value_as(key, key, value)

    def deep_set(self, key_path: str, value: t.Any) -> Sample:
        r"""Sets a value of an Item of this Sample through a path similar to
        `pydash.set_`. The path is built by splitting the mapping keys by `.`
        and enclosing list indexes within `[]`. Use `\` to escape the `.` character.

        Example::

            sample = Sample({"first": JsonMetadataItem({r"j.names\": ["Jo", "Jane"]})})
            sample.deep_set(r"first.j\.names\\[1]", "Jane Doe")
        """
        import pydash as py_

        key, path = SamplePathRegex.split(key_path)
        if not path:
            return self.set_value(key, value)

        new_value = copy.deepcopy(self._data[key]())
        py_.set_(new_value, path, value)
        return self.set_value(key, new_value)

    def deep_get(self, key_path: str, default: t.Any = None) -> t.Any:
        r"""Gets a value from the Sample through a key path similar to `pydash.get`.
        The path is built by splitting the mapping keys by `.` and enclosing list
        indexes within `[]`. Use `\` to escape the `.` character.

        Example::

            sample = Sample({"first": JsonMetadataItem({r"j.names\": ["Jo", "Jane"]})})
            jane = sample.deep_get(r"first.j\.names\\[1]")
        """
        import pydash as py_

        key, path = SamplePathRegex.split(key_path)
        if key not in self._data:
            return default

        value = self._data[key]()
        if path:
            value = py_.get(value, path, default)
        return value

    def change_key(self, old_key: str, new_key: str, delete_old_key: bool) -> Sample:
        if new_key not in self._data and old_key in self._data:
            new_data = dict(self._data)
            new_data[new_key] = new_data[old_key]
            if delete_old_key:
                del new_data[old_key]
            return Sample(new_data)
        return self

    def duplicate_key(self, reference_key: str, new_key: str) -> Sample:
        return self.change_key(reference_key, new_key, False)

    def rename_key(self, old_key: str, new_key: str) -> Sample:
        return self.change_key(old_key, new_key, True)

    def remove_keys(self, *key_to_remove: str) -> Sample:
        return Sample({k: v for k, v in self._data.items() if k not in key_to_remove})

    def extract_keys(self, *keys_to_keep: str) -> Sample:
        return Sample({k: v for k, v in self._data.items() if k in keys_to_keep})

    def merge(self, other: "Sample") -> Sample:
        return Sample({**self._data, **other._data})

    def update(self, other: "Sample") -> Sample:
        return self.merge(other)

    def __getitem__(self, key: str) -> Item:
        return self._data[key]

    def __iter__(self) -> t.Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def __repr__(self) -> str:
        return f"{self.__class__}{repr(self._data)}"

    def __str__(self) -> str:
        data_str = [f"  [{k}] {str(v)}" for k, v in self._data.items()]
        return "\n".join([f"{self.__class__.__name__}"] + data_str)
