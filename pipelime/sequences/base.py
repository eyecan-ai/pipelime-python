from abc import abstractmethod
import copy
import itertools
import re
import typing as t

from pipelime.items import Item


class SamplePathRegex:
    KEY_PATH_REGEX = re.compile(r"(?<!\\)(?:\\\\)*\.|(\[\d+\])")

    @classmethod
    def split(cls, key_path: str) -> t.Tuple[str, str]:
        key = cls.KEY_PATH_REGEX.split(key_path)[0]
        path = key_path[len(key) :]  # noqa
        key = key.replace(r"\\", "\\").replace(r"\.", r".")
        return key, path


class Sample(t.Mapping[str, Item]):
    """A Sample is a mapping from keys to Items. Any modification creates a new instance
    which is a shallow copy of the original Sample, ie, Item instances are shared, but
    the internal data mapping is not.
    """

    _data: t.Mapping[str, Item]

    def __init__(self, data: t.Optional[t.Mapping[str, Item]]):
        super().__init__()
        self._data = data if data is not None else {}

    def to_dict(self) -> t.Dict[str, t.Any]:
        return {k: v() for k, v in self.items()}

    def shallow_copy(self) -> "Sample":
        return Sample(copy.copy(self._data))

    def deep_copy(self) -> "Sample":
        return Sample(copy.deepcopy(self._data))

    def set_item(self, key: str, value: Item) -> "Sample":
        new_data = dict(self._data)
        new_data[key] = value
        return Sample(new_data)

    def set_value_as(
        self,
        target_key: str,
        reference_key: str,
        value: t.Any,
        shared_item: t.Optional[bool] = None,
    ) -> "Sample":
        ref_item = self._data[reference_key]
        new_data = dict(self._data)
        new_data[target_key] = ref_item.make_new(
            value, shared=ref_item.is_shared if shared_item is None else shared_item
        )
        return Sample(new_data)

    def set_value(self, key: str, value: t.Any) -> "Sample":
        return self.set_value_as(key, key, value)

    def deep_set(self, key_path: str, value: t.Any) -> "Sample":
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

    def change_key(self, old_key: str, new_key: str, delete_old_key: bool) -> "Sample":
        if new_key not in self._data and old_key in self._data:
            new_data = dict(self._data)
            new_data[new_key] = new_data[old_key]
            if delete_old_key:
                del new_data[old_key]
            return Sample(new_data)
        return self

    def duplicate_key(self, reference_key: str, new_key: str) -> "Sample":
        return self.change_key(reference_key, new_key, False)

    def rename_key(self, old_key: str, new_key: str) -> "Sample":
        return self.change_key(old_key, new_key, True)

    def remove_keys(self, *key_to_remove: str) -> "Sample":
        return Sample({k: v for k, v in self._data.items() if k not in key_to_remove})

    def extract_keys(self, *keys_to_keep: str) -> "Sample":
        return Sample({k: v for k, v in self._data.items() if k in keys_to_keep})

    def merge(self, other: "Sample") -> "Sample":
        return Sample({**self._data, **other._data})

    def update(self, other: "Sample") -> "Sample":
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


class SamplesSequence(t.Sequence[Sample]):
    """A generic sequence of samples. Subclasses should implement `size(self) -> int`
    and `get_sample(self, idx: int) -> Sample`"""

    operations: t.List[str] = []
    sources: t.List[str] = []

    @abstractmethod
    def size(self) -> int:
        pass

    @abstractmethod
    def get_sample(self, idx: int) -> Sample:
        pass

    def __len__(self) -> int:
        return self.size()

    def __getitem__(self, idx: t.Union[int, slice]) -> Sample:
        return (
            self.slice(idx.start, idx.stop, idx.step)  # type: ignore
            if isinstance(idx, slice)
            else self.get_sample(idx)
        )

    def is_normalized(self, max_items=-1) -> bool:
        """Checks if all samples have the same keys.

        :param max_items: limits to the first `max_items`, defaults to -1
        :type max_items: int, optional
        :return: True if all samples have the same keys
        :rtype: bool
        """
        max_items = len(self) if max_items < 0 else min(max_items, len(self))
        if max_items < 2:
            return True
        it = itertools.islice(self, max_items)
        key_ref = set(next(it).keys())
        for s in it:
            if key_ref != set(s.keys()):
                return False
        return True

    def best_zfill(self) -> int:
        """Computes the best zfill for integer indexing.

        :return: zfill values (maximum number of digits based on current size)
        :rtype: int
        """
        return len(str(len(self)))

    def __add__(self, other: "SamplesSequence") -> "SamplesSequence":
        return self.cat(other)  # type: ignore


def as_samples_sequence_functional(fn_name: str, is_static: bool = False):
    """A decorator registering a SamplesSequence subclass as functional attribute.

    :param fn_name: the name of the function that we are going to add.
    :type fn_name: str
    """

    def _wrapper(cls):
        import inspect

        docstr = inspect.getdoc(cls)
        if docstr:
            docstr = docstr.replace("\n", "\n    ")

        sig = inspect.signature(cls.__init__)
        prms_list = [i[1] for i in sig.parameters.items()]

        if is_static:
            prms_list = prms_list[1:]
        else:
            prms_list = [prms_list[0]] + prms_list[2:]

        # NB: including annotations would be great, but you'd need to import here
        # all kind of packages used by the subclass we are wrapping.
        prm_names = [p.name for p in prms_list]
        prm_defaults = [
            "" if p.default is inspect.Parameter.empty else f"={p.default}"
            for p in prms_list
        ]
        prm_def_str = ", ".join(
            f"{pname}{pdef}" for pname, pdef in zip(prm_names, prm_defaults)
        )

        fn_str = (
            "def _{0}({1}):\n".format(fn_name, prm_def_str)
            + ("    '''{0}\n    '''\n".format(docstr) if docstr else "")
            + "    from {0} import {1}\n".format(cls.__module__, cls.__name__)
            + "    return {0}({1})\n".format(
                cls.__name__,
                ", ".join(sig.replace(parameters=prms_list).parameters.keys()),
            )
        )

        local_scope = {}
        exec(fn_str, local_scope)

        fn_helper = local_scope[f"_{fn_name}"]
        if is_static:
            fn_helper = staticmethod(fn_helper)

        setattr(SamplesSequence, fn_name, fn_helper)

        if is_static:
            SamplesSequence.sources.append(fn_name)
        else:
            SamplesSequence.operations.append(fn_name)

        return cls

    return _wrapper
