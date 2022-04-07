from abc import abstractmethod
import copy
import itertools
import typing as t

from pipelime.items import Item, UnknownItem


class Sample(t.Mapping[str, Item]):
    """A Sample is a mapping from keys to Items. Any modification creates a new instance
    which is a shallow copy of the original Sample, ie, Item instances are shared, but
    the internal data mapping is not.
    """

    def __init__(self, data: t.Optional[t.Mapping[str, Item]]):
        super().__init__()
        self._data: t.Mapping[str, Item] = data if data is not None else {}

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
        new_data[target_key] = (
            ref_item.make_new(
                value, shared=ref_item.is_shared if shared_item is None else shared_item
            )
            if reference_key in self._data
            else UnknownItem(value)
        )
        return Sample(new_data)

    def set_value(self, key: str, value: t.Any) -> "Sample":
        return self.set_value_as(key, key, value)

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
        return Sample(
            {k: v for k, v in self._data.items() if k not in key_to_remove}
        )

    def extract_keys(self, *key_to_keep: str) -> "Sample":
        return Sample(
            {k: v for k, v in self._data.items() if k in key_to_keep}
        )

    def merge(self, other: "Sample") -> "Sample":
        return Sample({**self._data, **other._data})

    def update(self, other: "Sample") -> "Sample":
        return self.merge(other)

    def __getitem__(self, key) -> Item:
        return self._data[key]

    def __iter__(self) -> t.Iterator[str]:
        return iter(self._data.keys())

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, key) -> bool:
        return key in self._data

    def __repr__(self) -> str:
        return f"{self.__class__}{repr(self._data)}"

    def __str__(self) -> str:
        data_str = [f"  [{k}] {str(v)}" for k, v in self._data.items()]
        return "\n".join([f"{self.__class__.__name__}"] + data_str)


class SamplesSequence(t.Sequence[Sample]):
    """A generic sequence of samples. Subclasses should implement `size(self) -> int`
    and `get_sample(self, idx: int) -> Sample`"""

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

    @classmethod
    def register_functional(
        cls, fn_name: str, subcls: t.Type["SamplesSequence"], is_static: bool
    ):
        """Subclasses can register themself as method attribute.
        See also `as_samples_sequence_functional`.

        :param fn_name: the name of the function we are going to create.
        :type fn_name: str
        :param subcls: the subclass type that will be returned.
        :type subcls: t.Type[SamplesSequence]
        """
        if not issubclass(subcls, SamplesSequence):
            raise TypeError(
                "SamplesSequence.register_functional"
                " should be used only with subclasses."
            )

        if is_static:

            def _static_helper(*args, **kwargs):
                return subcls(*args, **kwargs)  # type: ignore

            setattr(cls, fn_name, staticmethod(_static_helper))
        else:

            def _self_helper(self, *args, **kwargs):
                return subcls(self, *args, **kwargs)  # type: ignore

            setattr(cls, fn_name, _self_helper)

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


def as_samples_sequence_functional(fn_name: str, is_static: bool = False):
    """A decorator registering a SamplesSequence subclass as functional attribute.

    :param fn_name: the name of the function that we are going to add.
    :type fn_name: str
    """

    def _wrapper(cls):
        SamplesSequence.register_functional(fn_name, cls, is_static=is_static)
        return cls

    return _wrapper
