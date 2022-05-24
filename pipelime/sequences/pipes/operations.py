import typing as t

import pydantic as pyd

import pipelime.items as pli
import pipelime.sequences as pls
import pipelime.stages as plst
from pipelime.sequences.pipes.base import PipedSequenceBase


@pls.piped_sequence
class MappedSequence(PipedSequenceBase, title="map"):
    """Applies a stage on all samples."""

    stage: plst.SampleStage = pyd.Field(..., description="The stage to map.")

    def __init__(self, stage: plst.SampleStage, **data):
        super().__init__(stage=stage, **data)  # type: ignore

    def get_sample(self, idx: int) -> pls.Sample:
        return self.stage(self.source[idx])


@pls.piped_sequence
class ZippedSequences(PipedSequenceBase, title="zip"):
    """Zips two Sequences by merging each Sample."""

    to_zip: pls.SamplesSequence = pyd.Field(..., description="The sequence to merge.")

    def __init__(self, to_zip: pls.SamplesSequence, **data):
        super().__init__(to_zip=to_zip, **data)  # type: ignore

    def size(self) -> int:
        return min(len(self.source), len(self.to_zip))

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[idx].merge(self.to_zip[idx])


@pls.piped_sequence
class ConcatSequences(PipedSequenceBase, title="cat"):
    """Concatenates two SamplesSequences."""

    to_cat: pls.SamplesSequence = pyd.Field(
        ..., description="The samples to concatenate."
    )

    def __init__(self, to_cat: pls.SamplesSequence, **data):
        super().__init__(to_cat=to_cat, **data)  # type: ignore

    def size(self) -> int:
        return len(self.source) + len(self.to_cat)

    def get_sample(self, idx: int) -> pls.Sample:
        if idx < len(self.source):
            return self.source[idx]
        return self.to_cat[idx - len(self.source)]


@pls.piped_sequence
class FilteredSequence(
    PipedSequenceBase, title="filter", underscore_attrs_are_private=True
):
    """A filtered view of a SamplesSequence."""

    filter_fn: t.Callable[[pls.Sample], bool] = pyd.Field(
        ..., description="A callable returning True for any valid sample."
    )

    _valid_idxs: t.Sequence[int]

    def __init__(self, filter_fn: t.Callable[[pls.Sample], bool], **data):
        super().__init__(filter_fn=filter_fn, **data)  # type: ignore
        self._valid_idxs = [
            idx for idx, sample in enumerate(self.source) if self.filter_fn(sample)
        ]

    def size(self) -> int:
        return len(self._valid_idxs)

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[self._valid_idxs[idx]]


@pls.piped_sequence
class SortedSequence(
    PipedSequenceBase, title="sort", underscore_attrs_are_private=True
):
    """A sorted view of an input SamplesSequence."""

    key_fn: t.Callable[[pls.Sample], t.Any] = pyd.Field(
        ...,
        description=(
            "The key function to compare Samples. Use `functools.cmp_to_key` to "
            "convert a compare function, ie, accepting two arguments, to a key "
            "function."
        ),
    )

    _sorted_idxs: t.Sequence[int]

    def __init__(self, key_fn: t.Callable[[pls.Sample], t.Any], **data):
        super().__init__(key_fn=key_fn, **data)  # type: ignore
        self._sorted_idxs = sorted(
            range(len(self.source)), key=lambda k: self.key_fn(self.source[k])
        )

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[self._sorted_idxs[idx]]


@pls.piped_sequence
class SlicedSequence(
    PipedSequenceBase, title="slice", underscore_attrs_are_private=True
):
    """Extracts a slice [start_idx:end_idx:step] from the input SamplesSequence."""

    start: t.Optional[int] = pyd.Field(
        None, description="The first index, defaults to the first element."
    )
    stop: t.Optional[int] = pyd.Field(
        None, description="The final index, defaults to the last element."
    )
    step: t.Optional[int] = pyd.Field(
        None, description="The slice step, defaults to 1."
    )

    _sliced_idxs: t.Sequence[int]

    def __init__(self, **data):
        super().__init__(**data)

        effective_start = (
            0 if self.start is None else max(0, min(len(self.source), self.start))
        )
        effective_stop = (
            len(self.source)
            if self.stop is None
            else max(0, min(len(self.source), self.stop))
        )
        effective_step = 1 if self.step is None else self.step
        self._sliced_idxs = range(effective_start, effective_stop, effective_step)

    def size(self) -> int:
        return len(self._sliced_idxs)

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[self._sliced_idxs[idx]]


@pls.piped_sequence
class IndexSelectionSequence(
    PipedSequenceBase, title="select", underscore_attrs_are_private=True
):
    """Given a list of indexes, extracts the corresponding samples from the input
    SamplesSequence. The index sequence is not automatically sorted.
    """

    indexes: t.Sequence[int] = pyd.Field(
        ...,
        description=(
            "The indexes to extract. Negative values start counting from the end."
        ),
    )

    def __init__(self, indexes: t.Sequence[int], **data):
        super().__init__(indexes=indexes, **data)  # type: ignore

        for idx in self.indexes:
            if idx < 0:
                idx += len(self.source)
            if idx < 0 or idx >= len(self.source):
                raise ValueError(
                    "Index {} is out of range [0, {})".format(idx, len(self.source))
                )

    def size(self) -> int:
        return len(self.indexes)

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[self.indexes[idx]]


@pls.piped_sequence
class ShuffledSequence(
    PipedSequenceBase, title="shuffle", underscore_attrs_are_private=True
):
    """Shuffles samples in the input SamplesSequence."""

    seed: t.Optional[int] = pyd.Field(None, description="The optional random seed.")

    _shuffled_idxs: t.Sequence[int]

    def __init__(self, **data):
        import random

        super().__init__(**data)
        if self.seed is not None:
            random.seed(self.seed)
        self._shuffled_idxs = list(range(len(self.source)))
        random.shuffle(self._shuffled_idxs)

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[self._shuffled_idxs[idx]]


@pls.piped_sequence
class EnumeratedSequence(
    PipedSequenceBase, title="enumerate", underscore_attrs_are_private=True
):
    """Add a new index item to each Sample in the input SamplesSequence."""

    idx_key: str = pyd.Field(
        "~idx", description="The new key containing the index item."
    )
    item_cls_path: str = pyd.Field(
        "pipelime.items.NpyNumpyItem", description="The item class holding the index."
    )

    _item_cls: t.Type[pli.Item]

    def __init__(self, **data):
        from pipelime.choixe.utils.imports import import_symbol

        super().__init__(**data)
        self._item_cls = import_symbol(self.item_cls_path)

    def get_sample(self, idx: int) -> pls.Sample:
        sample = self.source[idx]
        return sample.set_item(
            key=self.idx_key, value=self._item_cls(idx)  # type: ignore
        )


@pls.piped_sequence
class RepeatedSequence(PipedSequenceBase, title="repeat"):
    """Repeat this sequence so each sample is seen multiple times."""

    count_: pyd.NonNegativeInt = pyd.Field(
        ..., alias="count", description="The number of repetition."
    )

    def __init__(self, count: pyd.NonNegativeInt, **data):
        super().__init__(count=count, **data)  # type: ignore

    def size(self) -> int:
        return len(self.source) * self.count_

    def get_sample(self, idx: int) -> pls.Sample:
        if idx < 0 or idx >= len(self):
            raise IndexError(f"Sample index `{idx}` is out of range.")
        return self.source[idx % len(self.source)]
