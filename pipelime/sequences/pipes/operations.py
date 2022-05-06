from pipelime.sequences.pipes.base import PipedSequenceBase
import pipelime.sequences as pls
import pipelime.stages as plst
import pipelime.items as pli

import typing as t
import pydantic as pyd


@pls.piped_sequence("map")
class MappedSequence(PipedSequenceBase):
    """Applies a stage on all samples. Usage::

    s1 = SamplesSequence(...)
    fn1, fn2 = lambda x: x, lambda x: x
    sseq = s1.map(fn1).map(fn2)
    """

    stage: plst.SampleStage = pyd.Field(..., description="The stage to map.")

    def __init__(self, stage: plst.SampleStage, **data):
        super().__init__(stage=stage, **data)  # type: ignore

    def get_sample(self, idx: int) -> pls.Sample:
        return self.stage(self.source[idx])


@pls.piped_sequence("zip")
class ZippedSequences(PipedSequenceBase):
    """Zips two Sequences by merging each Sample. Usage::

    s1, s2, s3 = SamplesSequence(...), SamplesSequence(...), SamplesSequence(...)
    sseq = s1.zip(s2).zip(s3)
    """

    to_zip: pls.SamplesSequence = pyd.Field(..., description="The sequence to merge.")

    def __init__(self, to_zip: pls.SamplesSequence, **data):
        super().__init__(to_zip=to_zip, **data)  # type: ignore

    def size(self) -> int:
        return min(len(self.source), len(self.to_zip))

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[idx].merge(self.to_zip[idx])


@pls.piped_sequence("cat")
class ConcatSequences(PipedSequenceBase):
    """Concatenates two SamplesSequences. Usage::

    s1, s2, s3 = SamplesSequence(...), SamplesSequence(...), SamplesSequence(...)
    sseq = s1.cat(s2).cat(s3)
    """

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


@pls.piped_sequence("filter")
class FilteredSequence(PipedSequenceBase):
    """A filtered view of a SamplesSequence. Usage::

    s1 = SamplesSequence(...)
    filter_fn = lambda x: True if x['label'] == 1 else False
    sseq = s1.filter(filter_fn)
    """

    filter_fn: t.Callable[[pls.Sample], bool] = pyd.Field(
        ..., description="A callable returning True for any valid sample."
    )

    _valid_idxs: t.Sequence[int]

    class Config:
        underscore_attrs_are_private = True

    def __init__(self, filter_fn: t.Callable[[pls.Sample], bool], **data):
        super().__init__(filter_fn=filter_fn, **data)  # type: ignore
        self._valid_idxs = [
            idx for idx, sample in enumerate(self.source) if self.filter_fn(sample)
        ]

    def size(self) -> int:
        return len(self._valid_idxs)

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[self._valid_idxs[idx]]


@pls.piped_sequence("sort")
class SortedSequence(PipedSequenceBase):
    """A sorted view of an input SamplesSequence. Usage::

    s1 = SamplesSequence(...)
    key_fn = lambda x: x['weight']
    sseq = s1.sort(key_fn)
    """

    key_fn: t.Callable[[pls.Sample], t.Any] = pyd.Field(
        ...,
        description=(
            "The key function to compare Samples. Use `functools.cmp_to_key` to "
            "convert a compare function, ie, accepting two arguments, to a key "
            "function."
        ),
    )

    _sorted_idxs: t.Sequence[int]

    class Config:
        underscore_attrs_are_private = True

    def __init__(self, key_fn: t.Callable[[pls.Sample], t.Any], **data):
        super().__init__(key_fn=key_fn, **data)  # type: ignore
        self._sorted_idxs = sorted(
            range(len(self.source)), key=lambda k: self.key_fn(self.source[k])
        )

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[self._sorted_idxs[idx]]


@pls.piped_sequence("slice")
class SlicedSequence(PipedSequenceBase):
    """Extracts a slice [start_idx:end_idx:step] from the input SamplesSequence. Usage::

    s1 = SamplesSequence(...)
    sseq = s1.slice(12, 200, 3)
    # also
    sseq = s1[12:200:3]
    """

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

    class Config:
        underscore_attrs_are_private = True

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


@pls.piped_sequence("shuffle")
class ShuffledSequence(PipedSequenceBase):
    """Shuffles samples in the input SamplesSequence. Usage::

    s1 = SamplesSequence(...)
    sseq = s1.shuffle()
    """

    rnd_seed: t.Optional[int] = pyd.Field(None, description="The optional random seed.")

    _shuffled_idxs: t.Sequence[int]

    class Config:
        underscore_attrs_are_private = True

    def __init__(self, **data):
        import random

        super().__init__(**data)
        if self.rnd_seed is not None:
            random.seed(self.rnd_seed)
        self._shuffled_idxs = list(range(len(self.source)))
        random.shuffle(self._shuffled_idxs)

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[self._shuffled_idxs[idx]]


@pls.piped_sequence("enumerate")
class EnumeratedSequence(PipedSequenceBase):
    """Add a new index item to each Sample in the input SamplesSequence. Usage::

    s1 = SamplesSequence(...)
    sseq = s1.enumerate()
    """

    idx_key: str = pyd.Field(
        "~idx", description="The new key containing the index item."
    )
    item_cls_path: str = pyd.Field(
        "pipelime.items.NpyNumpyItem", description="The item class holding the index."
    )

    _item_cls: t.Type[pli.Item]

    class Config:
        underscore_attrs_are_private = True

    def __init__(self, **data):
        from pipelime.choixe.utils.imports import import_symbol

        super().__init__(**data)
        self._item_cls = import_symbol(self.item_cls_path)

    def get_sample(self, idx: int) -> pls.Sample:
        sample = self.source[idx]
        return sample.set_item(
            key=self.idx_key, value=self._item_cls(idx)  # type: ignore
        )


@pls.piped_sequence("repeat")
class RepeatedSequence(PipedSequenceBase):
    """Repeat this sequence so each sample is seen multiple times. Usage::

    s1 = SamplesSequence(...)
    sseq = s1.repeat(4)
    """

    count_: pyd.NonNegativeInt = pyd.Field(
        ..., alias="count", description="The number of repetition."
    )

    def __init__(self, count: int, **data):
        super().__init__(count=count, **data)  # type: ignore

    def size(self) -> int:
        return len(self.source) * self.count_

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[idx % len(self.source)]
