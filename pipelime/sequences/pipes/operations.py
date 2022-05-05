from pipelime.sequences.pipes.base import PipedSequenceBase
import pipelime.sequences as pls
import pipelime.stages as plst

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


@pls.piped_sequence("merge")
class MergedSequences(PipedSequenceBase):
    """Merges samples from two SamplesSequences. Usage::

    s1, s2, s3 = SamplesSequence(...), SamplesSequence(...), SamplesSequence(...)
    sseq = s1.merge(s2).merge(s3)
    """

    to_merge: pls.SamplesSequence = pyd.Field(..., description="The samples to merge.")

    def __init__(self, to_merge: pls.SamplesSequence, **data):
        super().__init__(to_merge=to_merge, **data)  # type: ignore

    def size(self) -> int:
        return min(len(self.source), len(self.to_merge))

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[idx].merge(self.to_merge[idx])


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
