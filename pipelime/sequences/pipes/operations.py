from pipelime.sequences.pipes.base import ProxySequenceBase
import pipelime.sequences.base as pls

import typing as t


@pls.as_samples_sequence_functional("map")
class MappedSequence(ProxySequenceBase):
    """Applies a callable on all samples. Usage::

        s1 = SamplesSequence(...)
        fn1, fn2 = lambda x: x, lambda x: x
        sseq = MappedSequence(MappedSequence(s1, fn1), fn2)
        # or, equivalently,
        sseq = s1.map(fn1).map(fn2)

    :param stage: the callable we are going to map.
    :type stage: t.Callable[[pls.Sample], pls.Sample]
    """

    def __init__(
        self,
        source: pls.SamplesSequence,
        stage: t.Callable[[pls.Sample], pls.Sample],
    ):
        super().__init__(source)
        self._stage = stage

    def get_sample(self, idx: int) -> pls.Sample:
        return self._stage(self.source[idx])


@pls.as_samples_sequence_functional("merge")
class MergedSequences(ProxySequenceBase):
    """Merges samples from two SamplesSequences. Usage::

    s1, s2, s3 = SamplesSequence(...), SamplesSequence(...), SamplesSequence(...)
    sseq = MergedSequences(MergedSequences(s1, s2), s3)
    # or, equivalently,
    sseq = s1.merge(s2).merge(s3)
    """

    def __init__(self, source: pls.SamplesSequence, to_merge: pls.SamplesSequence):
        super().__init__(source)
        self._to_merge = to_merge

    def size(self) -> int:
        return min(len(self.source), len(self._to_merge))

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[idx].merge(self._to_merge[idx])


@pls.as_samples_sequence_functional("cat")
class ConcatSequences(ProxySequenceBase):
    """Concatenates two SamplesSequences. Usage::

    s1, s2, s3 = SamplesSequence(...), SamplesSequence(...), SamplesSequence(...)
    sseq = ConcatSamplesSequence(ConcatSamplesSequence(s1, s2), s3)
    # or, equivalently,
    sseq = s1.cat(s2).cat(s3)
    """

    def __init__(self, source: pls.SamplesSequence, to_cat: pls.SamplesSequence):
        super().__init__(source)
        self._to_cat = to_cat

    def size(self) -> int:
        return len(self.source) + len(self._to_cat)

    def get_sample(self, idx: int) -> pls.Sample:
        if idx < len(self.source):
            return self.source[idx]
        return self._to_cat[idx - len(self.source)]


@pls.as_samples_sequence_functional("filter")
class FilteredSequence(ProxySequenceBase):
    """A filtered view of a SamplesSequence. Usage::

        s1 = SamplesSequence(...)
        filter_fn = lambda x: True if x['label'] == 1 else False
        sseq = FilteredSequence(s1, filter_fn)
        # or, equivalently,
        sseq = s1.filter(filter_fn)

    :param filter_fn:  a callable returning True for any valid sample.
    :type filter_fn: t.Callable[[pls.Sample], bool]
    """

    def __init__(
        self, source: pls.SamplesSequence, filter_fn: t.Callable[[pls.Sample], bool]
    ):
        super().__init__(source)
        self._valid_idx = [
            idx for idx, sample in enumerate(source) if filter_fn(sample)
        ]

    def size(self) -> int:
        return len(self._valid_idx)

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[self._valid_idx[idx]]


@pls.as_samples_sequence_functional("sort")
class SortedSequence(ProxySequenceBase):
    """A sorted view of an input SamplesSequence. Usage::

        s1 = SamplesSequence(...)
        key_fn = lambda x: x['weight']
        sseq = SortedSequence(s1, key_fn)
        # or, equivalently,
        sseq = s1.sort(key_fn)

    :param key_fn: the key function to compare Samples. Use `functools.cmp_to_key` to
        convert a compare function, ie, accepting two arguments, to a key function.
    :type key_fn: t.Callable[[pls.Sample], t.Any]
    """

    def __init__(
        self, source: pls.SamplesSequence, key_fn: t.Callable[[pls.Sample], t.Any]
    ):
        super().__init__(source)
        self._sorted_idx = sorted(range(len(source)), key=lambda k: key_fn(source[k]))

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[self._sorted_idx[idx]]


@pls.as_samples_sequence_functional("slice")
class SlicedSequence(ProxySequenceBase):
    """Extracts a slice [start_idx:end_idx:step] from the input SamplesSequence. Usage::

        s1 = SamplesSequence(...)
        sseq = SlicedSequence(s1, 12, 200, 3)
        # or, equivalently,
        sseq = s1.slice(12, 200, 3)
        # or, even
        sseq = s1[12:200:3]

    :param start: the first index, defaults to None (ie, first element).
    :type start: t.Optional[int], optional
    :param stop: the final index, defaults to None (ie, last element).
    :type stop: t.Optional[int], optional
    :param step: the slice step, defaults to None (ie, 1).
    :type step: t.Optional[int], optional
    """

    def __init__(
        self,
        source: pls.SamplesSequence,
        start: t.Optional[int] = None,
        stop: t.Optional[int] = None,
        step: t.Optional[int] = None,
    ):
        super().__init__(source)
        start = 0 if start is None else max(0, min(len(source), start))
        stop = len(source) if stop is None else max(0, min(len(source), stop))
        step = 1 if step is None else step
        self._sliced_idx = range(start, stop, step)

    def size(self) -> int:
        return len(self._sliced_idx)

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[self._sliced_idx[idx]]


@pls.as_samples_sequence_functional("shuffle")
class ShuffledSequence(ProxySequenceBase):
    """Shuffles samples in the input SamplesSequence. Usage::

        s1 = SamplesSequence(...)
        sseq = ShuffledSequence(s1)
        # or, equivalently,
        sseq = s1.shuffle()

    :param rnd_seed: the optional random seed.
    :type rnd_seed: t.Optional[int], optional
    """

    def __init__(self, source: pls.SamplesSequence, rnd_seed: t.Optional[int] = None):
        import random

        super().__init__(source)
        if rnd_seed is not None:
            random.seed(rnd_seed)
        self._shuffled_idxs = list(range(len(source)))
        random.shuffle(self._shuffled_idxs)

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[self._shuffled_idxs[idx]]
