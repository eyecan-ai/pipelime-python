import typing as t
from pathlib import Path

import pydantic.v1 as pyd

import pipelime.sequences as pls
from pipelime.sequences.pipes import PipedSequenceBase
from pipelime.utils.pydantic_types import ItemType


@pls.piped_sequence
class ZippedSequences(PipedSequenceBase, title="zip"):
    """Zips two Sequences by merging each Sample."""

    to_zip: pls.SamplesSequence = pyd.Field(..., description="The sequence to merge.")
    key_format: str = pyd.Field(
        "*",
        description=(
            "The zipped samples' key format. Any `*` will be replaced with the "
            "source key, eg, `my_*_key` on [`image`, `mask`] generates "
            "`my_image_key` and `my_mask_key`. If no `*` is found, the string is "
            "suffixed to source key, ie, `MyKey` on `image` gives "
            "`imageMyKey`. If empty, the source key will be used as-is."
        ),
    )

    _key_formatting_stage = pyd.PrivateAttr()

    @pyd.validator("key_format")
    def validate_key_format(cls, v):
        if "*" in v:
            return v
        return "*" + v

    def __init__(self, to_zip: pls.SamplesSequence, **data):
        from pipelime.stages import StageKeyFormat

        super().__init__(to_zip=to_zip, **data)  # type: ignore
        self._key_formatting_stage = StageKeyFormat(key_format=self.key_format)  # type: ignore

    def size(self) -> int:
        return min(len(self.source), len(self.to_zip))

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[idx].merge(self._key_formatting_stage(self.to_zip[idx]))


@pls.piped_sequence
class ConcatSequences(PipedSequenceBase, title="cat"):
    """Concatenates two or more SamplesSequences."""

    to_cat: t.Sequence[pls.SamplesSequence] = pyd.Field(
        ..., description="The samples sequence(s) to concatenate."
    )
    interleave: bool = pyd.Field(False, description="If TRUE, samples are interleaved.")

    _index_refs: t.Sequence[t.Tuple[int, t.Sequence[int]]] = pyd.PrivateAttr()

    def __init__(self, *seqs: pls.SamplesSequence, **data):
        if seqs:
            to_cat = data.setdefault("to_cat", list())
            data["to_cat"] = list(seqs) + (
                [to_cat] if isinstance(to_cat, pls.SamplesSequence) else to_cat
            )
        super().__init__(**data)

        if self.interleave:
            lengths = [
                (idx, len(seq)) for idx, seq in enumerate((self.source, *self.to_cat))
            ]
            lengths.sort(key=lambda x: x[1])

            offs, total = 0, 0
            self._index_refs = []
            for idx, l in enumerate(lengths):
                active_seqs = sorted([ll[0] for ll in lengths[idx:]])
                final_idx = total + (l[1] - offs) * len(active_seqs)
                self._index_refs.append((final_idx, active_seqs))
                offs = l[1]
                total = final_idx

    def size(self) -> int:
        if self.interleave:
            return self._index_refs[-1][0]
        return len(self.source) + sum(len(s) for s in self.to_cat)

    def get_sample(self, idx: int) -> pls.Sample:
        if self.interleave:
            seqs = (self.source, *self.to_cat)
            global_offs, seq_offs = 0, 0
            for final_idx, active_seqs in self._index_refs:
                if idx < final_idx:
                    # get the index relative to the current block
                    idx -= global_offs
                    # get the index relative to the current sequence
                    sample_idx = seq_offs + idx // len(active_seqs)
                    # get the sequence index among the active ones
                    seq_idx = idx % len(active_seqs)
                    return seqs[active_seqs[seq_idx]][sample_idx]
                global_offs = final_idx
                seq_offs = min(len(seqs[i]) for i in active_seqs)
        else:
            if idx < len(self.source):
                return self.source[idx]

            offidx = idx - len(self.source)
            for x in self.to_cat:
                if offidx < len(x):
                    return x[offidx]
                offidx -= len(x)

        raise IndexError(f"Sample index `{idx}` is out of range [0, {len(self)-1}].")


@pls.piped_sequence
class FilteredSequence(PipedSequenceBase, title="filter"):
    """A filtered view of a SamplesSequence."""

    filter_fn: t.Callable[[pls.Sample], bool] = pyd.Field(
        ..., description="A callable returning True for any valid sample."
    )
    lazy: bool = pyd.Field(
        True, description="Defer the sample sorting to first-time access."
    )
    insert_empty_samples: bool = pyd.Field(
        False,
        description=(
            "If True, empty samples are inserted in place of invalid samples. "
            "This makes the filtering real-time and multi-processing friendly."
        ),
    )

    _valid_idxs: t.Optional[t.Sequence[int]] = pyd.PrivateAttr(None)

    def __init__(self, filter_fn: t.Callable[[pls.Sample], bool], **data):
        super().__init__(filter_fn=filter_fn, **data)  # type: ignore
        if not self.insert_empty_samples and not self.lazy:
            self._get_valid_indexes()

    def _get_valid_indexes(self) -> t.Sequence[int]:
        if self._valid_idxs is None:
            self._valid_idxs = [
                idx for idx, sample in enumerate(self.source) if self.filter_fn(sample)
            ]
        return self._valid_idxs

    def size(self) -> int:
        if self.insert_empty_samples:
            return len(self.source)
        return len(self._get_valid_indexes())

    def get_sample(self, idx: int) -> pls.Sample:
        if self.insert_empty_samples:
            x = self.source[idx]
            return x if self.filter_fn(x) else pls.Sample()
        return self.source[self._get_valid_indexes()[idx]]


@pls.piped_sequence
class SortedSequence(PipedSequenceBase, title="sort"):
    """A sorted view of an input SamplesSequence."""

    key_fn: t.Callable[[pls.Sample], t.Any] = pyd.Field(
        ...,
        description=(
            "The key function to compare Samples. Use `functools.cmp_to_key` to "
            "convert a compare function, ie, accepting two arguments, to a key "
            "function."
        ),
    )
    lazy: bool = pyd.Field(
        True, description="Defer the sample sorting to first-time access."
    )

    _sorted_idxs: t.Optional[t.Sequence[int]] = pyd.PrivateAttr(None)

    def __init__(self, key_fn: t.Callable[[pls.Sample], t.Any], **data):
        super().__init__(key_fn=key_fn, **data)  # type: ignore
        if not self.lazy:
            self._get_sorted_indexes()

    def _get_sorted_indexes(self):
        if self._sorted_idxs is None:
            self._sorted_idxs = sorted(
                range(len(self.source)), key=lambda k: self.key_fn(self.source[k])
            )
        return self._sorted_idxs

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[self._get_sorted_indexes()[idx]]  # type: ignore


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

        effective_step = 1 if self.step is None else self.step
        effective_start = (
            (len(self.source) - 1 if effective_step < 0 else 0)
            if self.start is None
            else max(0, min(len(self.source) - 1, self._normalized(self.start)))
        )
        effective_stop = (
            (-1 if effective_step < 0 else len(self.source))
            if self.stop is None
            else max(-1, min(len(self.source), self._normalized(self.stop)))
        )

        self._sliced_idxs = range(effective_start, effective_stop, effective_step)

    def _normalized(self, idx: int) -> int:
        return idx if idx >= 0 else len(self.source) + idx

    def source_index(self, idx: int) -> int:
        return self._sliced_idxs[idx]

    def size(self) -> int:
        return len(self._sliced_idxs)

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[self.source_index(idx)]


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
    negate: bool = pyd.Field(
        False,
        description=(
            "FALSE to extract samples at `indexes`, "
            "TRUE to get all samples but the ones at `indexes`."
        ),
    )

    _effective_idxs: t.Sequence[int]

    def __init__(self, indexes: t.Sequence[int], **data):
        super().__init__(indexes=indexes, **data)  # type: ignore

        for idx in self.indexes:
            if idx < 0:
                idx += len(self.source)
            if idx < 0 or idx >= len(self.source):
                raise ValueError(
                    "Index {} is out of range [0, {})".format(idx, len(self.source))
                )
        self._effective_idxs = (
            [idx for idx in range(len(self.source)) if idx not in self.indexes]
            if self.negate
            else self.indexes
        )

    def size(self) -> int:
        return len(self._effective_idxs)

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[self._effective_idxs[idx]]


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
        rnd = random.Random(self.seed)
        self._shuffled_idxs = list(range(len(self.source)))
        rnd.shuffle(self._shuffled_idxs)

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[self._shuffled_idxs[idx]]


@pls.piped_sequence
class EnumeratedSequence(
    PipedSequenceBase, title="enumerate", underscore_attrs_are_private=True
):
    """Adds a new index item to each Sample in the input SamplesSequence."""

    idx_key: str = pyd.Field(
        "~idx", description="The new key containing the index item."
    )
    item_cls: ItemType = pyd.Field(
        default_factory=lambda: ItemType.create("TxtNumpyItem"),
        description="The item class holding the index.",
    )

    def get_sample(self, idx: int) -> pls.Sample:
        sample = self.source[idx]
        return sample.set_item(
            key=self.idx_key, value=self.item_cls(idx)  # type: ignore
        )


@pls.piped_sequence
class RepeatedSequence(PipedSequenceBase, title="repeat"):
    """Repeats this sequence so each sample is seen multiple times."""

    count_: pyd.NonNegativeFloat = pyd.Field(
        ..., alias="count", description="The number of repetition."
    )
    interleave: bool = pyd.Field(
        False, description="If TRUE, interleaves samples from this sequence."
    )

    def __init__(self, count: pyd.NonNegativeFloat, **data):
        super().__init__(count=count, **data)  # type: ignore

    def _fractional_size(self) -> int:
        return len(self) - len(self.source) * int(self.count_)

    def size(self) -> int:
        return round(len(self.source) * self.count_)

    def get_sample(self, idx: int) -> pls.Sample:
        import math

        if idx < 0 or idx >= len(self):
            raise IndexError(
                f"Sample index `{idx}` is out of range [0, {len(self)-1}]."
            )

        if self.interleave:
            ceil_count = math.ceil(self.count_)
            more_repeated = self._fractional_size() * ceil_count
            idx0 = min(idx, more_repeated) // ceil_count
            idx1 = max(idx - more_repeated, 0) // int(self.count_)
            final_idx = idx0 + idx1
        else:
            final_idx = idx % len(self.source)

        return self.source[final_idx]


@pls.piped_sequence
class CachedSequence(PipedSequenceBase, title="cache"):
    """Caches the input Samples the first time they are accessed."""

    cache_folder: t.Optional[Path] = pyd.Field(
        None,
        description=(
            "The cache folder path. Leave empty to use a temporary folder "
            "which will be deleted when the object will be garbage collected."
        ),
    )
    reuse_cache: bool = pyd.Field(
        False,
        description=(
            "If `cache_folder` exists, use it anyway if true, "
            "otherwise raise a FileExistsError."
        ),
    )

    _temp_folder = pyd.PrivateAttr(None)

    def __init__(self, cache_folder: t.Optional[Path] = None, **data):
        super().__init__(cache_folder=cache_folder, **data)  # type: ignore
        if self.cache_folder is None:
            from pipelime.choixe.utils.io import PipelimeTemporaryDirectory as PlTmpDir

            self._temp_folder = PlTmpDir()
            self.cache_folder = Path(self._temp_folder.name)
        else:
            if not self.reuse_cache and self.cache_folder.exists():
                raise FileExistsError(
                    f"The cache folder `{self.cache_folder}` already exists."
                )
            self.cache_folder.mkdir(parents=True, exist_ok=True)

    def get_sample(self, idx: int) -> pls.Sample:
        import pickle

        filename: Path = self.cache_folder / f"{idx}.pkl"  # type: ignore

        if filename.exists():
            with open(filename, "rb") as fd:
                return pickle.load(fd)

        x = self.source[idx]
        self._cache_sample(x, filename)
        return x

    def _cache_sample(self, x: pls.Sample, filename: Path):
        import pickle

        from filelock import FileLock, Timeout

        lock = FileLock(str(filename.with_suffix(filename.suffix + ".~lock")))
        try:
            with lock.acquire(timeout=1):
                # check again to avoid races
                if not filename.exists():  # pragma: no branch
                    with open(filename, "wb") as fd:
                        pickle.dump(x, fd, protocol=-1)
        except Timeout:  # pragma: no cover
            pass


@pls.piped_sequence
class EnableItemDataCache(PipedSequenceBase, title="data_cache"):
    """Enables item data caching on previous pipeline steps."""

    items: t.Union[ItemType, t.Sequence[ItemType]] = pyd.Field(
        default_factory=list,
        description="One or more item classes where data cache should be enabled.",
    )

    _item_cls: list = pyd.PrivateAttr()

    def __init__(self, *items, **data):
        super().__init__(items=items, **data)  # type: ignore
        self._item_cls = [
            it.value
            for it in (
                self.items if isinstance(self.items, t.Sequence) else [self.items]
            )
        ]

    def get_sample(self, idx: int) -> pls.Sample:
        from pipelime.items import data_cache

        with data_cache(*self._item_cls):  # type: ignore
            return super().get_sample(idx)


@pls.piped_sequence
class DisableItemDataCache(PipedSequenceBase, title="no_data_cache"):
    """Disables item data caching on previous pipeline steps."""

    items: t.Union[ItemType, t.Sequence[ItemType]] = pyd.Field(
        default_factory=list,
        description="One or more item classes where data cache should be disabled.",
    )

    _item_cls: list = pyd.PrivateAttr()

    def __init__(self, *items, **data):
        super().__init__(items=items, **data)  # type: ignore
        self._item_cls = [
            it.value
            for it in (
                self.items if isinstance(self.items, t.Sequence) else [self.items]
            )
        ]

    def get_sample(self, idx: int) -> pls.Sample:
        from pipelime.items import no_data_cache

        with no_data_cache(*self._item_cls):  # type: ignore
            return super().get_sample(idx)


class _BatchedIndex:
    def __init__(self, batch_size: int, source_length: int):
        self._batch_size = batch_size
        self._length = batch_size * source_length

    def __getitem__(self, index: int) -> t.Tuple[int, int]:
        return index // self._batch_size, index % self._batch_size

    def __len__(self):
        return self._length


@pls.piped_sequence
class UnbatchedSequences(PipedSequenceBase, title="unbatched"):
    """Un-batch items by un-stacking along the first dimension.
    Shared items are not touched.
    """

    batch_size: t.Union[pyd.PositiveInt, t.Literal["fixed", "variable"]] = pyd.Field(
        "fixed",
        description=(
            "The original batch size. If known, just use that. Otherwise, use `fixed` "
            "to get it from the first sample or `variable` to read all the samples."
        ),
    )
    key_list: t.Optional[t.Sequence[str]] = pyd.Field(
        None,
        description="Item keys to extract from the input sample before unbatching.",
    )

    _index_mapper: t.Union[_BatchedIndex, t.Sequence[t.Tuple[int, int]]] = (
        pyd.PrivateAttr()
    )

    def __init__(self, **data):
        import pipelime.items as pli

        super().__init__(**data)

        if len(self.source) == 0:
            self._index_mapper = []
            return

        if isinstance(self.batch_size, int):
            self._index_mapper = _BatchedIndex(self.batch_size, len(self.source))
        else:
            with pli.no_data_cache():
                if self.batch_size == "fixed":
                    bsize = self.infer_batch_size(self.source[0], self.key_list)
                    if bsize == 0:
                        raise ValueError(
                            "Could not infer batch size from the first sample "
                            '(found 0). Try with `batch_size="variable"`.'
                        )
                    self._index_mapper = _BatchedIndex(bsize, len(self.source))
                else:
                    self._index_mapper = [
                        (i, j)
                        for i, x in enumerate(self.source)
                        for j in range(max(self.infer_batch_size(x, self.key_list), 1))
                    ]

    @classmethod
    def infer_batch_size(
        cls, sample: pls.Sample, key_list: t.Optional[t.Sequence[str]]
    ) -> int:
        for k in key_list or sample.keys():
            if k in sample and not sample[k].is_shared:
                return len(sample[k]())  # type: ignore
        return 0

    def size(self) -> int:
        return len(self._index_mapper)

    def get_sample(self, idx: int) -> pls.Sample:
        sample_index, batch_index = self._index_mapper[idx]
        x = self.source[sample_index]
        return pls.Sample(
            {
                key: (
                    x[key]
                    if x[key].is_shared
                    else x[key].make_new(x[key]()[batch_index], shared=False)  # type: ignore
                )
                for key in self.key_list or x.keys()
                if key in x
            }
        )


@pls.piped_sequence
class BatchedSequences(PipedSequenceBase, title="batched"):
    """Batch items by stacking them along a new dimension.
    Shared items are not touched.
    """

    batch_size: pyd.PositiveInt = pyd.Field(..., description="The batch size.")
    drop_last: bool = pyd.Field(
        False, description="Drop the last incomplete batch, if any."
    )
    key_list: t.Optional[t.Sequence[str]] = pyd.Field(
        None, description="Item keys to extract from the input sample before batching."
    )

    def __init__(self, **data):
        super().__init__(**data)

        if self.drop_last and len(self.source) % self.batch_size != 0:
            new_size = self.batch_size * (len(self.source) // self.batch_size)
            self.source = self.source[:new_size]

    def size(self) -> int:
        # possibly including last incomplete batch
        return len(self.source) // self.batch_size + min(
            1, len(self.source) % self.batch_size
        )

    def get_sample(self, idx: int) -> pls.Sample:
        import numpy as np

        import pipelime.items as pli

        start_idx = idx * self.batch_size
        end_idx = min(start_idx + self.batch_size, len(self.source))

        batched_data = {}
        shared_data = {}
        for x in self.source[start_idx:end_idx]:
            for k in self.key_list or x.keys():
                if k in x:
                    if x[k].is_shared:
                        shared_data[k] = x[k]
                    else:
                        batched_data.setdefault(k, list()).append(x[k])

        for k, v in batched_data.items():
            if isinstance(v[0], pli.NumpyItem):
                # both images and raw arrays
                item_cls = pli.TiffImageItem if v[0]().ndim > 1 else pli.TxtNumpyItem
                batched_data[k] = item_cls(np.stack([x() for x in v], axis=0))
            else:
                batched_data[k] = v[0].__class__.make_new([x() for x in v])

        return pls.Sample({**batched_data, **shared_data})
