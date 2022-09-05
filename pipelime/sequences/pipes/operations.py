import typing as t
from pathlib import Path

import pydantic as pyd

import pipelime.items as pli
import pipelime.sequences as pls
from pipelime.stages.base import SampleStage, StageInput
from pipelime.stages import StageKeyFormat
from pipelime.sequences.pipes import PipedSequenceBase


@pls.piped_sequence
class MappedSequence(PipedSequenceBase, title="map"):
    """Applies a stage on all samples."""

    stage: StageInput = pyd.Field(..., description=StageInput.__doc__)  # type: ignore

    def __init__(
        self,
        stage: t.Union[SampleStage, str, t.Mapping[str, t.Mapping[str, t.Any]]],
        **data,
    ):
        super().__init__(stage=stage, **data)  # type: ignore

    def get_sample(self, idx: int) -> pls.Sample:
        return self.stage(self.source[idx])


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

    _key_formatting_stage: StageKeyFormat = pyd.PrivateAttr()

    @pyd.validator("key_format")
    def validate_key_format(cls, v):
        if "*" in v:
            return v
        return "*" + v

    def __init__(self, to_zip: pls.SamplesSequence, **data):
        super().__init__(to_zip=to_zip, **data)  # type: ignore
        self._key_formatting_stage = StageKeyFormat(key_format=self.key_format)

    def size(self) -> int:
        return min(len(self.source), len(self.to_zip))

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[idx].merge(self._key_formatting_stage(self.to_zip[idx]))


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

    _valid_idxs: t.Optional[t.Sequence[int]] = None

    def __init__(self, filter_fn: t.Callable[[pls.Sample], bool], **data):
        super().__init__(filter_fn=filter_fn, **data)  # type: ignore

    def _get_valid_idxs(self) -> t.Sequence[int]:
        if self._valid_idxs is None:
            self._valid_idxs = [
                idx for idx, sample in enumerate(self.source) if self.filter_fn(sample)
            ]
        return self._valid_idxs

    def size(self) -> int:
        return len(self._get_valid_idxs())

    def get_sample(self, idx: int) -> pls.Sample:
        return self.source[self._get_valid_idxs()[idx]]

    def __iter__(self) -> t.Iterator[pls.Sample]:
        _src_idx = 0
        while _src_idx < len(self.source):
            x = self.source[_src_idx]
            if self.filter_fn(x):
                yield x
            _src_idx += 1


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
            0
            if self.start is None
            else max(0, min(len(self.source), self._normalized(self.start)))
        )
        effective_stop = (
            len(self.source)
            if self.stop is None
            else max(0, min(len(self.source), self._normalized(self.stop)))
        )
        effective_step = 1 if self.step is None else self.step
        self._sliced_idxs = range(effective_start, effective_stop, effective_step)

    def _normalized(self, idx: int) -> int:
        return idx if idx >= 0 else len(self.source) + idx

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
        "TxtNumpyItem", description="The item class holding the index."
    )

    _item_cls: t.Type[pli.Item]

    @pyd.validator("item_cls_path", always=True)
    def _validate_item_cls_path(cls, value):
        if "." not in value:
            return "pipelime.items." + value
        return value

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


@pls.piped_sequence
class CachedSequence(PipedSequenceBase, title="cache"):
    """Cache the input Samples the first time they are accessed."""

    cache_folder: t.Optional[Path] = pyd.Field(
        None,
        description=(
            "The cache folder path. Leave empty to use a temporary folder "
            "which will be deleted when exiting the process."
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
            from tempfile import TemporaryDirectory

            self._temp_folder = TemporaryDirectory()
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
        from filelock import FileLock, Timeout
        import pickle

        lock = FileLock(str(filename.with_suffix(filename.suffix + ".~lock")))
        try:
            with lock.acquire(timeout=1):
                # check again to avoid races
                if not filename.exists():
                    with open(filename, "wb") as fd:
                        pickle.dump(x, fd, protocol=-1)
        except Timeout:  # pragma: no cover
            pass
