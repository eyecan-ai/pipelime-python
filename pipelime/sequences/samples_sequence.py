from __future__ import annotations

import itertools
import typing as t
from abc import abstractmethod

import pydantic.v1 as pyd
from loguru import logger

from pipelime.sequences.sample import Sample

if t.TYPE_CHECKING:
    from pathlib import Path

    import torch

    from pipelime.sequences.direct_access import DirectAccessSequence
    from pipelime.stages import SampleStage, StageInput
    from pipelime.utils.pydantic_types import SampleValidationInterface


def _sseq_stub_dummy(*args, **kwargs):
    raise NotImplementedError(
        "This is a stub function. It is not supposed to be called."
    )


def samples_sequence_stub(func):
    return _sseq_stub_dummy


class SamplesSequenceBase(t.Sequence[Sample]):
    @abstractmethod
    def size(self) -> int:
        pass

    @abstractmethod
    def get_sample(self, idx: int) -> Sample:
        pass

    def __len__(self) -> int:
        return self.size()

    def is_normalized(self, max_items: int = -1) -> bool:
        """Checks if all samples have the same keys.

        Args:
            max_items (int, optional): limits to the first `max_items`  (Default to -1)

        Returns:
            bool: True if all samples have the same keys
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

        Returns:
            int: zfill values (maximum number of digits based on current size)
        """
        return len(str(len(self) - 1))


class SamplesSequence(
    SamplesSequenceBase, pyd.BaseModel, extra="forbid", copy_on_model_validation="none"
):
    """A generic sequence of samples. Subclasses should implement `size(self) -> int`
    and `get_sample(self, idx: int) -> Sample`.

    The list of all available pipes and sources can be retrieved
    through `pipelime list-ops`.

    NB: when defining a pipe, the `source` sample sequence must be bound to a pydantic
    Field with `pipe_source=True`.
    """

    _sources: t.ClassVar[t.Dict[str, t.Type[SamplesSequence]]] = {}
    _pipes: t.ClassVar[t.Dict[str, t.Type[SamplesSequence]]] = {}
    _operator_path: t.ClassVar[str] = ""

    @t.overload
    def __getitem__(self, idx: int) -> Sample: ...

    @t.overload
    def __getitem__(self, idx: slice) -> SamplesSequence: ...

    def __getitem__(self, idx: t.Union[int, slice]) -> t.Union[Sample, SamplesSequence]:
        return (
            self.slice(start=idx.start, stop=idx.stop, step=idx.step)
            if isinstance(idx, slice)
            else self.get_sample(idx if idx >= 0 else len(self) + idx)
        )

    def __iter__(self) -> t.Iterator[Sample]:
        _local_idx = 0
        while _local_idx < len(self):
            yield self[_local_idx]
            _local_idx += 1

    def __add__(self, other: SamplesSequence) -> SamplesSequence:
        return self.cat(other)

    @classmethod
    def name(cls) -> str:
        if cls.__config__.title:
            return cls.__config__.title
        return cls.__name__

    def direct_access(self) -> "DirectAccessSequence":
        """Returns a sequence of key-to-value mappings,
        with no intermediate Sample and Item classes.

        Returns:
            DirectAccessSequence: a sequence of key-to-value mappings
        """
        from pipelime.sequences.direct_access import DirectAccessSequence

        return DirectAccessSequence(self)

    def torch_dataset(
        self,
    ) -> (
        "torch.utils.data.Dataset"
    ):  # pragma: no cover  # type: ignore # noqa: E602,F821
        """Returns a torch.utils.data.Dataset interface of this samples sequence.

        Returns:
            torch.utils.data.Dataset: a torch Dataset
        """
        from pipelime.sequences.torch import TorchDataset

        return TorchDataset(self)

    def batch(
        self, batch_size: int, drop_last: bool = False, fill: Sample = Sample()
    ) -> t.Iterator[t.Tuple[Sample, ...]]:
        """Returns a zip-like object to get batches of samples. If the number of
        samples is not a multiple of batch_size and `drop_last` is False,
        the last batch will be filled with `fill`.

        Args:
            batch_size (int): the number of samples in each batch
            drop_last (bool, optional): whether to drop the last batch if incomplete.
                (Default to False)
            fill (Sample, optional): filling value for the last incomplete batch.
                (Default to Sample()).

        Returns:
            t.Iterator[t.Tuple[Sample, ...]]: an iterator of batches of samples
        """
        src_iters = [iter(self)] * batch_size
        if drop_last:
            return zip(*src_iters)
        return itertools.zip_longest(*src_iters, fillvalue=fill)

    def apply(
        self,
        *,
        num_workers: int = 0,
        prefetch: int = 2,
        track_fn: t.Union[bool, str, t.Callable[[t.Iterable], t.Iterable], None] = True,
    ):
        """Goes through all the samples of the sequence, optionally using multiple
        processes and returns a new sequence holding the processed samples.
        Also, a `track_fn` can be defined, eg, to show the progress.

        Args:
            num_workers (int, optional): The number of processes to spawn. If negative,
                the number of (logical) cpu cores is used  (Default to 0)
            prefetch (int, optional): The number of samples loaded in advanced
                by each worker  (Default to 2)
            track_fn (track_fn: t.Union[bool, str, t.Callable[
                [t.Iterable], t.Iterable], None], optional): if True, a rich trackbar
                is shown; if a string is passed, it is set as the message for the
                default rich trackbar; otherwise you should provide your own callable
                to track the progress, defaults to True  (Default to True)
        """
        samples: t.List[Sample] = []

        def _store_sample(x: Sample, idx: int):
            if len(samples) <= idx:  # pragma: no branch
                samples.extend([Sample() for _ in range(idx - len(samples) + 1)])
            samples[idx] = x

        self.run(
            num_workers=num_workers,
            prefetch=prefetch,
            keep_order=False,
            sample_fn=_store_sample,
            track_fn=track_fn,
        )

        return SamplesSequence.from_list(samples)

    def run(
        self,
        *,
        num_workers: int = 0,
        prefetch: int = 2,
        keep_order: bool = False,
        sample_fn: t.Union[
            t.Callable[[Sample], None], t.Callable[[Sample, int], None], None
        ] = None,
        track_fn: t.Union[bool, str, t.Callable[[t.Iterable], t.Iterable], None] = True,
    ):
        """Goes through all the samples of the sequence, optionally using multiple
        processes and applying `sample_fn` to each sample. Also, a `track_fn` can be
        defined, eg, to show the progress.

        Args:
            num_workers (int, optional): The number of processes to spawn. If negative,
                the number of (logical) cpu cores is used  (Default to 0)
            prefetch (int, optional): The number of samples loaded in advanced
                by each worker  (Default to 2)
            keep_order (bool, optional): Whether to retrieve the samples in the original
                order  (Default to False)
            sample_fn (t.Optional[t.Callable[[Sample], None]], optional): a callable to
                run on each sample  (Default to None)
            track_fn (track_fn: t.Union[bool, str, t.Callable[
                [t.Iterable], t.Iterable], None], optional): if True, a trackbar
                is shown; if a string is passed, it is set as the message for the
                default trackbar; otherwise you should provide your own callable
                to track the progress  (Default to True)
        """
        from pipelime.sequences import Grabber, grab_all

        if isinstance(track_fn, (bool, str)):
            if track_fn:
                from pipelime.piper.progress.tracker.base import TqdmTask

                message = track_fn if isinstance(track_fn, str) else ""

                def _tqdm_track_fn(x):
                    return TqdmTask.default_bar(
                        iterable=x, total=len(self), message=message
                    )

                track_fn = _tqdm_track_fn
            else:
                track_fn = None

        grabber = Grabber(
            num_workers=num_workers, prefetch=prefetch, keep_order=keep_order
        )
        grab_all(grabber, self, sample_fn=sample_fn, track_fn=track_fn)  # type: ignore

    def to_pipe(
        self, recursive: bool = True, objs_to_str: bool = True
    ) -> t.List[t.Dict[str, t.Any]]:
        """Serializes this sequence to a pipe list. You can then pass this list to
        `pipelime.sequences.build_pipe` to reconstruct the sequence.
        NB: nested sequences are recursively serialized only if `recursive` is True,
        while other objects are not. Consider to use `pipelime.choixe` features to
        fully (de)-serialized them to yaml/json.

        Args:
            recursive (bool, optional): if True nested sequences are recursively
                serialized  (Default to True)
            objs_to_str (bool, optional): if True objects are converted to string.
                (Default to True)

        Returns:
            t.List[t.Dict[str, t.Any]]: the serialized pipe list.

        Raises:
            ValueError: if a field is tagged as `pipe_source` but it is not
                a SamplesSequence.

        """

        def _maybe_go_deeper(field_value):
            if isinstance(field_value, SamplesSequence):
                if recursive:
                    field_value = field_value.to_pipe(
                        recursive=recursive, objs_to_str=objs_to_str
                    )
            elif isinstance(field_value, pyd.BaseModel):
                if recursive:
                    # NB: do not unfold sub-pydantic models, since it may not be
                    # straightforward to de-serialize them when subclasses are used
                    field_value = field_value.dict()
            elif isinstance(field_value, t.Sequence):
                field_value = [_maybe_go_deeper(x) for x in field_value]
            elif isinstance(field_value, t.Mapping):
                field_value = {k: _maybe_go_deeper(v) for k, v in field_value.items()}

            if (
                not objs_to_str
                or isinstance(
                    field_value,
                    (str, bytes, int, float, bool, t.Mapping, t.Sequence),
                )
                or field_value is None
            ):
                return field_value
            return str(field_value)

        source_list = []
        arg_dict = {}
        for field_name, model_field in self.__fields__.items():
            field_value = getattr(self, field_name)
            field_alias = model_field.alias
            if model_field.field_info.extra.get("pipe_source", False):
                if not isinstance(field_value, SamplesSequence):
                    raise ValueError(
                        f"{field_alias} is tagged as `pipe_source`, "
                        "but it is not a SamplesSequence instance."
                    )
                source_list = field_value.to_pipe(
                    recursive=recursive, objs_to_str=objs_to_str
                )
            else:
                arg_dict[field_alias] = _maybe_go_deeper(field_value)

        return source_list + [{self._operator_path: arg_dict}]

    def __str__(self) -> str:
        return repr(self)

    ###########################################################################
    # FUNCTION STUBS FOR TYPE CHECKING AND AUTO-COMPLETION
    ###########################################################################

    @samples_sequence_stub
    @staticmethod
    def from_callable(
        *,
        generator_fn: t.Callable[[int], Sample],
        length: t.Union[int, t.Callable[[], int]],
    ) -> SamplesSequence:
        """A SamplesSequence calling a user-defined generator to get the samples.
        Run `pipelime help from_callable` to read the complete documentation.
        """

    @samples_sequence_stub
    @staticmethod
    def from_list(samples: t.Sequence[Sample]) -> SamplesSequence:
        """A SamplesSequence from a list of Samples.
        Run `pipelime help from_list` to read the complete documentation.
        """

    @samples_sequence_stub
    @staticmethod
    def from_underfolder(
        folder: "Path",  # type: ignore # noqa: E602,F821
        *,
        merge_root_items: bool = True,
        must_exist: bool = True,
        watch: bool = False,
    ) -> SamplesSequence:
        """A SamplesSequence loading data from an Underfolder dataset.
        Run `pipelime help from_underfolder` to read the complete documentation.
        """

    @samples_sequence_stub
    @staticmethod
    def from_images(
        folder: "Path",  # type: ignore # noqa: E602,F821
        *,
        must_exist: bool = True,
        image_key: str = "image",
        sort_files: bool = False,
        recursive: bool = True,
    ) -> SamplesSequence:
        """A SamplesSequence loading images from a folder.
        Run `pipelime help from_images` to read the complete documentation.
        """

    @samples_sequence_stub
    @staticmethod
    def from_video(
        video: "Path",  # type: ignore # noqa: E602,F821
        *,
        must_exist: bool = True,
        image_key: str = "image",
    ) -> SamplesSequence:
        """A SamplesSequence loading frames from a video.
        Run `pipelime help from_video` to read the complete documentation.
        """

    @samples_sequence_stub
    @staticmethod
    def toy_dataset(
        length: int,
        *,
        with_images: bool = True,
        with_masks: bool = True,
        with_instances: bool = True,
        with_objects: bool = True,
        with_bboxes: bool = True,
        with_kpts: bool = True,
        image_size: int = 256,
        key_format: str = "*",
        max_labels: int = 5,
        objects_range: t.Tuple[int, int] = (1, 5),
        seed: t.Optional[int] = None,
    ) -> SamplesSequence:
        """A fake sequence of generated samples.
        Run `pipelime help toy_dataset` to read the complete documentation.
        """

    @samples_sequence_stub
    def map(
        self,
        stage: t.Union[
            "StageInput",
            "SampleStage",
            str,
            bytes,
            t.Mapping[str, t.Optional[t.Mapping[str, t.Any]]],
        ],
    ) -> SamplesSequence:
        """Applies a stage on all samples.
        Run `pipelime help map` to read the complete documentation.
        """

    @samples_sequence_stub
    def map_if(
        self,
        stage: t.Union[
            "StageInput",
            "SampleStage",
            str,
            bytes,
            t.Mapping[str, t.Optional[t.Mapping[str, t.Any]]],
        ],
        condition: t.Union[
            t.Callable[[], bool],
            t.Callable[[int], bool],
            t.Callable[[int, Sample], bool],
            t.Callable[[int, Sample, SamplesSequence], bool],
        ],
    ) -> SamplesSequence:
        """Applies a stage on all samples if a condition returns True.
        Run `pipelime help map_if` to read the complete documentation.
        """

    @samples_sequence_stub
    def zip(self, to_zip: SamplesSequence, *, key_format: str = "*") -> SamplesSequence:
        """Zips two Sequences by merging each Sample.
        Run `pipelime help zip` to read the complete documentation.
        """

    @samples_sequence_stub
    def cat(self, *seqs: SamplesSequence, interleave: bool = False) -> SamplesSequence:
        """Concatenates two or more SamplesSequences.
        Run `pipelime help cat` to read the complete documentation.
        """

    @samples_sequence_stub
    def filter(
        self,
        filter_fn: t.Callable[[Sample], bool],
        lazy: bool = True,
        insert_empty_samples: bool = False,
    ) -> SamplesSequence:
        """A filtered view of a SamplesSequence.
        Run `pipelime help filter` to read the complete documentation.
        """

    @samples_sequence_stub
    def sort(
        self, key_fn: t.Callable[[Sample], t.Any], lazy: bool = True
    ) -> SamplesSequence:
        """A sorted view of an input SamplesSequence.
        Run `pipelime help sort` to read the complete documentation.
        """

    @samples_sequence_stub
    def slice(
        self,
        *,
        start: t.Optional[int] = None,
        stop: t.Optional[int] = None,
        step: t.Optional[int] = None,
    ) -> SamplesSequence:
        """Functional version of the slice operator `self[start_idx:end_idx:step]`.
        Run `pipelime help slice` to read the complete documentation.
        """

    @samples_sequence_stub
    def select(
        self, indexes: t.Sequence[int], *, negate: bool = False
    ) -> SamplesSequence:
        """Given a list of indexes, extracts the corresponding samples
        from the input SamplesSequence. The index sequence is not automatically sorted.
        Run `pipelime help select` to read the complete documentation.
        """

    @samples_sequence_stub
    def shuffle(self, *, seed: t.Optional[int] = None) -> SamplesSequence:
        """Shuffles samples in the input SamplesSequence.
        Run `pipelime help shuffle` to read the complete documentation.
        """

    @samples_sequence_stub
    def enumerate(
        self,
        *,
        idx_key: str = "~idx",
        item_cls: str = "pipelime.items.TxtNumpyItem",
    ) -> SamplesSequence:
        """Add a new index item to each Sample in the input SamplesSequence.
        Run `pipelime help enumerate` to read the complete documentation.
        """

    @samples_sequence_stub
    def repeat(self, count: int, interleave: bool = False) -> SamplesSequence:
        """Repeat this sequence so each sample is seen multiple times.
        Run `pipelime help repeat` to read the complete documentation.
        """

    @samples_sequence_stub
    def cache(
        self,
        cache_folder: t.Optional["pathlib.Path"] = None,  # type: ignore # noqa: E602,F821
        *,
        reuse_cache: bool = False,
    ) -> SamplesSequence:
        """Cache the input Samples the first time they are accessed.
        Run `pipelime help cache` to read the complete documentation.
        """

    @samples_sequence_stub
    def data_cache(
        self, *items: t.Union[t.Type["pipelime.items.Item"], str]  # type: ignore # noqa: E602,F821
    ) -> SamplesSequence:
        """Enables item data caching on previous pipeline steps.
        Run `pipelime help data_cache` to read the complete documentation.
        """

    @samples_sequence_stub
    def no_data_cache(
        self, *items: t.Union[t.Type["pipelime.items.Item"], str]  # type: ignore # noqa: E602,F821
    ) -> SamplesSequence:
        """Disables item data caching on previous pipeline steps.
        Run `pipelime help no_data_cache` to read the complete documentation.
        """

    @samples_sequence_stub
    def to_underfolder(
        self,
        folder: "Path",  # type: ignore # noqa: E602,F821
        *,
        zfill: t.Optional[int] = None,
        key_serialization_mode: t.Optional[
            t.Mapping[
                str, t.Union["pipelime.items.SerializationMode", str]  # type: ignore # noqa: E602,F821
            ]
        ] = None,
        exists_ok: bool = False,
    ) -> SamplesSequence:
        """Writes samples to an underfolder dataset while iterating over them.
        Run `pipelime help to_underfolder` to read the complete documentation.
        """

    @samples_sequence_stub
    def validate_samples(
        self, *, sample_schema: "SampleValidationInterface"
    ) -> SamplesSequence:
        """Validates the source sequence against a schema.
        Run `pipelime help validate_samples` to read the complete documentation.
        """

    @samples_sequence_stub
    def unbatched(
        self,
        *,
        batch_size: t.Union[int, t.Literal["fixed", "variable"]] = "fixed",
        key_list: t.Optional[t.Sequence[str]] = None,
    ) -> SamplesSequence:
        """Un-batch items by un-stacking along the first dimension.
        Shared items are not touched.
        """

    @samples_sequence_stub
    def batched(
        self,
        *,
        batch_size: int,
        drop_last: bool = False,
        key_list: t.Optional[t.Sequence[str]] = None,
    ) -> SamplesSequence:
        """Batch items by stacking them along a new dimension.
        Shared items are not touched.
        """

    ###########################################################################
    # STUBS END
    ###########################################################################


def _add_operator_path(cls: t.Type[SamplesSequence]) -> t.Type[SamplesSequence]:
    import inspect
    from pathlib import Path

    if cls.__module__.startswith("pipelime"):
        cls._operator_path = cls.name()
    else:
        try:
            source_path = inspect.getfile(cls)
            module_path = Path(source_path).resolve().as_posix()
        except (TypeError, OSError):
            # this happens when the class does not come from a file
            module_path = "__main__"

        if (cls.__module__.replace(".", "/") + ".py") in module_path:
            module_path = cls.__module__
        cls._operator_path = module_path + ":" + cls.name()
    return cls


def source_sequence(cls: t.Type[SamplesSequence]) -> t.Type[SamplesSequence]:
    import inspect

    if cls.name() in SamplesSequence._sources or cls.name() in SamplesSequence._pipes:
        logger.warning(f"Function {cls.name()} has been already registered.")

    def _helper(*args, **kwargs):
        return cls(*args, **kwargs)

    raw_doc = inspect.getdoc(cls) or ""
    _helper.__doc__ = (
        raw_doc
        + ("\n" if raw_doc else "")
        + f"\nRun ``$ pipelime help {cls.name()}`` to read the complete documentation."
    )
    _helper.__signature__ = inspect.signature(cls)

    setattr(SamplesSequence, cls.name(), staticmethod(_helper))
    SamplesSequence._sources[cls.name()] = cls
    cls = _add_operator_path(cls)
    return cls


def piped_sequence(cls: t.Type[SamplesSequence]) -> t.Type[SamplesSequence]:
    import inspect

    if cls.name() in SamplesSequence._sources or cls.name() in SamplesSequence._pipes:
        logger.warning(f"Function {cls.name()} has been already registered.")

    prms_source_name = None
    for mfield in cls.__fields__.values():
        if mfield.field_info.extra.get("pipe_source", False):
            if prms_source_name is not None:
                raise ValueError(
                    f"More than one field has `pipe_source=True` in {cls.__name__}."
                )
            prms_source_name = mfield.alias
    if prms_source_name is None:
        raise ValueError(
            f"{cls.__name__} is tagged as `piped`, but no field has `pipe_source=True`."
        )

    def _helper(self, *args, **kwargs):
        return cls(*args, **{prms_source_name: self}, **kwargs)

    raw_doc = inspect.getdoc(cls) or ""
    _helper.__doc__ = (
        raw_doc
        + ("\n" if raw_doc else "")
        + f"\nRun ``$ pipelime help {cls.name()}`` to read the complete documentation."
    )
    _helper.__signature__ = inspect.Signature(
        parameters=[inspect.Parameter("self", inspect.Parameter.POSITIONAL_ONLY)]
        + [
            v
            for k, v in inspect.signature(cls).parameters.items()
            if k != prms_source_name
        ],
        return_annotation=SamplesSequence,
    )

    setattr(SamplesSequence, cls.name(), _helper)
    SamplesSequence._pipes[cls.name()] = cls
    cls = _add_operator_path(cls)
    return cls
