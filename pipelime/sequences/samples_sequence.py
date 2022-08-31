from __future__ import annotations
from abc import abstractmethod
import itertools
import typing as t
import pydantic as pyd
from loguru import logger

from pipelime.sequences.sample import Sample


class SamplesSequenceBase(t.Sequence[Sample]):
    @abstractmethod
    def size(self) -> int:
        pass

    @abstractmethod
    def get_sample(self, idx: int) -> Sample:
        pass

    def __len__(self) -> int:
        return self.size()

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
        return len(str(len(self) - 1))


class SamplesSequence(
    SamplesSequenceBase, pyd.BaseModel, extra="forbid", copy_on_model_validation=False
):
    """A generic sequence of samples. Subclasses should implement `size(self) -> int`
    and `get_sample(self, idx: int) -> Sample`.

    The list of all available pipes and sources, along with respective pydantic models,
    can be retrieved through `pipelime list --details`.

    NB: when defining a pipe, the `source` sample sequence must be bound to a pydantic
    Field with `pipe_source=True`.
    """

    _sources: t.ClassVar[t.Dict[str, t.Type[SamplesSequence]]] = {}
    _pipes: t.ClassVar[t.Dict[str, t.Type[SamplesSequence]]] = {}
    _operator_path: t.ClassVar[str] = ""

    @t.overload
    def __getitem__(self, idx: int) -> Sample:
        ...

    @t.overload
    def __getitem__(self, idx: slice) -> SamplesSequence:
        ...

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

    def direct_access(self) -> t.Sequence[t.Mapping[str, t.Any]]:
        """Returns a sequence of key-to-value mappings,
        with no intermediate Sample and Item classes.
        """
        from pipelime.sequences.direct_access import DirectAccessSequence

        return DirectAccessSequence(self)

    def torch_dataset(self) -> "torch.utils.data.Dataset":  # type: ignore # noqa: E602,F821
        """Returns a torch.utils.data.Dataset interface of this samples sequence."""
        from pipelime.sequences.torch import TorchDataset

        return TorchDataset(self)

    def batch(self, batch_size: int, drop_last: bool = False, fill: Sample = Sample()):
        """Returns a zip-like object to get batches of samples. If the number of
        samples is not a multiple of batch_size and `drop_last` is False,
        the last batch will be filled with `fill`.
        """
        src_iters = [iter(self)] * batch_size
        if drop_last:
            return zip(*src_iters)
        return itertools.zip_longest(*src_iters, fillvalue=fill)

    def run(
        self,
        *,
        num_workers: int = 0,
        prefetch: int = 2,
        keep_order: bool = False,
        sample_fn: t.Optional[t.Callable[[Sample], None]] = None,
        track_fn: t.Optional[t.Callable[[t.Iterable], t.Iterable]] = None,
    ):
        """Go through all the samples of the sequence, optionally using multiple
        processes and applying `sample_fn` to each sample. Also, a `track_fn` can be
        defined, eg, to show the progress.

        :param num_workers: The number of processes to spawn. If negative,
            the number of (logical) cpu cores is used, defaults to 0
        :type num_workers: int, optional
        :param prefetch: The number of samples loaded in advanced by each worker,
            defaults to 2
        :type prefetch: int, optional
        :param keep_order: Whether to retrieve the samples in the original order,
            defaults to False
        :type keep_order: bool, optional
        :param sample_fn: a callable to run on each sample, defaults to None
        :type sample_fn: t.Optional[t.Callable[[Sample], None]], optional
        :param track_fn: a callable to track the progress, defaults to None
        :type track_fn: t.Optional[t.Callable[[t.Iterable], t.Iterable]], optional
        """
        from pipelime.sequences import Grabber, grab_all

        grabber = Grabber(
            num_workers=num_workers, prefetch=prefetch, keep_order=keep_order
        )
        grab_all(grabber, self, sample_fn=sample_fn, track_fn=track_fn)

    def to_pipe(
        self, recursive: bool = True, objs_to_str: bool = True
    ) -> t.List[t.Dict[str, t.Any]]:
        """Serializes this sequence to a pipe list. You can then pass this list to
        `pipelime.sequences.build_pipe` to reconstruct the sequence.
        NB: nested sequences are recursively serialized only if `recursive` is True,
        while other objects are not. Consider to use `pipelime.choixe` features to
        fully (de)-serialized them to YAML/JSON.

        :param recursive: if True nested sequences are recursively serialized,
            defaults to True.
        :type recursive: bool, optional.
        :param objs_to_str: if True objects are converted to string.
        :type objs_to_str: bool, optional.
        :raises ValueError: if a field is tagged as `pipe_source` but it is not
            a SamplesSequence.
        :return: the serialized pipe list.
        :rtype: t.List[t.Dict[str, t.Any]]
        """
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
                # NB: do not unfold sub-pydantic models, since it may not be
                # straightforward to de-serialize them when subclasses are used
                if recursive:
                    if isinstance(field_value, SamplesSequence):
                        field_value = field_value.to_pipe(
                            recursive=recursive, objs_to_str=objs_to_str
                        )
                    elif isinstance(field_value, pyd.BaseModel):
                        field_value = field_value.dict()
                arg_dict[field_alias] = (
                    field_value
                    if not objs_to_str
                    or isinstance(
                        field_value,
                        (str, bytes, int, float, bool, t.Mapping, t.Sequence),
                    )
                    else str(field_value)
                )
        return source_list + [{self._operator_path: arg_dict}]

    def __str__(self) -> str:
        return repr(self)

    ###########################################################################
    # FUNCTION STUBS FOR TYPE CHECKING AND AUTO-COMPLETION
    ###########################################################################

    @staticmethod
    def from_callable(
        *,
        generator_fn: t.Callable[[int], Sample],
        length: t.Union[int, t.Callable[[], int]],
    ) -> SamplesSequence:
        """A SamplesSequence calling a user-defined generator to get the samples.
        Run `pipelime help from_callable` to read the complete documentation.
        """
        ...

    @staticmethod
    def from_list(samples: t.Sequence[Sample]) -> SamplesSequence:
        """A SamplesSequence from a list of Samples.
        Run `pipelime help from_list` to read the complete documentation.
        """
        ...

    @staticmethod
    def from_underfolder(
        folder: "pathlib.Path",  # type: ignore # noqa: E602,F821
        *,
        merge_root_items: bool = True,
        must_exist: bool = True,
        watch: bool = False,
    ) -> SamplesSequence:
        """A SamplesSequence loading data from an Underfolder dataset.
        Run `pipelime help from_underfolder` to read the complete documentation.
        """
        ...

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
    ) -> SamplesSequence:
        """A fake sequence of generated samples.
        Run `pipelime help toy_dataset` to read the complete documentation.
        """
        ...

    def map(
        self,
        stage: t.Union[
            "pipelime.stages.SampleStage",  # type: ignore # noqa: E602,F821
            t.Mapping[str, t.Optional[t.Mapping[str, t.Any]]],
        ],
    ) -> SamplesSequence:
        """Applies a stage on all samples.
        Run `pipelime help map` to read the complete documentation.
        """
        ...

    def zip(self, to_zip: SamplesSequence, *, key_format: str = "*") -> SamplesSequence:
        """Zips two Sequences by merging each Sample.
        Run `pipelime help zip` to read the complete documentation.
        """
        ...

    def cat(self, to_cat: SamplesSequence) -> SamplesSequence:
        """Concatenates two SamplesSequences.
        Run `pipelime help cat` to read the complete documentation.
        """
        ...

    def filter(self, filter_fn: t.Callable[[Sample], bool]) -> SamplesSequence:
        """A filtered view of a SamplesSequence.
        Run `pipelime help filter` to read the complete documentation.
        """
        ...

    def sort(self, key_fn: t.Callable[[Sample], t.Any]) -> SamplesSequence:
        """A sorted view of an input SamplesSequence.
        Run `pipelime help sort` to read the complete documentation.
        """
        ...

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
        ...

    def select(
        self, indexes: t.Sequence[int], *, negate: bool = False
    ) -> SamplesSequence:
        """Given a list of indexes, extracts the corresponding samples
        from the input SamplesSequence. The index sequence is not automatically sorted.
        Run `pipelime help select` to read the complete documentation.
        """
        ...

    def shuffle(self, *, seed: t.Optional[int] = None) -> SamplesSequence:
        """Shuffles samples in the input SamplesSequence.
        Run `pipelime help shuffle` to read the complete documentation.
        """
        ...

    def enumerate(
        self,
        *,
        idx_key: str = "~idx",
        item_cls_path: str = "pipelime.items.TxtNumpyItem",
    ) -> SamplesSequence:
        """Add a new index item to each Sample in the input SamplesSequence.
        Run `pipelime help enumerate` to read the complete documentation.
        """
        ...

    def repeat(self, count: int) -> SamplesSequence:
        """Repeat this sequence so each sample is seen multiple times.
        Run `pipelime help repeat` to read the complete documentation.
        """
        ...

    def cache(
        self,
        cache_folder: t.Optional["pathlib.Path"] = None,  # type: ignore # noqa: E602,F821
        *,
        reuse_cache: bool = False,
    ) -> SamplesSequence:
        """Cache the input Samples the first time they are accessed.
        Run `pipelime help cache` to read the complete documentation.
        """
        ...

    def to_underfolder(
        self,
        folder: "pathlib.Path",  # type: ignore # noqa: E602,F821
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
        ...

    def validate_samples(
        self,
        *,
        sample_schema: t.Type[pyd.BaseModel],
        lazy: bool = False,
        max_samples: int = 1,
    ) -> SamplesSequence:
        """Validates the source sequence against a schema.
        Run `pipelime help validate_samples` to read the complete documentation.
        """
        ...

    ###########################################################################
    # STUBS END
    ###########################################################################


def _add_operator_path(cls: t.Type[SamplesSequence]) -> t.Type[SamplesSequence]:
    from pathlib import Path
    import inspect

    if cls.__module__.startswith("pipelime"):
        cls._operator_path = cls.name()
    else:
        module_path = Path(inspect.getfile(cls)).resolve().as_posix()
        if (cls.__module__.replace(".", "/") + ".py") in module_path:
            module_path = cls.__module__
        cls._operator_path = module_path + ":" + cls.name()
    return cls


def source_sequence(cls: t.Type[SamplesSequence]) -> t.Type[SamplesSequence]:
    if cls.name() in SamplesSequence._sources or cls.name() in SamplesSequence._pipes:
        logger.warning(f"Function {cls.name()} has been already registered.")

    setattr(SamplesSequence, cls.name(), cls)
    SamplesSequence._sources[cls.name()] = cls
    cls = _add_operator_path(cls)
    return cls


def piped_sequence(cls: t.Type[SamplesSequence]) -> t.Type[SamplesSequence]:
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

    setattr(SamplesSequence, cls.name(), _helper)
    SamplesSequence._pipes[cls.name()] = cls
    cls = _add_operator_path(cls)
    return cls
