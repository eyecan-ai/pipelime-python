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

    def __getitem__(self, idx: t.Union[int, slice]) -> Sample:
        return (
            self.slice(start=idx.start, stop=idx.stop, step=idx.step)  # type: ignore
            if isinstance(idx, slice)
            else self.get_sample(idx if idx >= 0 else len(self) + idx)
        )

    def __add__(self, other: SamplesSequence) -> SamplesSequence:
        return self.cat(other)  # type: ignore

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

    def torch_dataset(self) -> "torch.utils.data.Dataset":  # type: ignore # noqa: E602 # noqa: F821
        """Returns a torch.utils.data.Dataset interface of this samples sequence."""
        from pipelime.sequences.torch import TorchDataset

        return TorchDataset(self)

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
    if hasattr(SamplesSequence, cls.name()):
        logger.warning(f"Function {cls.name()} has been already registered.")

    setattr(SamplesSequence, cls.name(), cls)
    SamplesSequence._sources[cls.name()] = cls
    cls = _add_operator_path(cls)
    return cls


def piped_sequence(cls: t.Type[SamplesSequence]) -> t.Type[SamplesSequence]:
    if hasattr(SamplesSequence, cls.name()):
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
