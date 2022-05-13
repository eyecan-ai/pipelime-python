from __future__ import annotations
from abc import abstractmethod
import itertools
import typing as t
import pydantic as pyd

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

    def __add__(self, other: SamplesSequence) -> SamplesSequence:
        return self.cat(other)  # type: ignore


class SamplesSequence(SamplesSequenceBase, pyd.BaseModel):
    """A generic sequence of samples. Subclasses should implement `size(self) -> int`
    and `get_sample(self, idx: int) -> Sample`.

    The list of all available pipes and sources, along with respective schemas, can be
    retrieved through `SamplesSequence.pipes` and `SamplesSequence.sources`. Also,
    descriptive help messages are provided for each method, eg, try
    `help(SamplesSequence.map)`.

    NB: when defining a pipe, the `source` sample sequence must be bound to a pydantic
    Field with `pipe_source=True`.
    """

    _sources: t.ClassVar[t.Dict[str, t.Type[SamplesSequence]]] = {}
    _pipes: t.ClassVar[t.Dict[str, t.Type[SamplesSequence]]] = {}

    @classmethod
    def name(cls) -> str:
        if cls.__config__.title:
            return cls.__config__.title
        return cls.__name__

    def to_pipe(self, recursive: bool) -> t.List[t.Dict[str, t.Any]]:
        source_list = []
        arg_dict = {}
        for field_name, model_field in self.__fields__.items():
            field_value = getattr(self, field_name)
            if model_field.field_info.extra.get("pipe_source", False):
                if not isinstance(field_value, SamplesSequence):
                    raise ValueError(
                        f"{field_name} is tagged as `pipe_source`, "
                        "so it must be a SamplesSequence instance."
                    )
                source_list = field_value.to_pipe(recursive)
            else:
                # NB: do not unfold sub-pydantic models, since it may not be
                # straightforward to de-serialize them when subclasses are used
                if recursive and isinstance(field_value, SamplesSequence):
                    field_value = field_value.to_pipe(recursive)
                arg_dict[field_name] = field_value
        return source_list + [{self.name(): arg_dict}]


def source_sequence(cls: t.Type[SamplesSequence]) -> t.Type[SamplesSequence]:
    if hasattr(SamplesSequence, cls.name()):
        raise ValueError(f"Function {cls.name()} has been already registered.")

    setattr(SamplesSequence, cls.name(), cls)
    SamplesSequence._sources[cls.name()] = cls
    return cls


def piped_sequence(cls: t.Type[SamplesSequence]) -> t.Type[SamplesSequence]:
    if hasattr(SamplesSequence, cls.name()):
        raise ValueError(f"Function {cls.name()} has been already registered.")

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
    return cls
