import typing as t

import pydantic as pyd

import pipelime.commands.interfaces as pl_interfaces
from pipelime.piper import PipelimeCommand, PiperPortType


class SplitBase(
    pl_interfaces.PydanticFieldNoDefaultMixin,
    pyd.BaseModel,
    allow_population_by_field_name=True,
    extra="forbid",
    copy_on_model_validation="none",
):
    output: t.Optional[
        pl_interfaces.OutputDatasetInterface
    ] = pl_interfaces.OutputDatasetInterface.pyd_field(
        alias="o",
        is_required=False,
        description="Output split. Leave to None to not save to disk.",
    )

    def __repr__(self) -> str:
        return self.__piper_repr__()

    def __piper_repr__(self) -> str:
        return "" if self.output is None else self.output.__piper_repr__()


class PercSplit(SplitBase):
    _default_type_description: t.ClassVar[
        t.Optional[str]
    ] = "Percentage splits definition."
    _compact_form: t.ClassVar[t.Optional[str]] = "<fraction>[,<folder>]"

    fraction: t.Optional[float] = pyd.Field(
        ...,
        gt=0.0,
        le=1.0,
        description=(
            "Fraction of the dataset to keep. "
            "A None value takes all the samples left by the other splits."
        ),
    )

    def split_size(self, n_samples: int) -> t.Optional[int]:
        return int(n_samples * self.fraction) if self.fraction is not None else None

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value):
        if isinstance(value, PercSplit):
            return value
        if isinstance(value, (str, bytes, float)):
            data = {}
            if isinstance(value, float):
                data["fraction"] = value
            else:
                sz, _, fld = str(value).partition(",")
                try:
                    data["fraction"] = float(sz)
                except ValueError:
                    # NB: leave size as-is if not valid, it will raise an error
                    data["fraction"] = (
                        None if sz.lower() in ("none", "null", "nul") else sz
                    )
                if fld:
                    data["output"] = pl_interfaces.OutputDatasetInterface(
                        folder=fld  # type: ignore
                    )
            value = data
        if isinstance(value, t.Mapping):
            return PercSplit(**value)
        raise ValueError("Invalid perc split definition.")


class AbsoluteSplit(SplitBase):
    _default_type_description: t.ClassVar[
        t.Optional[str]
    ] = "Absolute splits definition."
    _compact_form: t.ClassVar[t.Optional[str]] = "<length>[,<folder>]"

    length: t.Optional[pyd.PositiveInt] = pyd.Field(
        ...,
        description=(
            "Number of elements to keep. "
            "A None value takes all the samples left by the other splits."
        ),
    )

    def split_size(self, *args, **kwargs) -> t.Optional[int]:
        return self.length

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value):
        if isinstance(value, AbsoluteSplit):
            return value
        if isinstance(value, (str, bytes, int)):
            data = {}
            if isinstance(value, int):
                data["length"] = value
            else:
                sz, _, fld = str(value).partition(",")
                try:
                    data["length"] = int(sz)
                except ValueError:
                    # NB: leave size as-is if not valid, it will raise an error
                    data["length"] = (
                        None if sz.lower() in ("none", "null", "nul") else sz
                    )

                if fld:
                    data["output"] = pl_interfaces.OutputDatasetInterface(
                        folder=fld  # type: ignore
                    )
            value = data
        if isinstance(value, t.Mapping):
            return AbsoluteSplit(**value)
        raise ValueError("Invalid absolute split definition.")


class Splits(pl_interfaces.PydanticFieldNoDefaultMixin):
    _default_type_description: t.ClassVar[t.Optional[str]] = "Splits definition."
    _compact_form: t.ClassVar[t.Optional[str]] = "<fraction|length>[,<folder>]"

    any_split_t = t.Union[
        PercSplit, AbsoluteSplit, t.Sequence[t.Union[PercSplit, AbsoluteSplit]]
    ]


class SplitCommand(PipelimeCommand, title="split"):
    """Split a dataset."""

    input: pl_interfaces.InputDatasetInterface = (
        pl_interfaces.InputDatasetInterface.pyd_field(
            alias="i", piper_port=PiperPortType.INPUT
        )
    )

    shuffle: t.Union[bool, pyd.PositiveInt] = pyd.Field(
        False,
        alias="shf",
        description=(
            "Shuffle the dataset before subsampling and splitting. "
            "Optionally specify the random seed."
        ),
    )
    subsample: pyd.PositiveInt = pyd.Field(
        1,
        alias="ss",
        description="Take 1-every-nth input sample. Applied after shuffling.",
    )

    splits: Splits.any_split_t = Splits.pyd_field(
        alias="s", piper_port=PiperPortType.OUTPUT
    )

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )

    def run(self):
        reader = self.input.create_reader()
        if self.shuffle:
            reader = reader.shuffle(
                seed=self.shuffle if not isinstance(self.shuffle, bool) else None
            )
        if self.subsample != 1:
            reader = reader[:: self.subsample]
        input_length = len(reader)

        splits = self.splits if isinstance(self.splits, t.Sequence) else [self.splits]
        split_sizes = [s.split_size(input_length) for s in splits]
        none_idx = -1
        split_total = 0
        for i, s in enumerate(split_sizes):
            if s is None:
                if none_idx >= 0:
                    raise ValueError("Only one split size can be None.")
                none_idx = i
            else:
                split_total += s

        if split_total > input_length:
            raise ValueError(
                f"The sum of the split sizes ({split_total}) "
                f"is greater than the input length ({input_length})."
            )
        if none_idx >= 0:
            split_sizes[none_idx] = input_length - split_total

        split_start = 0
        for idx, split_length, split in zip(
            range(len(split_sizes)), split_sizes, splits
        ):
            split_stop = split_start + split_length  # type: ignore
            if split.output is not None:
                seq = reader[split_start:split_stop]
                seq = split.output.append_writer(seq)
                self.grabber.grab_all(
                    seq,
                    grab_context_manager=split.output.serialization_cm(),
                    keep_order=False,
                    parent_cmd=self,
                    track_message=(
                        f"Writing split {idx + 1}/{len(split_sizes)} "
                        f"({split_length} samples)"
                    ),
                )
            split_start = split_stop


class _MatchHelper:
    def __init__(self, query: str, negate: bool):
        self._query = query
        self._negate = negate

    def __call__(self, x):
        m: bool = x.match(self._query)
        if self._negate:
            return not m
        return m


class SplitByQueryCommand(PipelimeCommand, title="split-query"):
    """Split a dataset by query."""

    input: pl_interfaces.InputDatasetInterface = (
        pl_interfaces.InputDatasetInterface.pyd_field(
            alias="i", piper_port=PiperPortType.INPUT
        )
    )

    query: str = pyd.Field(
        ...,
        alias="q",
        description=("A query to match (cfr. https://github.com/cyberlis/dictquery)."),
    )

    output_selected: t.Optional[
        pl_interfaces.OutputDatasetInterface
    ] = pl_interfaces.OutputDatasetInterface.pyd_field(
        alias="os",
        is_required=False,
        description=(
            "Output dataset of sample selected by the query. "
            "Leave to None to not save to disk."
        ),
        piper_port=PiperPortType.OUTPUT,
    )

    output_discarded: t.Optional[
        pl_interfaces.OutputDatasetInterface
    ] = pl_interfaces.OutputDatasetInterface.pyd_field(
        alias="od",
        is_required=False,
        description=(
            "Output dataset of sample discarded by the query. "
            "Leave to None to not save to disk."
        ),
        piper_port=PiperPortType.OUTPUT,
    )

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )

    def run(self):
        # NOTE: do not use lambda, since they do not play well with multiprocessing
        reader = self.input.create_reader()
        if self.output_selected is not None:
            self._filter(
                reader,
                self.output_selected,
                _MatchHelper(self.query, False),
                "selected samples",
            )
        if self.output_discarded is not None:
            self._filter(
                reader,
                self.output_discarded,
                _MatchHelper(self.query, True),
                "discarded samples",
            )

    def _filter(self, reader, output, fn, message):
        seq = reader.filter(fn)
        seq = output.append_writer(seq)
        self.grabber.grab_all(
            seq,
            grab_context_manager=output.serialization_cm(),
            keep_order=False,
            parent_cmd=self,
            track_message=f"Writing {message} ({len(seq)} samples)",
        )


class SplitByValueCommand(PipelimeCommand, title="split-value"):
    """Creates a new sequence for each unique value of a given key."""

    input: pl_interfaces.InputDatasetInterface = (
        pl_interfaces.InputDatasetInterface.pyd_field(
            alias="i", piper_port=PiperPortType.INPUT
        )
    )

    key: str = pyd.Field(
        ...,
        alias="k",
        description=(
            "The key to use. A pydash-like dot notation "
            "can be used to access nested attributes."
        ),
    )

    output: pl_interfaces.OutputDatasetInterface = (
        pl_interfaces.OutputDatasetInterface.pyd_field(
            alias="o",
            description=(
                "Common options for the output sequences, "
                "which will be placed in subfolders."
            ),
            piper_port=PiperPortType.OUTPUT,
        )
    )

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )

    def run(self):
        import uuid

        from pipelime.sequences import Sample

        class WorkerHelper:
            def __init__(self, idx_key, value_key):
                self._idx_key = idx_key
                self._value_key = value_key
                self._groups = {}

            def _value_to_str(self, value):
                import numpy as np

                if isinstance(value, np.ndarray):
                    if value.size == 1:
                        value = (
                            int(value)
                            if np.issubdtype(value.dtype, np.integer)
                            else float(value)
                        )
                    else:
                        value = tuple(value)
                return (
                    str(value)
                    .replace(",", "‚")
                    .replace(" ", "·")
                    .replace("/", "∕")
                    .replace("\\", "∖")
                    .replace("<", "‹")
                    .replace(">", "›")
                    .replace(":", "⁚")
                    .replace('"', "″")
                    .replace("|", "¦")
                    .replace("?", "⁇")
                    .replace("*", "⁎")
                )

            def __call__(self, x: Sample):
                value = x.deep_get(self._value_key)
                if value is not None:
                    value = self._value_to_str(value)
                    self._groups.setdefault(value, []).append(
                        int(x[self._idx_key]())  # type: ignore
                    )

        reader = self.input.create_reader()

        unique_idx_key = uuid.uuid1().hex
        worker = WorkerHelper(unique_idx_key, self.key)
        self.grabber.grab_all(
            reader.enumerate(idx_key=unique_idx_key),
            keep_order=True,
            parent_cmd=self,
            track_message="Gathering unique values",
            sample_fn=worker,
        )

        for idx, (group_val, group_idxs) in enumerate(worker._groups.items()):
            split_name = f"{self.key}={group_val}"
            split_output = self.output.copy(
                update={"folder": self.output.folder / split_name}
            )

            split_name = f"{split_name} " if len(split_name) < 20 else ""
            split_seq = split_output.append_writer(reader.select(group_idxs))
            self.grabber.grab_all(
                split_seq,
                grab_context_manager=split_output.serialization_cm(),
                keep_order=False,
                parent_cmd=self,
                track_message=(
                    f"Writing split {idx + 1}/{len(worker._groups)} "
                    f"{split_name}({len(split_seq)} samples)"
                ),
            )
