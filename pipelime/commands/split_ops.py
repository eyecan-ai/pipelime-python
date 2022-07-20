import typing as t

import pydantic as pyd

import pipelime.commands.interfaces as pl_interfaces
from pipelime.piper import PipelimeCommand, PiperPortType


class SplitBase(pyd.BaseModel, allow_population_by_field_name=True, extra="forbid"):
    output: t.Optional[
        pl_interfaces.OutputDatasetInterface
    ] = pl_interfaces.OutputDatasetInterface.pyd_field(
        alias="o",
        is_required=False,
        description="Output split. Leave to None to not save to disk.",
    )
    _output_validator = pl_interfaces.OutputDatasetInterface.pyd_validator("output")

    def __repr__(self) -> str:
        return self.__piper_repr__()

    def __piper_repr__(self) -> str:
        return "" if self.output is None else str(self.output.folder)


class PercSplit(SplitBase):
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


class AbsoluteSplit(SplitBase):
    length: t.Optional[pyd.PositiveInt] = pyd.Field(
        ...,
        description=(
            "Number of elements to keep. "
            "A None value takes all the samples left by the other splits."
        ),
    )

    def split_size(self, *args, **kwargs) -> t.Optional[int]:
        return self.length


class SplitCommand(PipelimeCommand, title="split"):
    """Split a dataset."""

    input: pl_interfaces.InputDatasetInterface = (
        pl_interfaces.InputDatasetInterface.pyd_field(
            alias="i", piper_port=PiperPortType.INPUT
        )
    )
    _input_validator = pl_interfaces.InputDatasetInterface.pyd_validator("input")

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

    splits: t.Union[
        PercSplit, AbsoluteSplit, t.Sequence[t.Union[PercSplit, AbsoluteSplit]]
    ] = pyd.Field(
        ...,
        alias="s",
        description="Splits definition.\nCompact form: `<fraction|length>[,<folder>]`",
        piper_port=PiperPortType.OUTPUT,
    )

    @pyd.root_validator(pre=True)
    def _validate_splits(cls, values):
        def _create_fn(val):
            if isinstance(val, (str, bytes, int, float)):
                data = {}
                if isinstance(val, int):
                    data["length"] = val
                elif isinstance(val, float):
                    data["fraction"] = val
                else:
                    sz, _, fld = str(val).partition(",")
                    try:
                        data["length"] = int(sz)
                    except ValueError:
                        try:
                            data["fraction"] = float(sz)
                        except ValueError:
                            # NB: leave size as-is if not valid, it will raise an error
                            data["length"] = (
                                None if sz.lower() in ("none", "null", "nul") else sz
                            )

                    if fld:
                        data["output"] = pl_interfaces.OutputDatasetInterface(
                            folder=fld  # type: ignore
                        )
                return (
                    PercSplit(**data) if "fraction" in data else AbsoluteSplit(**data)
                )
            return val

        return pl_interfaces._iter_field(cls, values, "splits", _create_fn)

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )
    _grabber_validator = pl_interfaces.GrabberInterface.pyd_validator("grabber")

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
                with split.output.serialization_cm():
                    self.grabber.grab_all(
                        seq,
                        keep_order=False,
                        parent_cmd=self,
                        track_message=(
                            f"Writing split {idx + 1}/{len(split_sizes)} "
                            f"({split_length} samples)"
                        ),
                    )
            split_start = split_stop


class SplitByQueryCommand(PipelimeCommand, title="split-query"):
    """Split a dataset by query."""

    input: pl_interfaces.InputDatasetInterface = (
        pl_interfaces.InputDatasetInterface.pyd_field(
            alias="i", piper_port=PiperPortType.INPUT
        )
    )
    _input_validator = pl_interfaces.InputDatasetInterface.pyd_validator("input")

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
    _output_selected_validator = pl_interfaces.OutputDatasetInterface.pyd_validator(
        "output_selected"
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
    _output_discarded_validator = pl_interfaces.OutputDatasetInterface.pyd_validator(
        "output_discarded"
    )

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )
    _grabber_validator = pl_interfaces.GrabberInterface.pyd_validator("grabber")

    def run(self):
        reader = self.input.create_reader()
        if self.output_selected is not None:
            self._filter(
                reader,
                self.output_selected,
                lambda x: x.match(self.query),
                "selected samples",
            )
        if self.output_discarded is not None:
            self._filter(
                reader,
                self.output_selected,
                lambda x: not x.match(self.query),
                "discarded samples",
            )

    def _filter(self, reader, output, fn, message):
        seq = reader.filter(fn)
        seq = output.append_writer(seq)

        with output.serialization_cm():
            self.grabber.grab_all(
                seq,
                keep_order=False,
                parent_cmd=self,
                track_message=f"Writing {message} ({len(seq)} samples)",
            )


class SplitByValueCommand(PipelimeCommand, title="split-value"):
    """Split a dataset into multiple sequences,
    one for each unique value of a given key."""

    input: pl_interfaces.InputDatasetInterface = (
        pl_interfaces.InputDatasetInterface.pyd_field(
            alias="i", piper_port=PiperPortType.INPUT
        )
    )
    _input_validator = pl_interfaces.InputDatasetInterface.pyd_validator("input")

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
    _output_validator = pl_interfaces.OutputDatasetInterface.pyd_validator("output")

    grabber: pl_interfaces.GrabberInterface = pl_interfaces.GrabberInterface.pyd_field(
        alias="g"
    )
    _grabber_validator = pl_interfaces.GrabberInterface.pyd_validator("grabber")

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
                    .replace(", ", "„")
                    .replace(",", "„")
                    .replace(" ", "_")
                    .replace("/", "-")
                    .replace("\\", "-")
                    .replace("<", "‹")
                    .replace(">", "›")
                    .replace(":", "⁚")
                    .replace('"', "'")
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
            with split_output.serialization_cm():
                self.grabber.grab_all(
                    split_seq,
                    keep_order=False,
                    parent_cmd=self,
                    track_message=(
                        f"Writing split {idx + 1}/{len(worker._groups)} "
                        f"{split_name}({len(split_seq)} samples)"
                    ),
                )
