import typing as t
import pydantic as pyd

import pipelime.commands.interfaces as pl_interfaces
from pipelime.piper import PipelimeCommand, PiperPortType


class SplitBase(pyd.BaseModel, extra="forbid"):
    output: t.Optional[pl_interfaces.OutputDatasetInterface] = pyd.Field(
        None, description="Output split. Set to None to not save to disk."
    )

    def __str__(self) -> str:
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

    input: pl_interfaces.InputDatasetInterface = pyd.Field(
        ..., description="Input dataset.", piper_port=PiperPortType.INPUT
    )
    splits: t.Sequence[t.Union[PercSplit, AbsoluteSplit]] = pyd.Field(
        ..., description="Splits definition.", piper_port=PiperPortType.OUTPUT
    )
    grabber: pl_interfaces.GrabberInterface = pyd.Field(
        default_factory=pl_interfaces.GrabberInterface,  # type: ignore
        description="Grabber options.",
    )

    def run(self):
        reader = self.input.create_reader()
        input_length = len(reader)

        split_sizes = [s.split_size(input_length) for s in self.splits]
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
                "The sum of the split sizes is greater than the input length."
            )
        if none_idx >= 0:
            split_sizes[none_idx] = input_length - split_total

        split_start = 0
        for idx, split_length, split in zip(
            range(len(split_sizes)), split_sizes, self.splits
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
                            f"Writing split {idx}/{len(split_sizes)} "
                            "({split_length} samples)..."
                        ),
                    )
            split_start = split_stop
