from __future__ import annotations
from pipelime.sequences import SamplesSequence
import typing as t


class DirectAccessSequence(t.Sequence[t.Mapping[str, t.Any]]):
    def __init__(self, sequence: SamplesSequence):
        self._sequence = sequence

    def __len__(self) -> int:
        return len(self._sequence)

    def __getitem__(self, idx: t.Union[int, slice]) -> t.Mapping[str, t.Any]:
        return (
            DirectAccessSequence(self._sequence[idx])  # type: ignore
            if isinstance(idx, slice)
            else self._sequence[idx].to_dict()
        )

    def __add__(
        self, other: t.Union[DirectAccessSequence, SamplesSequence]
    ) -> DirectAccessSequence:
        if isinstance(other, DirectAccessSequence):
            return DirectAccessSequence(
                self._sequence.cat(other._sequence)  # type: ignore
            )
        return DirectAccessSequence(self._sequence.cat(other))  # type: ignore
