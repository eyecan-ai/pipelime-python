from pipelime.sequences import SamplesSequence
import typing as t
from torch.utils.data import Dataset  # type: ignore


class TorchDataset(Dataset[t.Mapping[str, t.Any]]):
    def __init__(self, sequence: SamplesSequence):
        self._sequence = sequence

    def __len__(self) -> int:
        return len(self._sequence)

    def __getitem__(self, idx: t.Union[int, slice]) -> t.Mapping[str, t.Any]:
        return self._sequence[idx].to_dict()
