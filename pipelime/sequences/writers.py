from pathlib import Path
from filelock import FileLock, Timeout
import typing as t

import pipelime.sequences.base as pls
from pipelime.sequences.proxies import ProxySequenceBase
from pipelime.items import SerializationMode, Item


class _serialization_mode_override:
    def __init__(self, item: Item, mode: t.Optional[SerializationMode]):
        self._item = item
        self._mode = mode

    def __enter__(self):
        if self._mode is not None:
            self._prev_mode = self._item.serialization_mode
            self._item.serialization_mode = self._mode

    def __exit__(self, exc_type, exc_value, traceback):
        if self._mode is not None:
            self._item.serialization_mode = self._prev_mode


@pls.as_samples_sequence_functional("to_underfolder")
class UnderfolderWriter(ProxySequenceBase):
    """Writes samples to an underfolder dataset."""

    def __init__(
        self,
        source: pls.SamplesSequence,
        folder: t.Union[str, Path],
        zfill: t.Optional[int] = None,
        key_serialization_mode: t.Optional[t.Mapping[str, SerializationMode]] = None,
        exists_ok: bool = False,
    ):
        super().__init__(source)
        self._root_folder = Path(folder)
        self._data_folder = self._root_folder / "data"
        self._zfill = source.best_zfill() if zfill is None else zfill
        self._key_serialization_mode = (
            {} if key_serialization_mode is None else key_serialization_mode
        )

        if not exists_ok and (self._root_folder.exists() or self._data_folder.exists()):
            raise FileExistsError("Trying to overwrite an existing dataset.")

        self._data_folder.mkdir(parents=True, exist_ok=True)

    def get_sample(self, idx: int) -> pls.Sample:
        sample = self._source[idx]

        id_str = str(idx)
        if self._zfill is not None:
            id_str = id_str.zfill(self._zfill)

        for k, v in sample.items():
            with _serialization_mode_override(v, self._key_serialization_mode.get(k)):
                if v.is_shared:
                    filepath = self._root_folder / k
                    if not any(f.exists() for f in v.get_all_names(filepath)):
                        lock = FileLock(str(filepath.with_suffix("lock")))
                        try:
                            with lock.acquire(timeout=1):
                                v.serialize(filepath)
                        except Timeout:
                            pass
                else:
                    v.serialize(self._data_folder / f"{id_str}_{k}")

        return sample
