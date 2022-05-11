from pathlib import Path
from filelock import FileLock, Timeout
import typing as t
import pydantic as pyd

import pipelime.sequences as pls
from pipelime.sequences.pipes.base import PipedSequenceBase
from pipelime.items import SerializationMode, Item


class _serialization_mode_override:
    """Changes the serialization mode of a specific item."""

    def __init__(self, item: Item, mode: t.Optional[t.Union[SerializationMode, str]]):
        self._item = item
        self._mode = mode

    def __enter__(self):
        if self._mode is not None:
            self._prev_mode = self._item.serialization_mode
            self._item.serialization_mode = self._mode

    def __exit__(self, exc_type, exc_value, traceback):
        if self._mode is not None:
            self._item.serialization_mode = self._prev_mode


@pls.piped_sequence("to_underfolder")
class UnderfolderWriter(PipedSequenceBase):
    """Writes samples to an underfolder dataset while iterating over them. Usage::

        sseq = sseq.to_underfolder("out_path")

    :raises FileExistsError: if `exists_ok` is False and `folder` exists.
    """

    folder: Path = pyd.Field(..., description="The output folder.")
    zfill: t.Optional[int] = pyd.Field(None, description="Custom index zero-filling.")
    key_serialization_mode: t.Optional[
        t.Mapping[str, t.Union[SerializationMode, str]]
    ] = pyd.Field(None, description="Forced serialization mode for each key.")
    exists_ok: bool = pyd.Field(
        False, description="If False raises an error when `folder` exists."
    )

    _data_folder: Path
    _effective_zfill: int

    class Config:
        underscore_attrs_are_private = True

    def __init__(self, folder: Path, **data):
        super().__init__(folder=folder, **data)  # type: ignore

        self._data_folder = self.folder / "data"
        self._effective_zfill = (
            self.source.best_zfill() if self.zfill is None else self.zfill
        )
        if self.key_serialization_mode is None:
            self.key_serialization_mode = {}

        if not self.exists_ok and (self.folder.exists() or self._data_folder.exists()):
            raise FileExistsError("Trying to overwrite an existing dataset.")

        self._data_folder.mkdir(parents=True, exist_ok=True)

    def get_sample(self, idx: int) -> pls.Sample:
        sample = self.source[idx]

        id_str = str(idx)
        id_str = id_str.zfill(self._effective_zfill)

        for k, v in sample.items():
            with _serialization_mode_override(
                v, self.key_serialization_mode.get(k)  # type: ignore
            ):
                if v.is_shared:
                    filepath = self.folder / k
                    if not any(f.exists() for f in v.get_all_names(filepath)):
                        lock_filepath = filepath.with_suffix(".~lock")
                        lock = FileLock(str(lock_filepath))

                        delete_lockfile = False
                        try:
                            with lock.acquire(timeout=1):
                                # check again to avoid races
                                if not any(
                                    f.exists() for f in v.get_all_names(filepath)
                                ):
                                    v.serialize(filepath)
                                    # on Unix we must manually delete the lock file
                                    # NB: only the thread/process which has serialized
                                    # the file should delete the the lock file, ie, the
                                    # link to the inode, while other threads/processes
                                    # may still have a valid reference to the inode
                                    # itself.
                                    delete_lockfile = True
                        except Timeout:  # pragma: no cover
                            pass

                        if delete_lockfile:
                            lock_filepath.unlink(missing_ok=True)
                else:
                    v.serialize(self._data_folder / f"{id_str}_{k}")

        return sample
