import re
import typing as t
from pathlib import Path

import pydantic.v1 as pyd
from filelock import FileLock, Timeout

import pipelime.sequences as pls
from pipelime.items import Item, SerializationMode
from pipelime.sequences.pipes import PipedSequenceBase


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


@pls.piped_sequence
class UnderfolderWriter(
    PipedSequenceBase, title="to_underfolder", underscore_attrs_are_private=True
):
    """Writes samples to an underfolder dataset while iterating over them."""

    folder: Path = pyd.Field(..., description="The output folder.")
    zfill: t.Optional[pyd.NonNegativeInt] = pyd.Field(
        None, description="Custom index zero-filling."
    )
    key_serialization_mode: t.Optional[
        t.Mapping[str, t.Union[SerializationMode, str]]
    ] = pyd.Field(None, description="Forced serialization mode for each key.")
    exists_ok: bool = pyd.Field(
        False, description="If False raises an error when `folder` exists."
    )

    _data_folder: Path
    _effective_zfill: int
    _temp_folder: Path

    @pyd.validator("exists_ok", always=True)
    def _check_folder_exists(cls, v: bool, values: t.Mapping[str, t.Any]) -> bool:
        if not v and "folder" in values and values["folder"].exists():
            raise ValueError(
                f"Trying to overwrite an existing dataset: `{values['folder']}`. "
                "Please use `exists_ok=True` to overwrite."
            )
        return v

    def __init__(self, folder: Path, **data):
        from pipelime.choixe.utils.io import PipelimeTmp

        super().__init__(folder=folder, **data)  # type: ignore

        self._data_folder = self.folder / "data"
        self._effective_zfill = (
            self.source.best_zfill() if self.zfill is None else self.zfill
        )
        if self.key_serialization_mode is None:
            self.key_serialization_mode = {}

        self._data_folder.mkdir(parents=True, exist_ok=True)

        # will be automatically deleted
        self._temp_folder = PipelimeTmp.make_subdir()

    def get_sample(self, idx: int) -> pls.Sample:
        sample = self.source[idx]

        id_str_nofill = str(idx)
        id_str = id_str_nofill.zfill(self._effective_zfill)

        for k, v in sample.items():
            with _serialization_mode_override(
                v, self.key_serialization_mode.get(k)  # type: ignore
            ):
                if v.is_shared:
                    filepath = self.folder / k
                    if not any(f.exists() for f in v.get_all_names(filepath)):
                        lock = FileLock(str(Path(self._temp_folder / (k + ".~lock"))))
                        try:
                            with lock.acquire(timeout=1):
                                # check again to avoid races
                                if not any(  # pragma: no branch
                                    f.exists() for f in v.get_all_names(filepath)
                                ):
                                    v.serialize(filepath)
                        except Timeout:  # pragma: no cover
                            pass
                else:
                    # when overwriting, check for existing items with the same name
                    if self.exists_ok:
                        if self._check_existing_items(v, id_str_nofill, k):
                            continue
                    v.serialize(self._data_folder / f"{id_str}_{k}")

        return sample

    def _check_existing_items(self, item: Item, id_nofill: str, key: str):
        local_srcs = item.local_sources
        skip_serialization = False
        x = re.compile(r"^(0)*{}_{}\.[a-zA-Z]+$".format(id_nofill, key))
        for p in self._data_folder.glob(f"*{id_nofill}_{key}.*"):
            if x.fullmatch(p.name):
                p = p.resolve().absolute()
                if p in local_srcs:
                    skip_serialization = True
                else:
                    p.unlink()
        return skip_serialization
