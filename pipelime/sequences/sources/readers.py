import os
from pathlib import Path
import typing as t
import pydantic as pyd

import pipelime.sequences as pls
from pipelime.items.base import ItemFactory, Item


@pls.source_sequence("from_underfolder")
class UnderfolderReader(pls.SamplesSequence):
    """A SamplesSequence loading data from an Underfolder dataset. Usage::

    sseq = SamplesSequence.from_underfolder(folder)
    """

    folder: Path = pyd.Field(
        ..., description="The root folder of the Underfolder dataset."
    )
    merge_root_items: bool = pyd.Field(
        True,
        description=(
            "Adds root items as shared items "
            "to each sample (sample values take precedence)."
        ),
    )
    must_exist: bool = pyd.Field(
        True, description="If True raises an error when `folder` does not exist."
    )

    _samples: t.Sequence[pls.Sample] = []
    _root_sample: pls.Sample = pls.Sample(None)

    class Config:
        underscore_attrs_are_private = True

    @pyd.validator("must_exist")
    def check_folder_exists(cls, v, values, **kwargs):
        p = values["folder"]
        if v and not p.exists():
            raise ValueError(f"Root folder {p} does not exist.")
        return v

    def __init__(self, folder, **data):
        super().__init__(folder=folder, **data)  # type: ignore

        if self.folder.exists():
            # root files
            root_items: t.Dict[str, Item] = {}
            with os.scandir(str(self.folder)) as it:
                for entry in it:
                    if entry.is_file():
                        key = self._extract_key(entry.name)
                        if key:
                            root_items[key] = ItemFactory.get_instance(
                                entry.path, shared_item=True
                            )
            self._root_sample = pls.Sample(root_items)

            # samples
            data_folder = self.folder / "data"
            if data_folder.exists():
                sample_items: t.Dict[str, t.Dict[str, Item]] = {}
                with os.scandir(str(data_folder)) as it:
                    for entry in it:
                        if entry.is_file():
                            id_key = self._extract_id_key(entry.name)
                            if id_key:
                                item_map = sample_items.setdefault(id_key[0], {})
                                item_map[id_key[1]] = ItemFactory.get_instance(
                                    entry.path
                                )

                self._samples = [
                    pls.Sample(item_map) for _, item_map in sorted(sample_items.items())
                ]

    @property
    def root_sample(self) -> pls.Sample:
        return self._root_sample

    @property
    def samples(self) -> t.Sequence[pls.Sample]:
        return self._samples

    def _extract_key(self, name: str) -> str:
        return name.partition(".")[0]

    def _extract_id_key(self, name: str) -> t.Optional[t.Tuple[str, str]]:
        id_key_split = name.partition("_")
        if not id_key_split[2]:
            return None  # pragma: no cover
        try:
            return (id_key_split[0], self._extract_key(id_key_split[2]))
        except ValueError:  # pragma: no cover
            return None

    def size(self) -> int:
        return len(self._samples)

    def get_sample(self, idx: int) -> pls.Sample:
        sample = self._samples[idx]
        return self._root_sample.merge(sample) if self.merge_root_items else sample
