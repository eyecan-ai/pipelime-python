import os
import typing as t
from pathlib import Path

import pydantic as pyd

import pipelime.sequences as pls


@pls.source_sequence
class UnderfolderReader(pls.SamplesSequence, title="from_underfolder"):
    """A SamplesSequence loading data from an Underfolder dataset."""

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
    watch: bool = pyd.Field(
        False,
        description=(
            "If True, the dataset is scanned every time a new Sample is requested."
        ),
    )

    _samples: t.List[t.Union[pls.Sample, t.Dict[str, Path]]] = pyd.PrivateAttr(
        default_factory=list
    )
    _root_sample: t.Optional[t.Union[pls.Sample, t.Dict[str, Path]]] = pyd.PrivateAttr(
        None
    )

    @pyd.validator("must_exist", always=True)
    def check_folder_exists(cls, v, values, **kwargs):
        p = values["folder"]
        if v and not p.exists():
            raise ValueError(f"Root folder {p} does not exist.")
        return v

    def __init__(self, folder: Path, **data):
        super().__init__(folder=folder, **data)  # type: ignore

        if not self.watch:
            self._scan_root_files()

        # load samples even if we are `watch`ing, so that we know the initial length
        self._scan_sample_files()

    @property
    def root_sample(self) -> pls.Sample:
        from pipelime.items.base import ItemFactory

        # user may change the value of `self.watch` at any time,
        # so both checks are necessary
        if self.watch or self._root_sample is None:
            self._scan_root_files()
        if not isinstance(self._root_sample, pls.Sample):
            self._root_sample = pls.Sample(
                {
                    k: ItemFactory.get_instance(v, shared_item=True)
                    for k, v in self._root_sample.items()  # type: ignore
                }
            )
        return self._root_sample

    def _extract_key(self, name: str) -> str:
        return name.partition(".")[0]

    def _extract_id_key(self, name: str) -> t.Optional[t.Tuple[int, str]]:
        id_key_split = name.partition("_")
        if not id_key_split[2]:
            return None  # pragma: no cover
        try:
            return (int(id_key_split[0]), self._extract_key(id_key_split[2]))
        except ValueError:  # pragma: no cover
            return None

    def _scan_root_files(self):
        if self.folder.exists():
            root_items: t.Dict[str, Path] = {}
            with os.scandir(str(self.folder)) as it:
                for entry in it:
                    if entry.is_file():
                        key = self._extract_key(entry.name)
                        if key:
                            root_items[key] = entry.path
            self._root_sample = root_items if root_items else pls.Sample()
        else:
            self._root_sample = pls.Sample()

    def _scan_sample_files(self):
        data_folder = self.folder / "data"
        if data_folder.exists():
            self._samples = []
            with os.scandir(str(data_folder)) as it:
                for entry in it:
                    if entry.is_file():
                        id_key = self._extract_id_key(entry.name)
                        if id_key:
                            self._samples.extend(
                                [{} for _ in range(id_key[0] - len(self._samples) + 1)]
                            )
                            self._samples[id_key[0]][id_key[1]] = (  # type: ignore
                                entry.path
                            )

    def size(self) -> int:
        return len(self._samples)

    def get_sample(self, idx: int) -> pls.Sample:
        from pipelime.items.base import ItemFactory

        if self.watch:
            self._scan_sample_files()

        sample = self._samples[idx]
        if not isinstance(sample, pls.Sample):
            sample = pls.Sample(
                {
                    k: ItemFactory.get_instance(v, shared_item=False)
                    for k, v in sample.items()
                }
            )
            self._samples[idx] = sample
        return self.root_sample.merge(sample) if self.merge_root_items else sample
