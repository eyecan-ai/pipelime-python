from pathlib import Path
import os
import typing as t

import pipelime.sequences.base as pls
from pipelime.items.base import ItemFactory, Item


@pls.as_samples_sequence_functional("from_underfolder", is_static=True)
class UnderfolderReader(pls.SamplesSequence):
    """A SamplesSequence from an Underfolder dataset. Usage::

        sseq = UnderfolderReader(folder)
        # or, equivalently,
        sseq = SamplesSequence.from_underfolder(folder)

    :param folder: the root folder of the Underfolder dataset.
    :type folder: t.Union[str, Path]
    :param copy_root_files: propagates root files to samples, defaults to True
    :type copy_root_files: bool, optional
    """

    def __init__(self, folder: t.Union[str, Path], copy_root_files: bool = True):
        super().__init__()
        folder = Path(folder)
        self._copy_root_files = copy_root_files
        self._samples: t.Sequence[pls.Sample] = []
        self._root_sample = pls.Sample(None)

        if folder.exists():
            # root files
            root_items: t.Dict[str, Item] = {}
            with os.scandir(str(folder)) as it:
                for entry in it:
                    if entry.is_file():
                        key = self._extract_key(entry.name)
                        if key:
                            root_items[key] = ItemFactory.get_instance(
                                entry.path, shared_item=True
                            )
            self._root_sample = pls.Sample(root_items)

            # samples
            folder = folder / "data"
            if folder.exists():
                sample_items: t.Dict[str, t.Dict[str, Item]] = {}
                with os.scandir(str(folder)) as it:
                    for entry in it:
                        if entry.is_file():
                            id_key = self._extract_id_key(entry.name)
                            if id_key:
                                item_map = sample_items.setdefault(id_key[0], {})
                                item_map[id_key[1]] = ItemFactory.get_instance(
                                    entry.path
                                )

                self._samples = [
                    pls.Sample(item_map)
                    for _, item_map in sorted(sample_items.items())
                ]

    @property
    def root_sample(self) -> pls.Sample:
        return self._root_sample

    @property
    def samples(self) -> t.Sequence[pls.Sample]:
        return self._samples

    def _extract_key(self, name: str) -> str:
        return name.partition(".")[0]

    def _extract_id_key(self, name: str) -> t.Optional[t.Tuple[int, str]]:
        id_key_split = name.partition("_")
        if not id_key_split[2]:
            return None
        try:
            return (id_key_split[0], self._extract_key(id_key_split[2]))
        except ValueError:
            return None

    def size(self) -> int:
        return len(self._samples)

    def get_sample(self, idx: int) -> pls.Sample:
        sample = self._samples[idx]
        return self._root_sample.merge(sample) if self._copy_root_files else sample
