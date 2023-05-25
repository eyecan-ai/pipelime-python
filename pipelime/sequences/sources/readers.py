import os
import typing as t
from pathlib import Path

from loguru import logger
from pydantic import Field, PrivateAttr, validator

from pipelime.sequences import Sample, SamplesSequence, source_sequence


@source_sequence
class UnderfolderReader(SamplesSequence, title="from_underfolder"):
    """A SamplesSequence loading data from an Underfolder dataset."""

    folder: Path = Field(..., description="The root folder of the Underfolder dataset.")
    merge_root_items: bool = Field(
        True,
        description=(
            "Adds root items as shared items "
            "to each sample (sample values take precedence)."
        ),
    )
    must_exist: bool = Field(
        True, description="If True raises an error when `folder` does not exist."
    )
    watch: bool = Field(
        False,
        description=(
            "If True, the dataset is scanned every time a new Sample is requested."
        ),
    )

    _samples: t.List[t.Union[Sample, t.Dict[str, str]]] = PrivateAttr(
        default_factory=list
    )
    _root_sample: t.Optional[t.Union[Sample, t.Dict[str, str]]] = PrivateAttr(None)

    @validator("must_exist", always=True)
    def check_folder_exists(cls, v, values):
        if v:
            root_folder = values["folder"]
            if not root_folder.exists() or not root_folder.is_dir():
                raise ValueError(f"Root folder {root_folder} does not exist.")

            data_folder = cls.data_path(root_folder)
            if not data_folder.exists() or not data_folder.is_dir():
                raise ValueError(f"Data folder {data_folder} does not exist.")
        return v

    @classmethod
    def data_path(cls, root_folder: Path) -> Path:
        return root_folder / "data"

    def __init__(self, folder: Path, **data):
        super().__init__(folder=folder, **data)  # type: ignore

        if not self.watch:
            self._scan_root_files()
            self._scan_sample_files()

    @property
    def data_folder(self) -> Path:
        return self.data_path(self.folder)

    @property
    def root_sample(self) -> Sample:
        from pipelime.items import Item

        # user may change the value of `self.watch` at any time,
        # so both checks are necessary
        if self.watch or self._root_sample is None:
            self._scan_root_files()
        if not isinstance(self._root_sample, Sample):
            self._root_sample = Sample(
                {
                    k: Item.get_instance(v, shared_item=True)
                    for k, v in self._root_sample.items()  # type: ignore
                }
            )
        return self._root_sample

    def _extract_key(self, name: str) -> str:
        return name.partition(".")[0]

    def _extract_id_key(self, name: str) -> t.Optional[t.Tuple[int, str]]:
        id_key_split = name.partition("_")
        if not id_key_split[2]:  # pragma: no cover
            logger.warning(
                f"{self.__class__}: cannot parse file name {name} as <id>_<key>.<ext>"
            )
            return None
        try:
            return (int(id_key_split[0]), self._extract_key(id_key_split[2]))
        except ValueError:  # pragma: no cover
            logger.warning(
                f"{self.__class__}: file name `{name}` does not start with an integer"
            )
            return None

    def _scan_root_files(self):
        if self.folder.exists():
            root_items: t.Dict[str, str] = {}
            with os.scandir(str(self.folder)) as it:
                for entry in it:
                    if entry.is_file():
                        key = self._extract_key(entry.name)
                        if key:
                            root_items[key] = entry.path
            self._root_sample = root_items if root_items else Sample()
        else:  # pragma: no cover
            logger.warning(
                f"{self.__class__}: root folder `{self.folder}` does not exist"
            )
            self._root_sample = Sample()

    def _scan_sample_files(self):
        data_folder = self.data_folder
        if data_folder.exists():
            samples = []
            with os.scandir(str(data_folder)) as it:
                for entry in it:
                    if entry.is_file():
                        id_key = self._extract_id_key(entry.name)
                        if id_key:
                            samples.extend(
                                ({} for _ in range(id_key[0] - len(samples) + 1))
                            )
                            samples[id_key[0]][id_key[1]] = entry.path
            self._samples = samples
        else:  # pragma: no cover
            logger.warning(
                f"{self.__class__}: data folder `{data_folder}` does not exist"
            )
            self._samples = []

    def size(self) -> int:
        if self.watch:
            self._scan_sample_files()

        return len(self._samples)

    def get_sample(self, idx: int) -> Sample:
        from pipelime.items import Item

        if self.watch:
            self._scan_sample_files()

        sample = self._samples[idx]
        if not isinstance(sample, Sample):
            sample = Sample(
                {k: Item.get_instance(v, shared_item=False) for k, v in sample.items()}
            )
            if self.merge_root_items:
                sample = self.root_sample.merge(sample)
            self._samples[idx] = sample
        return sample


@source_sequence
class SequenceFromImageFolders(SamplesSequence, title="from_images"):
    """Recursively scan a folder tree and load all the images as samples."""

    folder: Path = Field(
        ..., description="The root folder in which to scan for images."
    )
    must_exist: bool = Field(
        True, description="If True raises an error when `folder` does not exist."
    )
    image_key: str = Field("image", description="The key of the image item.")
    sort_files: bool = Field(
        False, description="If True, read the files in sorted order."
    )
    recursive: bool = Field(True, description="If True, scan the `folder` recursively.")

    _samples: t.List[t.Union[str, Sample]] = PrivateAttr(default_factory=list)

    @validator("must_exist", always=True)
    def check_folder_exists(cls, v, values):
        p = values["folder"]
        if v and not p.exists():
            raise ValueError(f"Root folder {p} does not exist.")
        return v

    def __init__(self, folder: Path, **data):
        from pipelime.items import Item, ImageItem

        super().__init__(folder=folder, **data)  # type: ignore
        self._samples = []
        self._scan_folder(
            self.folder.as_posix(),
            # grab all the extensions of the ImageItem subclasses
            {
                ext
                for ext, item_cls in Item.ITEM_CLASSES.items()
                if issubclass(item_cls, ImageItem)
            },
        )

    def _scan_folder(self, folder: str, extensions: t.Container[str]):
        if not self.folder.exists():
            logger.warning(f"{self.__class__}: folder `{folder}` does not exist")
            return

        # NB: os.scandir is faster than pathlib
        with os.scandir(folder) as it:
            if self.sort_files:
                it = sorted(it, key=lambda entry: entry.name)
            for entry in it:
                if (
                    entry.is_file()
                    and os.path.splitext(entry.name)[1].lower() in extensions
                ):
                    self._samples.append(entry.path)
                elif entry.is_dir() and self.recursive:
                    self._scan_folder(entry.path, extensions)

    def size(self) -> int:
        return len(self._samples)

    def get_sample(self, idx: int) -> Sample:
        from pipelime.items import Item

        sample = self._samples[idx]
        if not isinstance(sample, Sample):
            image_item = Item.get_instance(sample, shared_item=False)  # type: ignore
            sample = Sample({self.image_key: image_item})
            self._samples[idx] = sample
        return sample
