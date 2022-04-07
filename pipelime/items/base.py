from abc import ABCMeta, abstractmethod
from pathlib import Path
from urllib.parse import ParseResult, urlparse
from enum import Enum
from contextlib import ContextDecorator
from loguru import logger
import io
import shutil
import os

import typing as t

import pipelime.remotes as plr


class SerializationMode(Enum):
    CREATE_NEW_FILE = "newfile"
    DEEP_COPY = "deepcopy"
    SYM_LINK = "symlink"
    HARD_LINK = "hardlink"
    REMOTE_FILE = "remote"


class ItemFactory(ABCMeta):
    """Item classes register themselves in this factory, so that they can be
    instantiated on request when loading file from disk. To store user-created data a
    specific Item should be explicitly created.
    """

    ITEM_CLASSES: t.Dict[str, t.Type["Item"]] = {}
    REMOTE_FILE_EXT = ".remote"
    ITEM_DATA_CACHE_MODE: t.Dict[t.Type["Item"], bool] = {}
    ITEM_SERIALIZATION_MODE: t.Dict[t.Type["Item"], SerializationMode] = {}

    def __init__(cls, name, bases, dct):
        """Registers item class extensions."""
        for ext in cls.file_extensions():  # type: ignore
            if ext != cls.REMOTE_FILE_EXT:
                cls.ITEM_CLASSES[ext] = cls  # type: ignore
        cls.ITEM_DATA_CACHE_MODE[cls] = True  # type: ignore
        cls.ITEM_SERIALIZATION_MODE[cls] = SerializationMode.DEEP_COPY  # type: ignore
        super().__init__(name, bases, dct)

    @classmethod
    def get_instance(
        cls, filepath: t.Union[Path, str], shared_item: bool = False
    ) -> "Item":
        """Returns the item that can handle the given file."""

        # make sure all Items have been properly registered
        import pipelime.items  # noqa

        filepath = Path(filepath)
        path_or_urls = filepath
        ext = filepath.suffix
        if ext == cls.REMOTE_FILE_EXT:
            with filepath.open("r") as fp:
                url_list = [urlparse(line) for line in fp if line]
            if not url_list:
                raise ValueError(f"The file {filepath} does not contain any remote.")
            ext = Path(url_list[0].path).suffix
            path_or_urls = url_list

        item_cls = cls.ITEM_CLASSES.get(ext, UnknownItem)
        return item_cls(path_or_urls, shared=shared_item)

    @classmethod
    def set_data_cache_mode(cls, item_cls: t.Type["Item"], enable_data_cache: bool):
        cls.ITEM_DATA_CACHE_MODE[item_cls] = enable_data_cache

    @classmethod
    def is_cache_enabled(cls, item_cls: t.Type["Item"]) -> bool:
        return cls.ITEM_DATA_CACHE_MODE[item_cls]

    @classmethod
    def set_serialization_mode(cls, item_cls: t.Type["Item"], mode: SerializationMode):
        cls.ITEM_SERIALIZATION_MODE[item_cls] = mode

    @classmethod
    def get_serialization_mode(cls, item_cls: t.Type["Item"]) -> SerializationMode:
        return cls.ITEM_SERIALIZATION_MODE[item_cls]


def set_item_serialization_mode(mode: SerializationMode):
    """Sets serialization mode for all items."""
    for itc in ItemFactory.ITEM_SERIALIZATION_MODE.keys():
        ItemFactory.set_serialization_mode(itc, mode)


def disable_item_data_cache(*item_cls: t.Type["Item"]):
    """Disables data cache on selected item classes."""
    for itc in item_cls:
        ItemFactory.set_data_cache_mode(itc, False)


def enable_item_data_cache(*item_cls: t.Type["Item"]):
    """Enables data cache on selected item classes."""
    for itc in item_cls:
        ItemFactory.set_data_cache_mode(itc, True)


class no_data_cache(ContextDecorator):
    """Use this class as context manager or function decorator to disable data caching
    on some or all item types.

    .. code-block::
    :caption: Example

        # disable data cache for all items
        with no_data_cache():
            ...

        # disable only for ImageItem and NumpyItem
        with no_data_cache(ImageItem, NumpyItem):
            ...

        # apply to function invocation
        @no_data_cache(ImageItem)
        def my_fn():
            ...
    """

    def __init__(self, *item_cls: t.Type["Item"]):
        self._items = item_cls if item_cls else ItemFactory.ITEM_DATA_CACHE_MODE.keys()

    def __enter__(self):
        self._prev_state = {
            itc: ItemFactory.is_cache_enabled(itc) for itc in self._items
        }
        disable_item_data_cache(*self._items)

    def __exit__(self, exc_type, exc_value, traceback):
        for itc, val in self._prev_state.items():
            ItemFactory.set_data_cache_mode(itc, val)


T = t.TypeVar("T")
_item_data_source = t.Union[Path, ParseResult]
_item_init_types = t.Union["Item", Path, ParseResult, t.BinaryIO, t.Any]


class Item(t.Generic[T], metaclass=ItemFactory):  # type: ignore
    """Base class for any supported Item. Concrete classes should ideally implement just
    the abstract methods, leaving `__init__` as is.
    """

    _data_cache: t.Optional[T]
    _file_sources: t.List[Path]
    _remote_sources: t.List[ParseResult]
    _cache_data: bool
    _shared: bool
    _serialization_mode: t.Optional[SerializationMode]

    def __init__(self, *sources: _item_init_types, shared: bool = False):
        super().__init__()
        self._data_cache = None
        self._file_sources = []
        self._remote_sources = []
        self._cache_data = True
        self._shared = shared
        self._serialization_mode = None

        # if not sources:
        #     logger.warning(f"{self.__class__}: no source data.")

        for src in sources:
            if isinstance(src, Item):
                src = src()

            if isinstance(src, (Path, ParseResult)):
                self._add_data_source(src)
            elif self._data_cache is not None:
                raise ValueError("Cannot set data twice.")
            elif isinstance(src, t.BinaryIO):
                self._data_cache = self.decode(src)
            else:
                self._data_cache = self.validate(src)

    @property
    def cache_data(self) -> bool:
        return self._cache_data

    @cache_data.setter
    def cache_data(self, enabled: bool):
        self._cache_data = enabled

    @property
    def serialization_mode(self) -> t.Optional[SerializationMode]:
        return self._serialization_mode

    @serialization_mode.setter
    def serialization_mode(self, mode: t.Optional[SerializationMode]):
        self._serialization_mode = mode

    @property
    def is_shared(self) -> bool:
        return self._shared

    def effective_serialization_mode(self) -> SerializationMode:
        return (
            self.serialization_mode
            if self.serialization_mode is not None
            else ItemFactory.get_serialization_mode(self.__class__)
        )

    @classmethod
    def make_new(cls, *sources: _item_init_types, shared: bool = False) -> "Item":
        return cls(*sources, shared=shared)

    @classmethod
    def as_default_name(cls, filepath: Path) -> Path:
        return filepath.with_suffix(cls.file_extensions()[0])

    @classmethod
    def as_default_remote_file(cls, filepath: Path) -> Path:
        filename = cls.as_default_name(filepath)
        return filename.parent / (filename.name + Item.REMOTE_FILE_EXT)

    @classmethod
    def get_all_names(cls, filepath: Path) -> t.List[Path]:
        names = [filepath.with_suffix(ext) for ext in cls.file_extensions()]
        names += [cls.as_default_remote_file(filepath)]
        return names

    def _check_source(self, source: _item_data_source):
        src_path = Path(source.path) if isinstance(source, ParseResult) else source
        if src_path.suffix not in self.file_extensions():
            srcstr = source.geturl() if isinstance(source, ParseResult) else str(source)
            raise ValueError(f"{self.__class__}: invalid extension for `{srcstr}`")

    def _add_data_source(self, source: _item_data_source) -> bool:
        self._check_source(source)
        if isinstance(source, Path):
            source = source.resolve()
            if source not in self._file_sources:
                self._file_sources.append(source)
                return True
        elif source not in self._remote_sources:
            self._remote_sources.append(source)
            return True
        return False

    def _serialize_to_local_file(self, path: Path) -> t.Optional[Path]:  # noqa
        def _try_copy(copy_fn: t.Callable[[str, str], None], p: str) -> bool:
            for f in self._file_sources:
                try:
                    copy_fn(str(f), p)
                    return True
                except Exception:
                    logger.exception(f"{self.__class__}: data serialization error.")
            return False

        path = path.resolve()
        smode: t.Optional[SerializationMode] = self.effective_serialization_mode()

        if smode is SerializationMode.REMOTE_FILE:
            path = self.as_default_remote_file(path)
        elif path.suffix not in self.file_extensions():
            path = self.as_default_name(path)

        # first delete the existing file, if any
        path.unlink(missing_ok=True)

        if smode is SerializationMode.REMOTE_FILE:
            try:
                with path.open("w") as fp:
                    fp.write(
                        "\n".join([rm.geturl() for rm in self._remote_sources]) + "\n"
                    )
                return None  # do not save remote file among file sources!
            except Exception:
                logger.exception(f"{self.__class__}: remote file serialization error.")

        if smode is SerializationMode.HARD_LINK:
            smode = (
                None if _try_copy(os.link, str(path)) else SerializationMode.DEEP_COPY
            )

        if smode is SerializationMode.SYM_LINK:
            smode = (
                None
                if _try_copy(os.symlink, str(path))
                else SerializationMode.DEEP_COPY
            )

        if smode is SerializationMode.DEEP_COPY:
            smode = (
                None
                if _try_copy(shutil.copy, str(path))
                else SerializationMode.CREATE_NEW_FILE
            )

        if smode is SerializationMode.CREATE_NEW_FILE:
            data = self()
            if data is not None:
                try:
                    with path.open("wb") as fp:
                        self.encode(data, fp)
                    smode = None
                except Exception:
                    logger.exception(f"{self.__class__}: data serialization error.")

        if smode is None:
            return path
        logger.warning(f"{self.__class__}: cannot serialize item data.")
        return None

    def _serialize_to_remote(self, remote_url: ParseResult) -> t.Optional[ParseResult]:
        remote, rm_paths = plr.create_remote(remote_url), plr.paths_from_url(remote_url)
        if remote is not None and rm_paths[0] is not None and rm_paths[1] is not None:
            data_stream = io.BytesIO()
            self.encode(self(), data_stream)

            data_stream.seek(0, io.SEEK_END)
            data_size = data_stream.tell()
            data_stream.seek(0, io.SEEK_SET)

            return remote.upload_stream(
                data_stream, data_size, rm_paths[0], self.file_extensions()[0]
            )

    def serialize(self, *targets: _item_data_source):
        for trg in targets:
            data_source = (
                self._serialize_to_local_file(trg)
                if isinstance(trg, Path)
                else self._serialize_to_remote(trg)
            )
            if data_source is not None:
                self._add_data_source(data_source)

    def remove_data_source(self, *sources: _item_data_source) -> "Item":
        def _normalize_source(src: _item_data_source) -> _item_data_source:
            if isinstance(src, Path):
                return src.resolve()
            src_path = Path(src.path)
            if src_path.suffix:
                src_path = src_path.parent
            return ParseResult(
                scheme=src.scheme,
                netloc=src.netloc,
                path=str(src_path),
                params="",
                query="",
                fragment="",
            )

        to_be_removed = [_normalize_source(src) for src in sources]

        new_sources: t.List[_item_init_types] = (
            [] if self._data_cache is None else [self._data_cache]
        )
        new_sources += [src for src in self._file_sources if src not in to_be_removed]
        new_sources += [
            src
            for src in self._remote_sources
            if _normalize_source(src) not in to_be_removed
        ]

        return self.make_new(new_sources, shared=self.is_shared)

    def __call__(self) -> t.Optional[T]:
        if self._data_cache is not None:
            return self._data_cache
        for fsrc in self._file_sources:
            try:
                with open(fsrc, "rb") as fp:
                    return self._decode_and_store(fp)
            except Exception:
                logger.exception(f"{self.__class__}: file source error.")
        for rmsrc in self._remote_sources:
            remote, rm_paths = plr.create_remote(rmsrc), plr.paths_from_url(rmsrc)
            if (
                remote is not None
                and rm_paths[0] is not None
                and rm_paths[1] is not None
            ):
                try:
                    data_stream = io.BytesIO()
                    if remote.download_stream(data_stream, rm_paths[0], rm_paths[1]):
                        data_stream.seek(0)
                        return self._decode_and_store(data_stream)
                except Exception:
                    logger.exception(f"{self.__class__}: remote source error.")
        return None

    def _decode_and_store(self, fp: t.BinaryIO) -> T:
        v = self.decode(fp)
        if self.cache_data and Item.is_cache_enabled(self.__class__):
            self._data_cache = v
        return v

    @classmethod
    @abstractmethod
    def file_extensions(cls) -> t.Sequence[str]:
        """Returns the list of valid file extensions with leading dot. The first one is
        assumed to be associated to the encoding applied by `encode`.

        :return: the list of valid file extensions.
        :rtype: t.Sequence[str]
        """
        return []

    @classmethod
    @abstractmethod
    def decode(cls, fp: t.BinaryIO) -> T:
        """Reads data from a binary stream raising an exception if the operation cannot
        complete.

        :param fp: the binary strem to decode.
        :type fp: t.BinaryIO
        :return: the value extracted
        :rtype: T
        """
        pass

    @classmethod
    @abstractmethod
    def encode(cls, value: T, fp: t.BinaryIO) -> None:
        """Writes data to a binary stream raising an exception if the operation cannot
        complete. The encoding should be described by the first extension returned by
        `file_extensions`.

        :param value: the data to write.
        :type value: T
        :param fp: the binary stream.
        :type fp: t.BinaryIO
        """
        pass

    @classmethod
    def validate(cls, raw_data: t.Any) -> T:
        """Subclasses can override to validate or process a raw value before storing.
        Raise an exception if the operation cannot complete.

        :param raw_data: the raw value got from the user.
        :type raw_data: t.Any
        :return: the processed value that can be internally stored.
        :rtype: T
        """
        return raw_data

    def __repr__(self):
        return (
            f"{self.__class__}(data={repr(self._data_cache)},"
            f" sources={self._file_sources}, remotes={self._remote_sources})"
        )

    def __str__(self):
        str_srcs = [f"    - {str(fs)}" for fs in self._file_sources]
        str_rmts = [f"    - {pr.geturl()}" for pr in self._remote_sources]
        return "\n".join(
            [
                f"{self.__class__.__name__}:",
                f"    data: {str(self._data_cache)}",
                "    sources:",
            ]
            + str_srcs
            + ["    remotes:"]
            + str_rmts
        )


class UnknownItem(Item[t.Any]):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return ["._"]

    @classmethod
    def decode(cls, fp: t.BinaryIO) -> t.Any:
        raise NotImplemented("Unknown item type.")

    @classmethod
    def encode(cls, value: t.Any, fp: t.BinaryIO):
        raise NotImplemented("Unknown item type.")
