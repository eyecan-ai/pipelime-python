from __future__ import annotations
from abc import ABCMeta, abstractmethod
from pathlib import Path
from urllib.parse import ParseResult, urlparse
from enum import IntEnum
from copy import deepcopy
from contextlib import ContextDecorator, contextmanager
from loguru import logger
import io
import shutil
import os
import json

import typing as t

import pipelime.remotes as plr


class SerializationMode(IntEnum):
    """Standard resolution is REMOTE FILE -> HARD LINK -> FILE COPY -> NEW FILE
    or SYM LINK -> FILE COPY -> NEW FILE. You can alter this behaviour by setting
    default and disabled serialization modes."""

    CREATE_NEW_FILE = 0
    DEEP_COPY = 1
    SYM_LINK = 2
    HARD_LINK = 3
    REMOTE_FILE = 4


class ItemFactory(ABCMeta):
    """Item classes register themselves in this factory, so that they can be
    instantiated on request when loading file from disk. To store user-created data a
    specific Item should be explicitly created.
    """

    ITEM_CLASSES: t.Dict[str, t.Type["Item"]] = {}
    REMOTE_FILE_EXT = ".remote"
    ITEM_DATA_CACHE_MODE: t.Dict[t.Type["Item"], t.Optional[bool]] = {}
    ITEM_SERIALIZATION_MODE: t.Dict[t.Type["Item"], SerializationMode] = {}
    ITEM_DISABLED_SERIALIZATION_MODES: t.Dict[
        t.Type["Item"], t.Set[SerializationMode]
    ] = {}

    def __init__(cls, name, bases, dct):
        """Registers item class extensions."""
        for ext in cls.file_extensions():  # type: ignore
            if ext == cls.REMOTE_FILE_EXT:
                raise ValueError(f"{cls.REMOTE_FILE_EXT} file extension is reserved")
            cls.ITEM_CLASSES[ext] = cls  # type: ignore
        cls.ITEM_DATA_CACHE_MODE[cls] = None  # type: ignore
        cls.ITEM_SERIALIZATION_MODE[cls] = SerializationMode.REMOTE_FILE  # type: ignore
        cls.ITEM_DISABLED_SERIALIZATION_MODES[cls] = set()  # type: ignore
        super().__init__(name, bases, dct)

    @classmethod
    def get_instance(
        cls, filepath: t.Union[Path, str], shared_item: bool = False
    ) -> Item:
        """Returns the item that can handle the given file."""
        filepath = Path(filepath)
        path_or_urls = [filepath]
        ext = filepath.suffix
        if ext == cls.REMOTE_FILE_EXT:
            with filepath.open("r") as fp:
                url_list = json.load(fp)
                url_list = [urlparse(u) for u in url_list if u]
            if not url_list:
                raise ValueError(f"The file {filepath} does not contain any remote.")
            ext = Path(url_list[0].path).suffix
            path_or_urls = url_list

        item_cls = cls.ITEM_CLASSES.get(ext, UnknownItem)
        return item_cls(*path_or_urls, shared=shared_item)

    @classmethod
    def set_data_cache_mode(
        cls, item_cls: t.Type["Item"], enable_data_cache: t.Optional[bool]
    ):
        cls.ITEM_DATA_CACHE_MODE[item_cls] = enable_data_cache

    @classmethod
    def is_cache_enabled(cls, item_cls: t.Type["Item"]) -> bool:
        for base_cls in item_cls.mro():
            if issubclass(base_cls, Item):
                value = cls.ITEM_DATA_CACHE_MODE.get(base_cls, None)
                if value is not None:
                    return value
        return True

    @classmethod
    def set_serialization_mode(cls, item_cls: t.Type["Item"], mode: SerializationMode):
        cls.ITEM_SERIALIZATION_MODE[item_cls] = mode

    @classmethod
    def get_serialization_mode(cls, item_cls: t.Type["Item"]) -> SerializationMode:
        smode = cls.ITEM_SERIALIZATION_MODE[item_cls]
        for base_cls in item_cls.mro():
            if issubclass(base_cls, Item):
                other_smode = cls.ITEM_SERIALIZATION_MODE[base_cls]
                if other_smode < smode:
                    smode = other_smode
        return smode

    @classmethod
    def set_disabled_serialization_modes(
        cls,
        item_cls: t.Type["Item"],
        modes: t.Union[t.Set[SerializationMode], t.List[SerializationMode]],
    ):
        cls.ITEM_DISABLED_SERIALIZATION_MODES[item_cls] = set(modes)

    @classmethod
    def get_disabled_serialization_modes(
        cls, item_cls: t.Type["Item"]
    ) -> t.List[SerializationMode]:
        smodes = set()
        for base_cls in item_cls.mro():
            if issubclass(base_cls, Item):
                smodes |= cls.ITEM_DISABLED_SERIALIZATION_MODES[base_cls]
        return list(smodes)


def set_item_serialization_mode(mode: SerializationMode, *item_cls: t.Type["Item"]):
    """Sets serialization mode for some or all items.
    Applies to all items if no item class is given."""
    for itc in ItemFactory.ITEM_SERIALIZATION_MODE.keys() if not item_cls else item_cls:
        ItemFactory.set_serialization_mode(itc, mode)


def set_item_disabled_serialization_modes(
    modes: t.List[SerializationMode], *item_cls: t.Type["Item"]
):
    """Disables serialization modes on selected item classes.
    Applies to all items if no item class is given."""
    for itc in ItemFactory.ITEM_SERIALIZATION_MODE.keys() if not item_cls else item_cls:
        ItemFactory.set_disabled_serialization_modes(itc, modes)


def disable_item_data_cache(*item_cls: t.Type["Item"]):
    """Disables data cache on selected item classes.
    Applies to all items if no item class is given."""
    for itc in item_cls if item_cls else ItemFactory.ITEM_DATA_CACHE_MODE.keys():
        ItemFactory.set_data_cache_mode(itc, False)


def enable_item_data_cache(*item_cls: t.Type["Item"]):
    """Enables data cache on selected item classes.
    Applies to all items if no item class is given."""
    for itc in item_cls if item_cls else ItemFactory.ITEM_DATA_CACHE_MODE.keys():
        ItemFactory.set_data_cache_mode(itc, True)


class item_serialization_mode(ContextDecorator):
    """Use this class as context manager or function decorator to temporarily change
    the items' serialization mode.

    .. code-block::
       :caption: Example

       # set the serialization mode for all items
       with item_serialization_mode("HARD_LINK"):
           ...

       # set the serialization mode only for ImageItem and NumpyItem
       with item_serialization_mode(SerializationMode.HARD_LINK, ImageItem, NumpyItem):
           ...

       # apply at function invocation
       @item_serialization_mode(SerializationMode.HARD_LINK, ImageItem)
       def my_fn():
           ...
    """

    def __init__(
        self, smode: t.Union[str, SerializationMode], *item_cls: t.Type["Item"]
    ):
        self._target_mode = (
            smode if isinstance(smode, SerializationMode) else SerializationMode[smode]
        )
        self._items = set(
            item_cls if item_cls else ItemFactory.ITEM_SERIALIZATION_MODE.keys()
        )

    def __enter__(self):
        self._prev_mode = {
            itc: ItemFactory.ITEM_SERIALIZATION_MODE[itc] for itc in self._items
        }
        set_item_serialization_mode(self._target_mode, *self._items)

    def __exit__(self, exc_type, exc_value, traceback):
        for itc, val in self._prev_mode.items():
            ItemFactory.set_serialization_mode(itc, val)


class item_disabled_serialization_modes(ContextDecorator):
    """Use this class as context manager or function decorator to temporarily change
    the items' disabled serialization modes.

    .. code-block::
       :caption: Example

       # disabled serialization modes for all items
       with item_disabled_serialization_modes(["HARD_LINK", "DEEP_COPY"]):
           ...

       # disabled serialization modes only for ImageItem and NumpyItem
       with item_disabled_serialization_modes(
           SerializationMode.HARD_LINK, ImageItem, NumpyItem
       ):
           ...

       # apply at function invocation
       @item_disabled_serialization_modes(
           ["REMOTE_FILE", SerializationMode.SYM_LINK], ImageItem
       )
       def my_fn():
           ...
    """

    def __init__(
        self,
        smodes: t.Union[
            str, SerializationMode, t.Sequence[t.Union[str, SerializationMode]]
        ],
        *item_cls: t.Type["Item"],
    ):
        if isinstance(smodes, str) or isinstance(smodes, SerializationMode):
            smodes = [smodes]
        self._target_modes = [
            m if isinstance(m, SerializationMode) else SerializationMode[m]
            for m in smodes
        ]
        self._items = set(
            item_cls
            if item_cls
            else ItemFactory.ITEM_DISABLED_SERIALIZATION_MODES.keys()
        )

    def __enter__(self):
        self._prev_modes = {
            itc: ItemFactory.ITEM_DISABLED_SERIALIZATION_MODES[itc]
            for itc in self._items
        }
        set_item_disabled_serialization_modes(self._target_modes, *self._items)

    def __exit__(self, exc_type, exc_value, traceback):
        for itc, val in self._prev_modes.items():
            ItemFactory.set_disabled_serialization_modes(itc, val)


class no_data_cache(ContextDecorator):
    """Use this class as context manager or function decorator to disable data caching
    on some or all item types.

    .. code-block::
       :caption: Example

       # disable data cache for all items
       with no_data_cache():
           ...

       # disable only for BinaryItem and NumpyItem
       with no_data_cache(BinaryItem, NumpyItem):
           ...

       # apply at function invocation
       @no_data_cache(ImageItem)
       def my_fn():
           ...
    """

    def __init__(self, *item_cls: t.Type["Item"]):
        self._items = item_cls if item_cls else ItemFactory.ITEM_DATA_CACHE_MODE.keys()

    def __enter__(self):
        self._prev_state = {
            itc: ItemFactory.ITEM_DATA_CACHE_MODE[itc] for itc in self._items
        }
        disable_item_data_cache(*self._items)

    def __exit__(self, exc_type, exc_value, traceback):
        for itc, val in self._prev_state.items():
            ItemFactory.set_data_cache_mode(itc, val)


class data_cache(ContextDecorator):
    """Use this class as context manager or function decorator to enable data caching
    on some or all item types. Useful when nested with ``no_data_cache``.

    .. code-block::
       :caption: Example

       # disable data cache for all items, then re-enable it
       with no_data_cache(...):
           with data_cache(...):
               ...
           ...
    """

    def __init__(self, *item_cls: t.Type["Item"]):
        self._items = item_cls if item_cls else ItemFactory.ITEM_DATA_CACHE_MODE.keys()

    def __enter__(self):
        self._prev_state = {
            itc: ItemFactory.ITEM_DATA_CACHE_MODE[itc] for itc in self._items
        }
        enable_item_data_cache(*self._items)

    def __exit__(self, exc_type, exc_value, traceback):
        for itc, val in self._prev_state.items():
            ItemFactory.set_data_cache_mode(itc, val)


@contextmanager
def _unclosable_BytesIO():
    """A BytesIO that is closed only when exiting this context."""
    fd = io.BytesIO()
    close = fd.close
    fd.close = lambda: None
    yield fd
    fd.close = close
    fd.close()


T = t.TypeVar("T")
_item_data_source = t.Union[Path, ParseResult]
_item_init_types = t.Union["Item", Path, ParseResult, t.BinaryIO, t.Any]


class Item(t.Generic[T], metaclass=ItemFactory):  # type: ignore
    """Base class for any supported Item. Concrete classes should ideally implement just
    the abstract methods, leaving ``__init__`` as is.
    """

    _data_cache: t.Optional[T]
    _file_sources: t.List[Path]
    _remote_sources: t.List[ParseResult]
    _cache_data: t.Optional[bool]
    _shared: bool
    _serialization_mode: t.Optional[SerializationMode]

    def __init__(
        self,
        *sources: _item_init_types,
        shared: bool = False,
        dont_check_paths: bool = False,
    ):
        super().__init__()
        self._data_cache = None
        self._file_sources = []
        self._remote_sources = []
        self._cache_data = None
        self._shared = shared
        self._serialization_mode = None

        # if not sources:
        #     logger.warning(f"{self.__class__}: no source data.")

        for src in sources:
            if isinstance(src, Item):
                src = src()

            if isinstance(src, (Path, ParseResult)):
                self._add_data_source(src, dont_check_paths)
            elif self._data_cache is not None:
                raise ValueError(f"{self.__class__.__name__}: Cannot set data twice.")
            elif isinstance(src, (t.BinaryIO, io.IOBase)):
                self._data_cache = self.decode(src)
            else:
                self._data_cache = self.validate(src)

    @property
    def cache_data(self) -> t.Optional[bool]:
        return self._cache_data

    @cache_data.setter
    def cache_data(self, enabled: t.Optional[bool]):
        self._cache_data = enabled

    @property
    def serialization_mode(self) -> t.Optional[SerializationMode]:
        return self._serialization_mode

    @serialization_mode.setter
    def serialization_mode(self, mode: t.Optional[t.Union[SerializationMode, str]]):
        self._serialization_mode = (
            mode
            if mode is None or isinstance(mode, SerializationMode)
            else SerializationMode[mode]
        )

    @property
    def is_shared(self) -> bool:
        return self._shared

    @property
    def local_sources(self) -> t.Sequence[Path]:
        return deepcopy(self._file_sources)

    @property
    def remote_sources(self) -> t.Sequence[ParseResult]:
        return deepcopy(self._remote_sources)

    def effective_serialization_mode(self) -> SerializationMode:
        smode = (
            self.serialization_mode
            if self.serialization_mode is not None
            else ItemFactory.get_serialization_mode(self.__class__)
        )
        if smode is SerializationMode.REMOTE_FILE and (
            not self._remote_sources
            or not self.is_mode_enabled(SerializationMode.REMOTE_FILE)
        ):
            smode = SerializationMode.HARD_LINK
        return smode

    def is_mode_enabled(self, mode: SerializationMode) -> bool:
        return mode not in ItemFactory.get_disabled_serialization_modes(self.__class__)

    @classmethod
    def make_new(cls, *sources: _item_init_types, shared: bool = False) -> Item:
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
            raise ValueError(
                f"{self.__class__.__name__}: invalid extension for `{srcstr}`"
            )

    def _add_data_source(
        self, source: _item_data_source, dont_check_paths: bool = False
    ) -> bool:
        if not dont_check_paths:
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

    def _serialize_to_local_file(self, path: Path) -> t.Optional[Path]:  # noqa: C901
        def _try_copy(copy_fn: t.Callable[[str, str], None], p: str) -> bool:
            for f in self._file_sources:
                try:
                    copy_fn(str(f), p)
                    return True
                except Exception:
                    # logger.exception(f"{self.__class__}: data serialization error.")
                    pass
            return False

        target_path = path.resolve()
        smode: t.Optional[SerializationMode] = self.effective_serialization_mode()

        # At this point if smode is REMOTE_FILE, then REMOTE_FILE is not disabled.
        if smode is SerializationMode.REMOTE_FILE:
            path = self.as_default_remote_file(target_path)
        elif target_path.suffix not in self.file_extensions():
            path = self.as_default_name(target_path)
        else:
            path = target_path

        if smode is SerializationMode.REMOTE_FILE:
            # it's safe to delete an existing remote file
            path.unlink(missing_ok=True)

            try:
                with path.open("w") as fp:
                    json.dump([rm.geturl() for rm in self._remote_sources], fp)
                return None  # do not save remote file among file sources!
            except Exception as exc:
                logger.warning(  # logger.exception(
                    f"{self.__class__}: remote file serialization error `{exc}`."
                )

                # remove any unfinished remote file
                path.unlink(missing_ok=True)

                # fall back to local file path
                path = (
                    self.as_default_name(target_path)
                    if target_path.suffix not in self.file_extensions()
                    else target_path
                )
                smode = SerializationMode.HARD_LINK

        # skip if local file already exists
        if path in self._file_sources:
            return None
        path.unlink(missing_ok=True)

        if smode is SerializationMode.HARD_LINK:
            smode = (
                None
                if (
                    self.is_mode_enabled(SerializationMode.HARD_LINK)
                    and _try_copy(os.link, str(path))
                )
                else SerializationMode.DEEP_COPY
            )

        if smode is SerializationMode.SYM_LINK:
            smode = (
                None
                if self.is_mode_enabled(SerializationMode.SYM_LINK)
                and _try_copy(os.symlink, str(path))
                else SerializationMode.DEEP_COPY
            )

        if smode is SerializationMode.DEEP_COPY:
            smode = (
                None
                if self.is_mode_enabled(SerializationMode.DEEP_COPY)
                and _try_copy(shutil.copy, str(path))
                else SerializationMode.CREATE_NEW_FILE
            )

        if smode is SerializationMode.CREATE_NEW_FILE and self.is_mode_enabled(
            SerializationMode.CREATE_NEW_FILE
        ):
            data = self()
            if data is not None:
                try:
                    with path.open("wb") as fp:
                        self.encode(data, fp)
                    smode = None
                except Exception as exc:
                    logger.warning(  # logger.exception(
                        f"{self.__class__}: new file serialization error `{exc}`."
                    )
                    pass

        if smode is None:
            return path
        logger.warning(f"{self.__class__}: cannot serialize item data.")
        return None

    def _serialize_to_remote(self, remote_url: ParseResult) -> t.Optional[ParseResult]:
        remote, rm_paths = plr.create_remote(remote_url), plr.paths_from_url(remote_url)
        if remote is not None and rm_paths[0] is not None:
            data = self()
            if data is None:
                logger.warning(f"{self.__class__}: no data to upload to remote.")
            else:
                with _unclosable_BytesIO() as data_stream:
                    self.encode(data, data_stream)

                    data_stream.seek(0, io.SEEK_END)
                    data_size = data_stream.tell()
                    data_stream.seek(0, io.SEEK_SET)

                    return remote.upload_stream(
                        data_stream, data_size, rm_paths[0], self.file_extensions()[0]
                    )
        return None

    def serialize(self, *targets: _item_data_source):
        for trg in targets:
            data_source = (
                self._serialize_to_local_file(trg)
                if isinstance(trg, Path)
                else self._serialize_to_remote(trg)
            )
            if data_source is not None:
                self._add_data_source(data_source)

    def remove_data_source(self, *sources: _item_data_source) -> Item:
        def _normalize_source(src: _item_data_source) -> t.List[_item_data_source]:
            if isinstance(src, Path):
                src = src.resolve()
                # Path.parents supports slicing only from 3.10 onwards
                pp = [src] + [p for p in src.parents]
                pp.pop()  # remove last element, which is `.`, `/`, `c:\` etc.
                return pp  # type: ignore
            src_path = Path(src.path)
            if src_path.suffix:
                src_path = src_path.parent
            return [
                ParseResult(
                    scheme=src.scheme,
                    netloc=src.netloc if src.netloc else "localhost",
                    path=src_path.as_posix(),
                    params="",
                    query="",
                    fragment="",
                )
            ]

        to_be_removed = [pp for src in sources for pp in _normalize_source(src)]

        new_sources: t.List[t.Any] = []
        for src in self._file_sources:
            src_ps = src.parents
            for tbr in to_be_removed:
                if tbr in src_ps:
                    break
            else:
                new_sources.append(src)

        new_sources += [
            src
            for src in self._remote_sources
            if _normalize_source(src)[0] not in to_be_removed
        ]
        new_sources += (
            [self._data_cache]
            if self._data_cache is not None
            else ([self()] if not new_sources else [])
        )

        return self.make_new(*new_sources, shared=self.is_shared)

    def __call__(self) -> t.Optional[T]:
        if self._data_cache is not None:
            return self._data_cache
        for fsrc in self._file_sources:
            try:
                with open(fsrc, "rb") as fp:
                    return self._decode_and_store(fp)
            except Exception as exc:
                logger.warning(  # logger.exception(
                    f"{self.__class__}: file source error `{exc}`."
                )
        for rmsrc in self._remote_sources:
            remote, rm_paths = plr.create_remote(rmsrc), plr.paths_from_url(rmsrc)
            if (
                remote is not None
                and rm_paths[0] is not None
                and rm_paths[1] is not None
            ):
                try:
                    with _unclosable_BytesIO() as data_stream:
                        if remote.download_stream(
                            data_stream, rm_paths[0], rm_paths[1]
                        ):
                            data_stream.seek(0)
                            return self._decode_and_store(data_stream)
                except Exception as exc:
                    logger.warning(  # logger.exception(
                        f"{self.__class__}: remote source error `{exc}`."
                    )
        return None

    def _decode_and_store(self, fp: t.BinaryIO) -> T:
        v = self.decode(fp)
        if (
            self.cache_data is None
            and Item.is_cache_enabled(self.__class__)
            or self.cache_data
        ):
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

    def __repr__(self) -> str:
        return (
            f"{self.__class__}(data={repr(self._data_cache)}, "
            f"sources={self._file_sources}, remotes={self._remote_sources}) "
            f"shared={self.is_shared}, cache={self.cache_data}, "
            f"serialization={self.serialization_mode})"
        )

    def __str__(self) -> str:
        str_srcs = [f"    - {str(fs)}" for fs in self._file_sources]
        str_rmts = [f"    - {pr.geturl()}" for pr in self._remote_sources]
        return "\n".join(
            [
                f"{self.__class__.__name__}:",
                f"  data: {str(self._data_cache)}",
                "  sources:",
            ]
            + str_srcs
            + ["  remotes:"]
            + str_rmts
            + [
                f"  shared: {self.is_shared}",
                f"  cache: {self.cache_data}",
                f"  serialization: {self.serialization_mode}",
            ]
        )

    def __pl_pretty__(self) -> t.Any:
        from rich.tree import Tree
        from rich.panel import Panel
        from rich import box

        item_tree = Tree(f"{self.__class__.__name__}")
        if self._data_cache is None:
            item_tree.add("data")
        else:
            item_tree.add(
                Panel.fit(
                    self.pl_pretty_data(self._data_cache),
                    title="data",
                    title_align="left",
                    box=box.HORIZONTALS,
                )
            )
        branch = item_tree.add("sources")
        for fs in self._file_sources:
            branch.add(str(fs))
        branch = item_tree.add("remotes")
        for pr in self._remote_sources:
            branch.add(pr.geturl())
        item_tree.add(f"shared: {self.is_shared}")
        item_tree.add(f"cache: {self.cache_data}")
        item_tree.add(f"serialization: {self.serialization_mode}")
        return item_tree

    @classmethod
    def pl_pretty_data(cls, value: T) -> t.Any:
        return str(value)


class UnknownItem(Item[t.Any]):
    @classmethod
    def file_extensions(cls) -> t.Sequence[str]:
        return ["._"]

    @classmethod
    def decode(cls, fp: t.BinaryIO) -> t.Any:
        raise NotImplementedError(f"{cls.__name__}: cannot decode.")  # pragma: no cover

    @classmethod
    def encode(cls, value: t.Any, fp: t.BinaryIO):
        raise NotImplementedError(f"{cls.__name__}: cannot encode.")  # pragma: no cover
