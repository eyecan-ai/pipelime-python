from __future__ import annotations

import inspect
import io
import json
import os
import shutil
import typing as t
from abc import ABCMeta, abstractmethod
from contextlib import ContextDecorator, contextmanager
from copy import deepcopy
from enum import IntEnum
from pathlib import Path
from urllib.parse import ParseResult, urlparse

from loguru import logger


# IMPORTANT: IF YOU CHANGE THIS ENUM, YOU MUST ALSO CHANGE THE
#   ACCEPTED VALUES IN THE SERIALIZATION INTERFACE
# SEE: pipelime\commands\interfaces.py
#   -> SerializationModeInterface
#   -> any_serialization
class SerializationMode(IntEnum):
    """Standard resolution is HARD LINK -> FILE COPY -> NEW FILE
    or SYM LINK -> FILE COPY -> NEW FILE. You can alter this behaviour
    by setting default and disabled serialization modes.
    """

    CREATE_NEW_FILE = 0
    DEEP_COPY = 1
    SYM_LINK = 2
    HARD_LINK = 3


class deferred_classattr:
    """A class attribute that is set on first access.
    Useful to resolve circular dependencies.
    """

    def __init__(self, fget):
        if not isinstance(fget, (classmethod, staticmethod)):
            fget = classmethod(fget)
        self._fget = fget

    def __get__(self, instance, owner=None):
        if owner is None:
            owner = type(instance)
        src_fn = self._fget.__get__(instance, owner)
        value = src_fn()

        # replace this descriptor with the actual value
        setattr(owner, src_fn.__name__, value)
        return value


class ItemFactory(ABCMeta):
    """Item classes register themselves in this factory, so that they can be
    instantiated on request when loading file from disk. To store user-created data a
    specific Item should be explicitly created.
    """

    ITEM_CLASSES: t.Dict[str, t.Type[Item]] = {}
    ITEM_DATA_CACHE_MODE: t.Dict[t.Type[Item], t.Optional[bool]] = {}
    ITEM_SERIALIZATION_MODE: t.Dict[t.Type[Item], SerializationMode] = {}
    ITEM_DISABLED_SERIALIZATION_MODES: t.Dict[
        t.Type[Item], t.Set[SerializationMode]
    ] = {}

    def __init__(cls, name, bases, dct, **kwargs):
        """Registers item class extensions."""
        for ext in cls.file_extensions():  # type: ignore
            if ext in cls.ITEM_CLASSES:
                raise ValueError(f"File extension `{ext}` is already registered")
            cls.ITEM_CLASSES[ext] = cls  # type: ignore
        cls.ITEM_DATA_CACHE_MODE[cls] = None  # type: ignore
        cls.ITEM_SERIALIZATION_MODE[cls] = SerializationMode.HARD_LINK  # type: ignore
        cls.ITEM_DISABLED_SERIALIZATION_MODES[cls] = set()  # type: ignore

        super().__init__(name, bases, dct)

    @classmethod
    def get_instance(
        cls, filepath: t.Union[Path, str], shared_item: bool = False
    ) -> Item:
        """Returns the item that can handle the given file.

        Args:
          filepath: t.Union[Path:str]: the path to the file.
          shared_item: bool: whether the new Item should be shared
            (Default value = False)

        Returns:
            Item: an Item instance wrapping the given file.
        """
        filepath = Path(filepath)
        path_or_urls = [filepath]
        ext = filepath.suffix

        item_cls = cls.ITEM_CLASSES.get(ext, UnknownItem)
        return item_cls(*path_or_urls, shared=shared_item)

    @classmethod
    def set_data_cache_mode(
        cls, item_cls: t.Type[Item], enable_data_cache: t.Optional[bool]
    ):
        cls.ITEM_DATA_CACHE_MODE[item_cls] = enable_data_cache

    @classmethod
    def is_cache_enabled(cls, item_cls: t.Type[Item]) -> bool:
        for base_cls in item_cls.mro():
            if issubclass(base_cls, Item):
                value = cls.ITEM_DATA_CACHE_MODE.get(base_cls, None)
                if value is not None:
                    return value
        return True

    @classmethod
    def set_serialization_mode(cls, item_cls: t.Type[Item], mode: SerializationMode):
        cls.ITEM_SERIALIZATION_MODE[item_cls] = mode

    @classmethod
    def get_serialization_mode(cls, item_cls: t.Type[Item]) -> SerializationMode:
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
        item_cls: t.Type[Item],
        modes: t.Union[t.Set[SerializationMode], t.List[SerializationMode]],
    ):
        cls.ITEM_DISABLED_SERIALIZATION_MODES[item_cls] = set(modes)

    @classmethod
    def get_disabled_serialization_modes(
        cls, item_cls: t.Type[Item]
    ) -> t.List[SerializationMode]:
        smodes = set()
        for base_cls in item_cls.mro():
            if issubclass(base_cls, Item):
                smodes |= cls.ITEM_DISABLED_SERIALIZATION_MODES[base_cls]
        return list(smodes)


def set_item_serialization_mode(mode: SerializationMode, *item_cls: t.Type[Item]):
    """Sets serialization mode for some or all items.
    Applies to all items if no item class is given.
    """
    for itc in ItemFactory.ITEM_SERIALIZATION_MODE.keys() if not item_cls else item_cls:
        ItemFactory.set_serialization_mode(itc, mode)


def set_item_disabled_serialization_modes(
    modes: t.List[SerializationMode], *item_cls: t.Type[Item]
):
    """Disables serialization modes on selected item classes.
    Applies to all items if no item class is given.
    """
    for itc in ItemFactory.ITEM_SERIALIZATION_MODE.keys() if not item_cls else item_cls:
        ItemFactory.set_disabled_serialization_modes(itc, modes)


def disable_item_data_cache(*item_cls: t.Type[Item]):
    """Disables data cache on selected item classes.
    Applies to all items if no item class is given.
    """
    for itc in item_cls if item_cls else ItemFactory.ITEM_DATA_CACHE_MODE.keys():
        ItemFactory.set_data_cache_mode(itc, False)


def enable_item_data_cache(*item_cls: t.Type[Item]):
    """Enables data cache on selected item classes.
    Applies to all items if no item class is given.
    """
    for itc in item_cls if item_cls else ItemFactory.ITEM_DATA_CACHE_MODE.keys():
        ItemFactory.set_data_cache_mode(itc, True)


class item_serialization_mode(ContextDecorator):
    """Use this class as context manager or function decorator to temporarily change
    the items' serialization mode.

    Examples:
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

    def __init__(self, smode: t.Union[str, SerializationMode], *item_cls: t.Type[Item]):
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

    Examples:
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
           ["HARD_LINK", SerializationMode.SYM_LINK], ImageItem
       )
       def my_fn():
           ...
    """

    def __init__(
        self,
        smodes: t.Union[
            str, SerializationMode, t.Sequence[t.Union[str, SerializationMode]]
        ],
        *item_cls: t.Type[Item],
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

    Examples:
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

    def __init__(self, *item_cls: t.Type[Item]):
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

    Examples:
       # disable data cache for all items, then re-enable it
       with no_data_cache(...):
           with data_cache(...):
               ...
           ...
    """

    def __init__(self, *item_cls: t.Type[Item]):
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
DerivedItemTp = t.TypeVar("DerivedItemTp", bound="Item")
_item_data_source = t.Union[Path, ParseResult]
_item_init_types = t.Union["Item", Path, ParseResult, t.BinaryIO, t.Any]


class Item(t.Generic[T], metaclass=ItemFactory):
    """Base class for any supported Item. Concrete classes should ideally implement just
    the abstract methods (`file_extensions`, `decode`, `encode`),
    leaving `__init__` as is. Optionally, `validate` may also be implemented to process
    any raw data before storing it as type `T`.

    Finally, derived abstract classes may specify a preferred concrete class to be used
    when calling `make_new`. To this end, just override the method `default_concrete`
    (using the `@deferred_classattr` decorator) or add `default_concrete=ItemClass`
    in the class definition, eg::

        class MyItem(Item, default_concrete=MyItemConcrete):
            pass

        class MyOtherItem(Item):
            @deferred_classattr
            def default_concrete(cls):
                return MyOtherItemConcrete
    """

    _data_cache: t.Optional[T]
    _file_sources: t.List[Path]
    _cache_data: t.Optional[bool]
    _shared: bool
    _serialization_mode: t.Optional[SerializationMode]

    @deferred_classattr
    def default_concrete(cls) -> t.Type[Item]:
        return UnknownItem

    @classmethod
    def __init_subclass__(cls, default_concrete: t.Optional[t.Type[Item]] = None):
        if default_concrete:
            cls.default_concrete = default_concrete  # type: ignore
        elif not inspect.isabstract(cls):
            cls.default_concrete = cls  # type: ignore

        super().__init_subclass__()

    def __init__(
        self,
        *sources: _item_init_types,
        shared: bool = False,
        dont_check_paths: bool = False,
    ):
        super().__init__()
        self._data_cache = None
        self._file_sources = []
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

    def effective_serialization_mode(self) -> SerializationMode:
        smode = (
            self.serialization_mode
            if self.serialization_mode is not None
            else ItemFactory.get_serialization_mode(self.__class__)
        )
        return smode

    def is_mode_enabled(self, mode: SerializationMode) -> bool:
        return mode not in ItemFactory.get_disabled_serialization_modes(self.__class__)

    @classmethod
    def make_new(
        cls: t.Type[DerivedItemTp], *sources: _item_init_types, shared: bool = False
    ) -> DerivedItemTp:
        return cls.default_concrete(*sources, shared=shared)

    def default_extension(self) -> t.Optional[str]:
        ext = self.file_extensions()[0]
        if ext is None and self._file_sources:
            return self._file_sources[0].suffix
        return ext

    def with_extension(self, filepath: Path, ext: t.Optional[str]) -> Path:
        if ext is None:
            ext = self.default_extension()
        return filepath if ext is None else filepath.with_suffix(ext)

    def as_default_name(self, filepath: Path) -> Path:
        return self.with_extension(filepath, None)

    def get_all_names(self, filepath: Path) -> t.List[Path]:
        names = [self.with_extension(filepath, ext) for ext in self.file_extensions()]
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
        if not (dont_check_paths or None in self.file_extensions()):
            self._check_source(source)
        if isinstance(source, Path):
            source = source.resolve().absolute()
            if source not in self._file_sources:
                self._file_sources.append(source)
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

        target_path = path.resolve().absolute()
        smode: t.Optional[SerializationMode] = self.effective_serialization_mode()

        if target_path.suffix not in self.file_extensions():
            path = self.as_default_name(target_path)
        else:
            path = target_path

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

    def serialize(self, *targets: _item_data_source):
        for trg in targets:
            data_source = None
            if isinstance(trg, Path) or isinstance(trg, str):
                trg = Path(trg).absolute().resolve()
                data_source = self._serialize_to_local_file(trg)
            if data_source is not None:
                self._add_data_source(data_source)
                if (
                    self.cache_data is None
                    and Item.is_cache_enabled(self.__class__) is False
                    or self.cache_data is False
                ):
                    self._data_cache = None

    def remove_data_source(
        self: DerivedItemTp, *sources: _item_data_source
    ) -> DerivedItemTp:
        def _normalize_source(src: _item_data_source) -> t.List[_item_data_source]:
            if isinstance(src, Path):
                src = src.resolve().absolute()
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

        Returns:
          t.Sequence[str]: the list of valid file extensions.
        """
        return []

    @classmethod
    @abstractmethod
    def decode(cls, fp: t.BinaryIO) -> T:
        """Reads data from a binary stream raising an exception if the operation cannot
        complete.

        Args:
          fp (t.BinaryIO): the binary stream to decode.

        Returns:
          T: the value extracted
        """
        pass

    @classmethod
    @abstractmethod
    def encode(cls, value: T, fp: t.BinaryIO) -> None:
        """Writes data to a binary stream raising an exception if the operation cannot
        complete. The encoding should be described by the first extension returned by
        `file_extensions`.

        Args:
          value (T): the data to write.
          fp (t.BinaryIO): the output binary stream.
        """
        pass

    @classmethod
    def validate(cls, raw_data: t.Any) -> T:
        """Subclasses can override to validate or process a raw value before storing.
        Raise an exception if the operation cannot complete.

        Args:
          raw_data (Any): the raw value got from the user.

        Returns:
          T: the processed value that can be internally stored.
        """
        return raw_data

    def __repr__(self) -> str:
        return (
            f"{self.__class__}(data={repr(self._data_cache)}, "
            f"sources={self._file_sources}, "
            f"shared={self.is_shared}, cache={self.cache_data}, "
            f"serialization={self.serialization_mode})"
        )

    def __str__(self) -> str:
        str_srcs = [f"    - {str(fs)}" for fs in self._file_sources]
        return "\n".join(
            [
                f"{self.__class__.__name__}:",
                f"  data: {str(self._data_cache)}",
                "  sources:",
            ]
            + str_srcs
            + [
                f"  shared: {self.is_shared}",
                f"  cache: {self.cache_data}",
                f"  serialization: {self.serialization_mode}",
            ]
        )

    def __pl_pretty__(self) -> t.Any:
        from rich import box
        from rich.panel import Panel
        from rich.tree import Tree

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
        # THIS IS A VERY SPECIAL PLACEHOLDER
        return [None]  # type: ignore

    @classmethod
    def decode(cls, fp: t.BinaryIO) -> t.Any:
        raise NotImplementedError(f"{cls.__name__}: cannot decode.")

    @classmethod
    def encode(cls, value: t.Any, fp: t.BinaryIO):
        raise NotImplementedError(f"{cls.__name__}: cannot encode.")
