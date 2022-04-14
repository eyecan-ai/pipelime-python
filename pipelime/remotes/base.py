from abc import ABCMeta, abstractmethod
from pathlib import Path
import hashlib
from urllib.parse import ParseResult, unquote_plus, urlparse
from loguru import logger

import typing as t


class RemoteRegister(ABCMeta):
    """A factory class managing a single remote instance
    for any (scheme, netloc) pair.
    """

    REMOTE_CLASSES: t.Dict[str, t.Type["BaseRemote"]] = {}
    REMOTE_INSTANCES: t.Dict[t.Tuple[str, str], "BaseRemote"] = {}

    def __init__(cls, name, bases, dct):
        cls.REMOTE_CLASSES[cls.scheme()] = cls  # type: ignore
        super().__init__(name, bases, dct)

    @classmethod
    def get_instance(
        cls, scheme: str, netloc: str, **kwargs
    ) -> t.Optional["BaseRemote"]:
        """Return a remote instance for this scheme and netloc.

        :param scheme: the protocol name, eg, 's3' or 'file'.
        :type scheme: str
        :param netloc: the address of the endpoint.
        :type netloc: str
        :param **kwargs: arguments forwarded to the remote instance `__init__()`
        :return: the remote instance or None if the scheme is unknown.
        :rtype: Optional[BaseRemote]
        """
        remote_instance = cls.REMOTE_INSTANCES.get((scheme, netloc))
        if remote_instance is None:
            remote_class = cls.REMOTE_CLASSES.get(scheme)
            if remote_class is not None:
                remote_instance = remote_class(netloc, **kwargs)
                cls.REMOTE_INSTANCES[(scheme, netloc)] = remote_instance
            else:
                logger.warning(f"Unknown remote scheme '{scheme}'.")
        return remote_instance


def make_remote_url(
    scheme: str = "", netloc: str = "", path: t.Union[str, Path] = "", **init_args
) -> ParseResult:
    # we want to normalize the path as "/path/to/loc" or "/c:/path/to/loc"
    return urlparse(
        ParseResult(
            scheme=scheme,
            netloc=netloc,
            path=Path(path).as_posix(),
            params="",
            query=":".join([k + "=" + str(v) for k, v in init_args.items()]),
            fragment="",
        ).geturl()
    )


def create_remote(url: ParseResult) -> t.Optional["BaseRemote"]:
    """Return a remote instance for this scheme and netloc.

    :param url: the url describing the remote as
        `<scheme>://<netloc>/<any_path>[?<init-kw>=<init-val>:<init-kw>=<init-val>...]`
    :type url: ParseResult
    :return: the remote instance or None if the scheme is unknown.
    :rtype: Optional[BaseRemote]
    """

    def _convert_val(val: str):
        if val == "True":
            return True
        if val == "False":
            return False
        try:
            num = int(val)
            return num
        except ValueError:
            pass
        try:
            num = float(val)
            return num
        except ValueError:
            pass
        return val

    init_args = {
        kw.split("=", 1)[0]: _convert_val(kw.split("=", 1)[1])
        for kw in url.query.split(":")
        if len(kw) >= 3 and "=" in kw
    }
    return RemoteRegister.get_instance(url.scheme, url.netloc, **init_args)


def paths_from_url(
    url: ParseResult,
) -> t.Union[t.Tuple[str, str], t.Tuple[None, None]]:
    """Extract base path and target name from the path component of a given url.

    :param url: the url to split.
    :type url: ParseResult
    :return: the base path and the target name.
    :rtype: Union[Tuple[str, str], Tuple[None, None]]
    """
    if len(url.path) > 1:
        unquoted = unquote_plus(url.path)
        file_full_path = Path(unquoted[1:] if unquoted.startswith("/") else unquoted)
        if not file_full_path.suffix:
            return str(file_full_path.as_posix()), ""
        return str(file_full_path.parent.as_posix()), str(file_full_path.name)
    else:
        return None, None


class BaseRemote(metaclass=RemoteRegister):  # type: ignore
    """Base class for any remote."""

    def __init__(self, netloc: str):
        """Set the network address.

        :param netloc: the network address.
        :type netloc: str
        """
        self._netloc = netloc

    def upload_file(
        self, local_file: t.Union[Path, str], target_base_path: str
    ) -> t.Optional[ParseResult]:
        """Upload an existing file to remote storage.

        :param local_file: local file path.
        :type local_file: Union[Path, str]
        :param target_base_path: the remote base path, eg, the bucket name.
        :type target_base_path: str
        :return: a url to get back the file.
        :rtype: Optional[ParseResult]
        """
        local_file = Path(local_file)
        file_size = local_file.stat().st_size
        with open(local_file, "rb") as file_data:
            return self.upload_stream(
                file_data, file_size, target_base_path, "".join(local_file.suffixes)
            )

    def upload_stream(
        self,
        local_stream: t.BinaryIO,
        local_stream_size: int,
        target_base_path: str,
        target_suffix: str,
    ) -> t.Optional[ParseResult]:
        """Upload data from a readable stream.

        :param local_stream: a binary data stream.
        :type local_stream: BinaryIO
        :param local_stream_size: the byte size to upload.
        :type local_stream_size: int
        :param target_base_path: the remote base path, eg, the bucket name.
        :type target_base_path: str
        :param target_suffix: the remote file suffix.
        :type target_suffix: str
        :return: a url to get back the file.
        :rtype: Optional[ParseResult]
        """
        hash_name = self._compute_hash(
            local_stream, self._get_hash_fn(target_base_path)
        )
        target_name = hash_name + target_suffix
        if self.target_exists(target_base_path, target_name) or self._upload(
            local_stream, local_stream_size, target_base_path, target_name
        ):
            return self._make_url(f"{target_base_path}/{target_name}")
        return None

    def download_file(
        self,
        local_file: t.Union[Path, str],
        source_base_path: str,
        source_name: str,
    ) -> bool:
        """Download and write data to a local file.

        :param local_file: the file to write to.
        :type local_file: Union[Path, str]
        :param source_base_path: the remote base path, eg, the bucket name.
        :type source_base_path: str
        :param source_name: the remote object name.
        :type source_name: str
        :return: True if no error occurred.
        :rtype: bool
        """
        local_file = Path(local_file)
        source_name_path = Path(source_name)
        if local_file.suffixes != source_name_path.suffixes:
            local_file = local_file.with_suffix("".join(source_name_path.suffixes))
        if local_file.is_file():
            with open(local_file, "rb") as local_stream:
                hash_name = self._compute_hash(
                    local_stream, self._get_hash_fn(source_base_path)
                )
                if (
                    hash_name
                    == source_name_path.name[: -len("".join(source_name_path.suffixes))]
                ):
                    # local file is up-to-date
                    return True

        local_file.parent.mkdir(parents=True, exist_ok=True)
        part_file = local_file.with_suffix(local_file.suffix + ".part")
        offset: int = 0
        try:
            offset = part_file.stat().st_size
        except IOError:
            pass

        ok = False
        with open(part_file, "ab") as part_stream:
            ok = self.download_stream(
                part_stream, source_base_path, source_name, offset
            )

        if ok:
            local_file.unlink(missing_ok=True)
            part_file.rename(local_file)

        return ok

    def download_stream(
        self,
        local_stream: t.BinaryIO,
        source_base_path: str,
        source_name: str,
        source_offset: int = 0,
    ) -> bool:
        """Download and write data to a writable stream.

        :param local_stream: a writable stream.
        :type local_stream: BinaryIO
        :param source_base_path: the remote base path, eg, the bucket name.
        :type source_base_path: str
        :param source_name: the remote object name.
        :type source_name: str
        :param source_offset: where to start reading the remote data, defaults to 0
        :type source_offset: int, optional
        :return: True if no error occurred.
        :rtype: bool
        """
        return self._download(
            local_stream, source_base_path, source_name, source_offset
        )

    @abstractmethod
    def target_exists(self, target_base_path: str, target_name: str) -> bool:
        """Check if a remote object exists.

        :param target_base_path: the remote base path, eg, the bucket name.
        :type target_base_path: str
        :param target_name: the remote object name.
        :type target_name: str
        :return: True if <target_base_path>/<target_name> exists on this remote.
        :rtype: bool
        """
        ...

    def _compute_hash(self, stream: t.BinaryIO, hash_fn: t.Any = None) -> str:
        """Compute a hash value based on the content of a readable stream.

        :param stream: the stream to hash.
        :type stream: BinaryIO
        :param hash_fn: the hash funtion, if None `hashlib.sha256()` will be used,
            defaults to None
        :type hash_fn: Any, optional
        :return: the computed hash
        :rtype: str
        """
        if hash_fn is None:
            hash_fn = hashlib.sha256()
        b = bytearray(1024 * 1024)
        mv = memoryview(b)
        fpos = stream.tell()
        for n in iter(lambda: stream.readinto(mv), 0):  # type: ignore
            hash_fn.update(mv[:n])
        stream.seek(fpos)
        return hash_fn.hexdigest()

    def _make_url(self, target_full_path: str) -> ParseResult:
        """Make a complete url for this remote path.

        :param target_full_path: the remote path.
        :type target_full_path: str
        :return: the url
        :rtype: ParseResult
        """
        return ParseResult(
            scheme=self.scheme(),
            netloc=self.netloc,
            path=Path(target_full_path).as_posix(),
            params="",
            query="",
            fragment="",
        )

    @abstractmethod
    def _get_hash_fn(self, target_base_path: str) -> t.Any:
        """Return the hash function used for the object in the give remote base path.

        :param target_base_path: the remote base path, eg, the bucket name.
        :type target_base_path: str
        :return: the hash function, eg, `hashlib.sha256()`.
        :rtype: Any
        """
        ...

    @abstractmethod
    def _upload(
        self,
        local_stream: t.BinaryIO,
        local_stream_size: int,
        target_base_path: str,
        target_name: str,
    ) -> bool:
        """Upload data from a readable stream.

        :param local_stream: a binary data stream.
        :type local_stream: BinaryIO
        :param local_stream_size: the byte size to upload.
        :type local_stream_size: int
        :param target_base_path: the remote base path, eg, the bucket name.
        :type target_base_path: str
        :param target_name: the remote object name.
        :type target_name: str
        :return: True if no error occurred.
        :rtype: bool
        """
        ...

    @abstractmethod
    def _download(
        self,
        local_stream: t.BinaryIO,
        source_base_path: str,
        source_name: str,
        source_offset: int,
    ) -> bool:
        """Download and write data to a writable stream.

        :param local_stream: a writable stream.
        :type local_stream: BinaryIO
        :param source_base_path: the remote base path, eg, the bucket name.
        :type source_base_path: str
        :param source_name: the remote object name.
        :type source_name: str
        :param source_offset: where to start reading the remote data, defaults to 0
        :type source_offset: int, optional
        :return: True if no error occurred.
        :rtype: bool
        """
        ...

    @classmethod
    @abstractmethod
    def scheme(cls) -> str:
        """Return the scheme name, eg, 's3' or 'file'.

        :return: the scheme name.
        :rtype: str
        """
        pass

    @property
    def netloc(self) -> str:
        """Return the remote network address.

        :return: the remote network address.
        :rtype: str
        """
        return self._netloc

    @property
    @abstractmethod
    def is_valid(self) -> bool:
        """Check if this remote instance is valid and usable.

        :return: True if this instance is valid.
        :rtype: bool
        """
        pass
