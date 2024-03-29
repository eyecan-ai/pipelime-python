import hashlib
import typing as t
from abc import ABCMeta, abstractmethod
from pathlib import Path
from urllib.parse import ParseResult, unquote_plus, urlparse

from loguru import logger


class NetlocData:
    __slots__ = "host", "user", "password", "init_args"

    host: str
    user: str
    password: str
    init_args: t.Dict[str, t.Union[int, float, str, bool, None]]

    def __init__(self, url: ParseResult):
        user_pwd, _, self.host = url.netloc.rpartition("@")
        self.user, _, self.password = user_pwd.partition(":")

        self.init_args = {
            kw.split("=", 1)[0]: NetlocData._convert_val(kw.split("=", 1)[1])
            for kw in url.query.split(":")
            if len(kw) >= 3 and "=" in kw
        }

    @staticmethod
    def _convert_val(val: str):
        if val.lower() == "true":
            return True
        if val.lower() == "false":
            return False
        if val.lower() in ("none", "null"):
            return None
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
        cls, scheme: str, netloc_data: NetlocData
    ) -> t.Optional["BaseRemote"]:
        """Return a remote instance for this scheme and netloc.

        Args:
          scheme (str): the protocol name, eg, 's3' or 'file'.
          netloc_data (NetlocData): the netloc data info, ie, the host address and
            user account.
        """
        remote_instance = cls.REMOTE_INSTANCES.get((scheme, netloc_data.host))
        if remote_instance is None:
            remote_class = cls.REMOTE_CLASSES.get(scheme)
            if remote_class is not None:
                remote_instance = remote_class(netloc_data)
                cls.REMOTE_INSTANCES[(scheme, netloc_data.host)] = remote_instance
            else:
                logger.warning(f"Unknown remote scheme '{scheme}'.")  # pragma: no cover
        return remote_instance


def make_remote_url(
    *,
    scheme: str = "",
    user: str = "",
    password: str = "",
    host: str = "",
    port: t.Optional[int] = None,
    bucket: t.Union[str, Path] = "",
    **init_args,
) -> ParseResult:
    # NB: we want to normalize the path as "/path/to/loc" or "/c:/path/to/loc"
    if user:
        user_pwd = user
        if password:
            user_pwd += f":{password}"
        user_pwd += "@"
    else:
        user_pwd = ""
    host_port = host + (f":{port}" if port else "")
    return urlparse(
        ParseResult(
            scheme=scheme,
            netloc=f"{user_pwd}{host_port}",
            path=Path(bucket).as_posix(),
            params="",
            query=":".join(f"{k}={v}" for k, v in init_args.items()),
            fragment="",
        ).geturl()
    )


def create_remote(url: ParseResult) -> t.Optional["BaseRemote"]:
    """Return a remote instance for this scheme and netloc.

    Args:
      url (ParseResult): the url describing the remote as
        `<scheme>://[user[:pwd]@]<netloc>/<any_path>[?<init-kw>=<init-val>:<init-kw>=<init-val>...]`

    Returns:
      Optional[BaseRemote]: the remote instance or None if the scheme is unknown.
    """
    return RemoteRegister.get_instance(url.scheme, NetlocData(url))


def paths_from_url(
    url: ParseResult,
) -> t.Union[t.Tuple[str, str], t.Tuple[None, None]]:
    """Extract base path and target name from the path component of a given url.

    Args:
      url (ParseResult): the url to split.

    Returns:
      Union[Tuple[str, str], Tuple[None, None]]: the base path and the target name.
    """
    if len(url.path) > 1:
        unquoted = unquote_plus(url.path)
        file_full_path = Path(unquoted[1:] if unquoted.startswith("/") else unquoted)
        if not file_full_path.suffix:
            return str(file_full_path.as_posix()), ""
        return str(file_full_path.parent.as_posix()), str(file_full_path.name)
    else:
        return None, None  # pragma: no cover


class BaseRemote(metaclass=RemoteRegister):  # type: ignore
    """Base class for any remote."""

    def __init__(self, netloc_data: NetlocData):
        """Set the network address.

        Args:
            netloc_data (NetlocData): the network data info.
        """
        self._netloc = netloc_data.host

    def upload_file(
        self, local_file: t.Union[Path, str], target_base_path: str
    ) -> t.Optional[ParseResult]:
        """Upload an existing file to remote storage.

        Args:
          local_file (Union[Path, str]): local file path.
          target_base_path (str): the remote base path, eg, the bucket name.

        Returns:
          Optional[ParseResult]: a url to get back the file.
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

        Args:
          local_stream (BinaryIO): a binary data stream.
          local_stream_size (int): the byte size to upload.
          target_base_path (str): the remote base path, eg, the bucket name.
          target_suffix (str): the remote file suffix.

        Returns:
          Optional[ParseResult]: a url to get back the file.
        """
        hash_name = self._compute_hash(
            local_stream, self._get_hash_fn(target_base_path)
        )
        target_name = hash_name + target_suffix
        if self.target_exists(target_base_path, target_name) or self._upload(
            local_stream, local_stream_size, target_base_path, target_name
        ):
            return self._make_url(f"{target_base_path}/{target_name}")
        # upload has failed
        return None

    def download_file(
        self,
        local_file: t.Union[Path, str],
        source_base_path: str,
        source_name: str,
    ) -> bool:
        """Download and write data to a local file.

        Args:
          local_file (Union[Path, str]): the file to write to.
          source_base_path (str): the remote base path, eg, the bucket name.
          source_name (str): the remote object name.

        Returns:
          bool: True if no error occurred.
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

        Args:
          local_stream (BinaryIO): a writable stream.
          source_base_path (str): the remote base path, eg, the bucket name.
          source_name (str): the remote object name.
          source_offset (int, optional): where to start reading the remote data.
            (Default value = 0)

        Returns:
          bool: True if no error occurred.
        """
        return self._download(
            local_stream, source_base_path, source_name, source_offset
        )

    @abstractmethod
    def target_exists(self, target_base_path: str, target_name: str) -> bool:
        """Check if a remote object exists.

        Args:
          target_base_path (str): the remote base path, eg, the bucket name.
          target_name (str): the remote object name.

        Returns:
          bool: True if <target_base_path>/<target_name> exists on this remote.
        """
        pass

    def _compute_hash(self, stream: t.BinaryIO, hash_fn: t.Any = None) -> str:
        """Compute a hash value based on the content of a readable stream.

        Args:
          stream (BinaryIO): the stream to hash.
          hash_fn (Any, optional): the hash funtion, if None `hashlib.blake2b()` will
            be used  (Default value = None)

        Returns:
          str: the computed hash
        """
        if hash_fn is None:
            hash_fn = hashlib.blake2b()  # pragma: no cover
        b = bytearray(1024 * 1024)
        mv = memoryview(b)
        fpos = stream.tell()
        for n in iter(lambda: stream.readinto(mv), 0):  # type: ignore
            hash_fn.update(mv[:n])
        stream.seek(fpos)
        return hash_fn.hexdigest()

    def _make_url(self, target_full_path: str) -> ParseResult:
        """Make a complete url for this remote path.

        Args:
          target_full_path (str): the remote path.

        Returns:
          ParseResult: the url
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

        Args:
          target_base_path (str): the remote base path, eg, the bucket name.

        Returns:
          Any: the hash function, eg, `hashlib.blake2b()`.
        """
        pass

    @abstractmethod
    def _upload(
        self,
        local_stream: t.BinaryIO,
        local_stream_size: int,
        target_base_path: str,
        target_name: str,
    ) -> bool:
        """Upload data from a readable stream.

        Args:
          local_stream (BinaryIO): a binary data stream.
          local_stream_size (int): the byte size to upload.
          target_base_path (str): the remote base path, eg, the bucket name.
          target_name (str): the remote object name.

        Returns:
          bool: True if no error occurred.
        """
        pass

    @abstractmethod
    def _download(
        self,
        local_stream: t.BinaryIO,
        source_base_path: str,
        source_name: str,
        source_offset: int,
    ) -> bool:
        """Download and write data to a writable stream.

        Args:
          local_stream (BinaryIO): a writable stream.
          source_base_path (str): the remote base path, eg, the bucket name.
          source_name (str): the remote object name.
          source_offset (int, optional): where to start reading the remote data

        Returns:
          bool: True if no error occurred.
        """
        pass

    @classmethod
    @abstractmethod
    def scheme(cls) -> str:
        """Return the scheme name, eg, 's3' or 'file'.

        Returns:
          str: the scheme name.
        """
        pass

    @property
    def netloc(self) -> str:
        """Return the remote network address.

        Returns:
          str: the remote network address.
        """
        return self._netloc

    @property
    @abstractmethod
    def is_valid(self) -> bool:
        """Check if this remote instance is valid and usable.

        Returns:
          bool: True if this instance is valid.
        """
        pass
