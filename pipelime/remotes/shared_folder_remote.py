import hashlib
import os
import shutil
import typing as t
from pathlib import Path, WindowsPath

from loguru import logger

from pipelime.remotes.base import BaseRemote, NetlocData


class SharedFolderRemote(BaseRemote):
    _PL_FOLDER_ = ".pl"
    _TAGS_FILE_ = "tags.json"
    _HASH_FN_KEY_ = "__HASH_FN__"
    _DEFAULT_HASH_FN_ = "blake2b"

    def __init__(self, netloc_data: NetlocData):
        """Filesystem-based remote.

        :param netloc_data: the network data info.
        :type netloc: NetlocData
        """
        if netloc_data.host == "localhost" or netloc_data.host == "127.0.0.1":
            netloc_data.host = ""
        super().__init__(netloc_data)

    def _maybe_create_root(self, target_base_path: Path):
        if not target_base_path.exists():
            logger.info(f"Creating folder tree '{target_base_path}'.")
            target_base_path.mkdir(parents=True, exist_ok=True)
        pldir = target_base_path / self._PL_FOLDER_
        if not pldir.is_dir():
            pldir.mkdir(parents=True, exist_ok=True)

    def _get_hash_fn(self, target_base_path: str) -> t.Any:
        if self.is_valid:
            try:
                import json

                target_root = self._make_file_path(target_base_path, "")
                self._maybe_create_root(target_root)

                tags = {}
                try:
                    with open(
                        target_root / self._PL_FOLDER_ / self._TAGS_FILE_, "r"
                    ) as jtags:
                        tags = json.load(jtags)
                except Exception:
                    pass

                hash_fn_name = tags.get(self._HASH_FN_KEY_)

                # try-get
                if isinstance(hash_fn_name, str) and len(hash_fn_name) > 0:
                    try:
                        hash_fn = getattr(hashlib, hash_fn_name)
                        return hash_fn()
                    except AttributeError:
                        pass

                tags[self._HASH_FN_KEY_] = self._DEFAULT_HASH_FN_
                with open(
                    target_root / self._PL_FOLDER_ / self._TAGS_FILE_, "w"
                ) as jtags:
                    json.dump(tags, jtags)

                hash_fn = getattr(hashlib, self._DEFAULT_HASH_FN_)
                return hash_fn()
            except Exception as exc:
                logger.debug(str(exc))
        return None

    def target_exists(self, target_base_path: str, target_name: str) -> bool:
        if self.is_valid:
            return self._make_file_path(target_base_path, target_name).exists()
        return False

    def _upload(
        self,
        local_stream: t.BinaryIO,
        local_stream_size: int,
        target_base_path: str,
        target_name: str,
    ) -> bool:
        if self.is_valid:
            try:
                target_full_path = self._make_file_path(target_base_path, target_name)
                self._maybe_create_root(target_full_path.parent)

                with target_full_path.open("wb") as target:
                    shutil.copyfileobj(local_stream, target)

                return True
            except Exception as exc:
                logger.debug(str(exc))
                return False

        return False

    def _download(
        self,
        local_stream: t.BinaryIO,
        source_base_path: str,
        source_name: str,
        source_offset: int,
    ) -> bool:
        if self.is_valid:
            try:
                source_full_path = self._make_file_path(source_base_path, source_name)
                if not source_full_path.is_file():
                    logger.debug(f"File '{source_full_path}' does not exist.")
                    return False

                with source_full_path.open("rb") as source:
                    source.seek(source_offset)
                    shutil.copyfileobj(source, local_stream)

                return True
            except Exception as exc:
                logger.debug(str(exc))
                return False

        return False

    def _make_file_path(self, file_path: str, file_name: str) -> Path:
        full_path = Path(file_path) / Path(file_name)
        if self.netloc:
            return Path(
                "{0}{0}{1}{0}{2}".format(os.path.sep, self.netloc, str(full_path))
            )
        elif isinstance(full_path, WindowsPath):
            return full_path
        else:
            return Path(os.path.sep + str(full_path))

    @classmethod
    def scheme(cls) -> str:
        return "file"

    @property
    def is_valid(self) -> bool:
        return True
