import json
import os.path
import tempfile
import uuid
import weakref
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


def load(path: Path) -> Any:
    """Loads an object from a file with a supported markup format.
    Supported formats include:
    - yaml
    - json

    Args:
        path (Path): Path to the file to load.

    Returns:
        Any: The loaded object.
    """
    with open(path, "r") as fd:
        ext = path.suffix
        if ext == ".json":
            return json.load(fd)
        else:
            return yaml.safe_load(fd)


def dump(obj: Any, path: Path) -> None:
    """Dumps an object to a file with a supported markup format.
    Supported formats include:
    - yaml
    - json
    Args:
        obj (Any): The object to dump.
        path (Path): Path to the file to write.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as fd:
        ext = path.suffix
        if ext == ".json":
            json.dump(obj, fd)
        else:
            yaml.safe_dump(obj, fd, sort_keys=False)


class PipelimeTmp:
    SESSION_TMP_DIR: Optional[Path] = None

    @staticmethod
    def base_dir() -> Path:
        """Base directory for temporary directories."""
        return Path(tempfile.gettempdir())

    @staticmethod
    def prefix() -> str:
        """Prefix for temporary directories."""
        return "pipelime-of-"

    @staticmethod
    def current_user() -> str:
        """Current user name."""
        return Path.home().stem

    @staticmethod
    def user_prefix(user: str) -> str:
        """Prefix with user name for temporary directories."""
        return f"{PipelimeTmp.prefix()}{user}-"

    @staticmethod
    def current_user_prefix() -> str:
        """Prefix with user name for temporary directories."""
        return PipelimeTmp.user_prefix(PipelimeTmp.current_user())

    @staticmethod
    def make_session_dir() -> Path:
        """Creates a temporary directory. Unique for each session.

        Returns:
            Path: Path to the temporary directory.
        """
        if not PipelimeTmp.SESSION_TMP_DIR:
            PipelimeTmp.SESSION_TMP_DIR = Path(
                tempfile.mkdtemp(prefix=PipelimeTmp.current_user_prefix())
            )
        return PipelimeTmp.SESSION_TMP_DIR

    @staticmethod
    def make_subdir(subdir: Optional[str] = None) -> Path:
        """Creates a temporary subdirectory and returns its path.

        Returns:
            Path: Path to the temporary subdirectory.
        """
        path = PipelimeTmp.make_session_dir()
        if subdir:
            path /= subdir
            path.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def get_temp_dirs() -> Dict[str, List[str]]:
        """Gets the temporary directories of all users.

        Returns:
            Dict[str, str]: Dict of temporary directories for each user.
        """
        from glob import iglob

        user_dirs: Dict[str, List[str]] = {}
        for d in iglob(str(PipelimeTmp.base_dir() / (PipelimeTmp.prefix() + "*"))):
            if os.path.isdir(d):
                names = os.path.basename(d).split("-")
                if len(names) >= 3:
                    user_dirs.setdefault(names[2], []).append(d)
        return user_dirs


class PipelimeTemporaryDirectory(tempfile.TemporaryDirectory):
    """A tempfile.TemporaryDirectory within the session temporary directory."""

    def __init__(self, name: Optional[str] = None):
        self.name = PipelimeTmp.make_subdir(name or uuid.uuid1().hex)
        self._finalizer = weakref.finalize(
            self,
            self._cleanup,  # type: ignore
            str(self.name),
            warn_message="Implicitly cleaning up {!r}".format(self),
        )
