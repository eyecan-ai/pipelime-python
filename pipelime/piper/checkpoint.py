import os
import os.path
import shutil
import typing as t
from contextlib import nullcontext
from pathlib import Path
from loguru import logger

import pydantic as pyd
from filelock import FileLock

from pipelime.choixe.utils.io import PipelimeTmp
from pipelime.utils.pydantic_types import NewPath


class Checkpoint:
    def create_lock(self, namespace: str) -> t.ContextManager:
        """Return a context manager that locks the given namespace
        for multiple data read/write. You should use it as follows:

            ```
            with checkpoint.create_lock("my_namespace") as lock:
                checkpoint.write_data("my_namespace", "key", value, lock)
                checkpoint.read_data("my_namespace", "key", default, lock)
            ```
        """
        return nullcontext()

    def add_asset(
        self,
        namespace: str,
        source: t.Union[str, Path],
        lock: t.Optional[t.ContextManager] = None,
    ):
        """Add a file or folder to the checkpoint."""

    def get_asset(
        self, namespace: str, name: str, lock: t.Optional[t.ContextManager] = None
    ) -> t.Optional[Path]:
        """Retrieve a file or folder from the checkpoint."""
        return None

    def write_data(
        self,
        namespace: str,
        key_path: str,
        value: t.Any,
        lock: t.Optional[t.ContextManager] = None,
    ):
        """Write data to the checkpoint."""

    def read_data(
        self,
        namespace: str,
        key_path: str,
        default: t.Any,
        lock: t.Optional[t.ContextManager] = None,
    ) -> t.Any:
        """Read data from the checkpoint."""
        return default

    def get_namespace(self, namespace: str) -> "CheckpointNamespace":
        return CheckpointNamespace(self, namespace)


class CheckpointNamespace:
    def __init__(self, checkpoint: Checkpoint = Checkpoint(), namespace: str = ""):
        self._checkpoint = checkpoint
        self._namespace = namespace

    @property
    def checkpoint(self) -> Checkpoint:
        return self._checkpoint

    @property
    def namespace(self) -> str:
        return self._namespace

    def create_lock(self) -> t.ContextManager:
        """Return a context manager that locks this namespace
        for multiple data read/write. You should use it as follows:

            ```
            ckpt_ns = checkpoint.get_namespace("my_namespace")
            with ckpt_ns.create_lock() as lock:
                ckpt_ns.write_data("key", value, lock)
                ckpt_ns.read_data("key", default, lock)
            ```
        """
        return self._checkpoint.create_lock(self._namespace)

    def add_asset(
        self, source: t.Union[str, Path], lock: t.Optional[t.ContextManager] = None
    ):
        """Add a file or folder to the checkpoint."""
        return self._checkpoint.add_asset(self._namespace, source, lock)

    def get_asset(
        self, name: str, lock: t.Optional[t.ContextManager] = None
    ) -> t.Optional[Path]:
        """Retrieve a file or folder from the checkpoint."""
        return self._checkpoint.get_asset(self._namespace, name, lock)

    def write_data(
        self, key_path: str, value: t.Any, lock: t.Optional[t.ContextManager] = None
    ):
        """Write data to the checkpoint."""
        return self._checkpoint.write_data(self._namespace, key_path, value, lock)

    def read_data(
        self, key_path: str, default: t.Any, lock: t.Optional[t.ContextManager] = None
    ) -> t.Any:
        """Read data from the checkpoint."""
        return self._checkpoint.read_data(self._namespace, key_path, default, lock)

    def get_namespace(self, namespace: str) -> "CheckpointNamespace":
        return CheckpointNamespace(self._checkpoint, self._namespace + "_" + namespace)


class LocalCheckpoint(Checkpoint, pyd.BaseModel):
    folder: t.Union[pyd.DirectoryPath, NewPath] = pyd.Field(
        ..., description="The folder where checkpoints are read/written"
    )
    try_link: bool = pyd.Field(
        True, description="Try to link assets instead of copying"
    )

    _temp_folder: Path = pyd.PrivateAttr()

    _assets_dir: t.ClassVar[str] = "__assets"
    _data_dir: t.ClassVar[str] = "__data"

    @pyd.validator("folder")
    def _validate_folder(cls, v):
        return v.resolve().absolute()

    def __init__(self, **data):
        super().__init__(**data)
        self._temp_folder = PipelimeTmp.make_subdir()
        logger.debug(f"Checkpoint folder: {self.folder}")

    def create_lock(self, namespace: str) -> t.ContextManager:
        """Return a context manager that locks the given namespace
        for multiple data read/write.
        """
        return FileLock(self._default_lock_file_name(namespace))

    def add_asset(
        self,
        namespace: str,
        source: t.Union[str, Path],
        lock: t.Optional[t.ContextManager] = None,
    ):
        """Add a file or folder to the checkpoint."""

        with self._get_lock(namespace, lock):
            source = Path(source)
            target = self.folder / self._assets_dir / namespace / source.name

            if target.exists() or not source.exists():
                return

            target.parent.mkdir(parents=True, exist_ok=True)

            if self.try_link:
                if source.is_dir():
                    for parent_dir, dirs, files in os.walk(source):
                        subdir = os.path.relpath(parent_dir, source)
                        trg_parent = os.path.join(target, subdir)
                        for d in dirs:
                            os.mkdir(os.path.join(trg_parent, d))
                        for f in files:
                            srcf = os.path.join(parent_dir, f)
                            trgf = os.path.join(trg_parent, f)
                            try:
                                os.link(srcf, trgf)
                            except Exception:
                                shutil.copy2(srcf, trgf)
                else:
                    try:
                        os.link(source, target)
                    except Exception:
                        shutil.copy2(source, target)
            elif source.is_dir():
                shutil.copytree(source, target)
            else:
                shutil.copy2(source, target)

    def get_asset(
        self, namespace: str, name: str, lock: t.Optional[t.ContextManager] = None
    ) -> t.Optional[Path]:
        """Retrieve a file or folder from the checkpoint."""

        with self._get_lock(namespace, lock):
            filepath = self.folder / self._assets_dir / namespace / name
            if not filepath.exists():
                return None
            return filepath

    def write_data(
        self,
        namespace: str,
        key_path: str,
        value: t.Any,
        lock: t.Optional[t.ContextManager] = None,
    ):
        from pipelime.items import YamlMetadataItem
        from pipelime.sequences import Sample

        with self._get_lock(namespace, lock):
            item_path = Path(
                self.folder
                / self._data_dir
                / (namespace + YamlMetadataItem.file_extensions()[0])
            )
            if item_path.exists():
                item = YamlMetadataItem(YamlMetadataItem(item_path)())
            else:
                item_path.parent.mkdir(parents=True, exist_ok=True)
                item = YamlMetadataItem({})

            key_path = namespace if not key_path else f"{namespace}.{key_path}"
            sample = Sample({namespace: item}).deep_set(key_path, value)
            sample[namespace].serialize(item_path)

    def read_data(
        self,
        namespace: str,
        key_path: str,
        default: t.Any,
        lock: t.Optional[t.ContextManager] = None,
    ) -> t.Any:
        from pipelime.items import YamlMetadataItem
        from pipelime.sequences import Sample

        with self._get_lock(namespace, lock):
            item_path = Path(
                self.folder
                / self._data_dir
                / (namespace + YamlMetadataItem.file_extensions()[0])
            )
            if not item_path.exists():
                return default

            key_path = namespace if not key_path else f"{namespace}.{key_path}"
            sample = Sample({namespace: YamlMetadataItem(item_path)})
            return sample.deep_get(key_path, default)

    def _default_lock_file_name(self, namespace: str) -> str:
        return str(Path(self._temp_folder) / (namespace + ".~lock"))

    def _get_lock(self, namespace: str, lock: t.Optional[t.ContextManager] = None):
        if lock is None:
            return self.create_lock(namespace)
        return lock
