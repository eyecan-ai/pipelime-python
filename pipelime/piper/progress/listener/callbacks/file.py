import typing as t
from pathlib import Path
from enum import Enum
from loguru import logger
from pipelime.piper.progress.listener.base import ListenerCallback
from pipelime.piper.progress.model import ProgressUpdate


class FileListenerCallback(ListenerCallback):
    """Listener callback writing the progress to a JSON/YAML file."""

    class Format(Enum):
        AUTO = "auto"
        JSON = "json"
        YAML = "yaml"

    def __init__(
        self,
        filename: t.Union[str, Path] = "progress.json",
        format: Format = Format.AUTO,
        show_token: bool = False,
    ) -> None:
        super().__init__(show_token)
        self._filename = Path(filename)
        if format == self.Format.AUTO:
            if self._filename.suffix == ".json":
                format = self.Format.JSON
            elif self._filename.suffix in (".yaml", ".yml"):
                format = self.Format.YAML
            else:
                format = self.Format.JSON
                logger.warning(
                    f"{self.__class__.__name__}: "
                    f"unknown file extension for {filename}, using JSON format"
                )
        if format == self.Format.YAML:
            import yaml

            self._dump = yaml.safe_dump
            self._load = yaml.safe_load
        else:
            import json

            self._dump = json.dump
            self._load = json.load

        self._filename.touch()

        logger.info(f"Writing progress to {filename}")

    def on_update(self, prog: ProgressUpdate) -> None:
        """Called when a progress update is received"""
        root = (
            self._data.setdefault(prog.op_info.token, {})
            if self.show_token
            else self._data
        )

        chunk_data = root.setdefault(prog.op_info.node, {}).setdefault(
            prog.op_info.chunk, {}
        )
        chunk_data["message"] = prog.op_info.message
        chunk_data["total"] = prog.op_info.total
        chunk_data["progress"] = prog.progress
        chunk_data["finished"] = prog.finished

        with self._filename.open("w") as f:
            self._dump(self._data, f)
