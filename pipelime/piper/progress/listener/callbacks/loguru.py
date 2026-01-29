import ast
import json
import os
from typing import ClassVar

from loguru import logger

from pipelime.piper.progress.listener.base import ListenerCallback
from pipelime.piper.progress.model import ProgressUpdate


class LoguruListenerCallback(ListenerCallback):
    """Listener callback that sends progress updates to loguru.

    Args:
        show_token: Whether to include the token in the logged data.
        log_extras: Extra fields to bind to the logger. Can also be set via
            environment variables with the prefix PIPELIME_PROGRESS_LOG_EXTRA_
            (e.g., PIPELIME_PROGRESS_LOG_EXTRA_key=value). Environment variables
            take precedence over this argument.
        level: Logging level to use. Can also be set via the environment variable
            PIPELIME_PROGRESS_LOG_LEVEL. Defaults to "DEBUG".
    """

    LOG_LEVEL_ENV_VAR: ClassVar[str] = "PIPELIME_PROGRESS_LOG_LEVEL"
    LOG_EXTRA_ENV_VAR_PREFIX: ClassVar[str] = "PIPELIME_PROGRESS_LOG_EXTRA_"

    @classmethod
    def update_data(cls, data: dict, prog: ProgressUpdate, show_token: bool) -> None:
        """Update the data dictionary with the progress update"""
        root = data.setdefault(prog.op_info.token, dict()) if show_token else data

        chunk_data = root.setdefault(prog.op_info.node, dict()).setdefault(
            str(prog.op_info.chunk), dict()
        )
        chunk_data["message"] = prog.op_info.message
        chunk_data["total"] = prog.op_info.total
        chunk_data["progress"] = prog.progress
        chunk_data["finished"] = prog.finished

    def __init__(
        self,
        show_token: bool = False,
        log_extras: dict = {},
        level: str = "DEBUG",
    ) -> None:
        super().__init__(show_token)

        self._data: dict = dict()

        # collect log extras from environment variables
        env_extras = {}
        for key, value in os.environ.items():
            if key.startswith(self.LOG_EXTRA_ENV_VAR_PREFIX):
                extra_key = key[len(self.LOG_EXTRA_ENV_VAR_PREFIX) :].lower()
                try:
                    env_extras[extra_key] = ast.literal_eval(value)
                except (ValueError, SyntaxError):
                    env_extras[extra_key] = value

        # merge env extras with provided extras (env vars take precedence)
        self._log_extras = {**log_extras, **env_extras}

        # determine log level (env var takes precedence)
        self._level = os.getenv(self.LOG_LEVEL_ENV_VAR, level).upper()

    def _log_data(self) -> None:
        if self._data:
            logger.bind(**self._log_extras).log(self._level, json.dumps(self._data))

    def on_update(self, prog: ProgressUpdate) -> None:
        """Called when a progress update is received"""
        LoguruListenerCallback.update_data(self._data, prog, self.show_token)
        self._log_data()

    def on_stop(self) -> None:
        """Called when the listener stops - ensures final progress is sent"""
        self._log_data()
