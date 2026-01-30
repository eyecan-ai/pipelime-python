import json
from io import BytesIO, TextIOWrapper
from typing import TextIO

import pytest
from loguru import logger

from pipelime.piper.progress.listener.callbacks.loguru import LoguruListenerCallback
from pipelime.piper.progress.model import OperationInfo, ProgressUpdate


@pytest.fixture(scope="function")
def loguru_sink():
    textio = TextIOWrapper(BytesIO(), encoding="utf-8")
    sinkid = logger.add(sink=textio, level="TRACE", serialize=True)
    yield textio
    logger.remove(sinkid)


class TestLoguruListenerCallback:
    @pytest.mark.parametrize("show_token", [True, False])
    @pytest.mark.parametrize(
        "arg_level,env_level,expected_level",
        [
            ("CRITICAL", None, "CRITICAL"),
            (None, "INFO", "INFO"),
            ("WARNING", "ERROR", "ERROR"),
            (None, None, "DEBUG"),
        ],
    )
    @pytest.mark.parametrize(
        "arg_extras,env_extras,expected_extras",
        [
            ({}, {}, {}),
            (
                {"number": 42.16, "bool": True, "info": "something"},
                {},
                {"number": 42.16, "bool": True, "info": "something"},
            ),
            (
                {"string": "abc"},
                {"PIPELIME_PROGRESS_LOG_EXTRA_integer": "123"},
                {"string": "abc", "integer": 123},
            ),
            (
                {"key1": "value1"},
                {"PIPELIME_PROGRESS_LOG_EXTRA_key1": "value2"},
                {"key1": "value2"},
            ),
        ],
    )
    def test_callback(
        self,
        loguru_sink: TextIO,
        monkeypatch: pytest.MonkeyPatch,
        show_token: bool,
        arg_level: str | None,
        env_level: str | None,
        expected_level: str,
        arg_extras: dict,
        env_extras: dict,
        expected_extras: dict,
    ) -> None:
        # set environment variables
        if env_level is not None:
            monkeypatch.setenv(LoguruListenerCallback.LOG_LEVEL_ENV_VAR, env_level)
        for key, value in env_extras.items():
            monkeypatch.setenv(key, value)

        # create callback
        callback = LoguruListenerCallback(
            show_token=show_token,
            level=arg_level or "DEBUG",
            log_extras=arg_extras,
        )

        # create progress updates
        op_info = OperationInfo(
            token="token", node="node", chunk=1, message="msg", total=10
        )
        progress_updates = [
            ProgressUpdate(op_info=op_info, progress=0, finished=False),
            ProgressUpdate(op_info=op_info, progress=5, finished=False),
            ProgressUpdate(op_info=op_info, progress=10, finished=True),
        ]

        def _get_last_log_data() -> dict:
            # read the last log entry
            loguru_sink.seek(0)
            lines = loguru_sink.readlines()
            log = json.loads(lines[-1])["record"]

            # verify log level and extras
            assert log["level"]["name"] == expected_level
            for key, value in expected_extras.items():
                assert log["extra"][key] == value

            # return the logged message data
            # (it should be the json dump of the progress data)
            return json.loads(log["message"])

        expected_data = {}

        # send progress updates and verify logs
        for pu in progress_updates:
            callback.on_update(pu)
            last_log_data = _get_last_log_data()
            LoguruListenerCallback.update_data(expected_data, pu, show_token=show_token)
            assert last_log_data == expected_data

    def test_on_stop(self, loguru_sink: TextIO) -> None:
        callback = LoguruListenerCallback()

        # call on_stop without prior updates
        callback.on_stop()

        # read the log entries
        loguru_sink.seek(0)
        lines = loguru_sink.readlines()
        # there should be no log entries
        assert len(lines) == 0

        # call on_update and then on_stop
        op_info = OperationInfo(
            token="token", node="node", chunk=1, message="msg", total=10
        )
        progress_update = ProgressUpdate(op_info=op_info, progress=10, finished=True)
        callback.on_update(progress_update)
        callback.on_stop()

        # read the log entries
        loguru_sink.seek(0)
        lines = loguru_sink.readlines()
        # there should be two log entries: one for the update and one for the stop
        assert len(lines) == 2
