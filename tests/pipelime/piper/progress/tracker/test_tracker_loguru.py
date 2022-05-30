from io import TextIOWrapper, BytesIO
from typing import TextIO

import pytest
from pipelime.piper.progress.model import ProgressUpdate, OperationInfo
from pipelime.piper.progress.tracker.loguru import LoguruTrackCallback

from loguru import logger


@pytest.fixture(scope="function")
def loguru_sink() -> TextIO:
    textio = TextIOWrapper(BytesIO(), encoding="utf-8")
    sinkid = logger.add(sink=textio, level="INFO")
    yield textio
    logger.remove(sinkid)


class TestLoguruTrackCallback:
    def test_on_start(self, loguru_sink: TextIO):
        # Create callback
        callback = LoguruTrackCallback()

        # Create a progress update
        op_info = OperationInfo(
            token="token", node="node", chunk=1, message="msg", total=10
        )
        progress_update = ProgressUpdate(op_info=op_info, progress=0, finished=False)

        def _check_log():
            loguru_sink.seek(0)
            assert "token" in loguru_sink.readlines()[-1]

        # Assert that the loguru sink was called on start hook
        callback.on_start(progress_update)
        _check_log()

        # Assert that the loguru sink was called on advance hook
        callback.on_advance(progress_update)
        _check_log()

        # Assert that the loguru sink was called on finish hook
        callback.on_finish(progress_update)
        _check_log()
