import json
import time

import pytest
import zmq

from pipelime.piper.progress.model import OperationInfo, ProgressUpdate
from pipelime.piper.progress.tracker.zmq import ZmqTrackCallback


@pytest.fixture(scope="function")
def zmq_socket() -> zmq.Socket:
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://localhost:5556")
    socket.subscribe(b"token")
    time.sleep(0.2)
    yield socket
    socket.close()


class TestLoguruTrackCallback:
    def test_on_start(self, zmq_socket: zmq.Socket):
        # Create callback
        callback = ZmqTrackCallback()

        # Create a progress update
        op_info = OperationInfo(
            token="token", node="node", chunk=1, message="msg", total=10
        )
        progress_update = ProgressUpdate(op_info=op_info, progress=0, finished=False)

        def _check_zmq():
            token, msg = zmq_socket.recv_multipart()
            prog = ProgressUpdate.parse_obj(json.loads(msg.decode("utf-8")))
            assert token.decode() == "token"
            assert prog == progress_update

        # Assert that the loguru sink was called on start hook
        callback.on_start(progress_update)
        _check_zmq()

        # Assert that the loguru sink was called on advance hook
        callback.on_advance(progress_update)
        _check_zmq()

        # Assert that the loguru sink was called on finish hook
        callback.on_finish(progress_update)
        _check_zmq()
