import json
import time

import pytest
import zmq

from pipelime.piper.progress.model import OperationInfo, ProgressUpdate
from pipelime.piper.progress.tracker.zmq import ZmqTrackCallback


@pytest.fixture(scope="function")
def zmq_socket():
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(f"tcp://localhost:{ZmqTrackCallback.DEFAULT_PORT_NUMBER}")
    socket.subscribe(b"token")
    yield socket
    socket.close()


class TestZmqTrackCallback:
    def test_callback(self, zmq_socket: zmq.Socket):
        # Create callback
        callback = ZmqTrackCallback()

        # Create a progress update
        op_info = OperationInfo(
            token="token", node="node", chunk=1, message="msg", total=10
        )
        progress_update = ProgressUpdate(op_info=op_info, progress=0, finished=False)

        def _check_zmq():
            t0 = time.time()
            while time.time() - t0 < 10:
                try:
                    token, msg = zmq_socket.recv_multipart(flags=zmq.NOBLOCK)
                    prog = ProgressUpdate.parse_obj(json.loads(msg.decode("utf-8")))
                    assert token.decode() == "token"
                    assert prog == progress_update
                    return
                except zmq.ZMQError:
                    pass
            assert False

        callback.update(progress_update)
        _check_zmq()
