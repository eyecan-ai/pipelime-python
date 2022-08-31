import time

from pipelime.piper.progress.listener.callbacks.rich_table import (
    RichTableListenerCallback,
)
from pipelime.piper.progress.model import OperationInfo, ProgressUpdate

# This test will not perform any asserts, it will just call the methods and ensure
# no exception is thrown. TODO: Find a way to capture rich live object printed from
# another thread.


class TestRichTableListenerCallback:
    def test_callback(self):
        callback = RichTableListenerCallback()
        op_infos = [
            OperationInfo(
                token="token", node=f"node{i}", chunk=0, total=4, message="message"
            )
            for i in range(3)
        ]

        try:
            # Start the callback
            callback.on_start()

            time.sleep(0.1)

            # Update the callback
            for op_info in op_infos:
                for i in range(op_info.total):
                    callback.on_update(ProgressUpdate(op_info=op_info, progress=i))

        finally:
            # Stop the callback
            callback.on_stop()
