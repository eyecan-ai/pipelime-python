import time
from threading import Thread

import zmq

from pipelime.piper.progress.listener.receiver.zmq import ZMQProgressReceiver
from pipelime.piper.progress.model import OperationInfo, ProgressUpdate


class TestZMQProgressReceiver:
    N_PACKETS = 10

    def _sending_thread(self, op_info: OperationInfo) -> None:
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.bind(f"tcp://*:{ZMQProgressReceiver.DEFAULT_PORT_NUMBER}")
        time.sleep(1)
        for i in range(self.N_PACKETS):
            token = "token" if i % 2 == 0 else "token2"
            prog = ProgressUpdate(op_info=op_info, progress=i // 2)
            socket.send_multipart([token.encode(), prog.json().encode()])
            time.sleep(0.1)
        socket.close()

    def test_receiver(self):
        op_info = OperationInfo(
            token="token",
            node="node",
            chunk=0,
            message="Message",
            total=self.N_PACKETS,
        )
        receiver = ZMQProgressReceiver("token")

        thread = Thread(target=self._sending_thread, args=[op_info])
        thread.start()

        try:
            packets = []
            t = time.time()
            while len(packets) < self.N_PACKETS and time.time() - t < 10:
                prog = next(receiver)
                if prog is not None:
                    packets.append(prog)
                    print(prog)

            assert len(packets) == self.N_PACKETS // 2
            for i, prog in enumerate(packets):
                assert isinstance(prog, ProgressUpdate)
                assert prog.op_info == op_info
                assert prog.progress == i
        finally:
            thread.join()
