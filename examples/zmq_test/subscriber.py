import json
import zmq

context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect("tcp://localhost:5556")
socket.subscribe(b"TOKEN")


while True:
    topic, messagedata = socket.recv_multipart()
    messagedata = json.loads(messagedata.decode())
    print(topic, messagedata)
