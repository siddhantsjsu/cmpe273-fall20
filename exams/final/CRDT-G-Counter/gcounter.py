import zmq
from multiprocessing import Process
import threading
from time import sleep

class GCounter(object):
    def __init__(self, i, n, zmq_port):
        self.i = i # server id
        self.n = n # number of servers
        self.xs = [0] * n
        self.zmq_port = zmq_port
        # You can assume all servers are running at host tcp://127.0.0.1:xxxx
        # Start a new ZMQ server instance or process.
        self.context = zmq.Context()
        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.connect(f"tcp://localhost:{self.zmq_port}")
        threading.Thread(target=self._listen_merge_request_from_peer).start()

    def query(self):
        return sum(self.xs)

    def add(self, x):
        assert x >= 0
        self.xs[self.i] += x

    def merge(self, c):
        zipped = zip(self.xs, c['xs'])
        self.xs = [max(x, y) for (x, y) in zipped]
    
    def to_dict(self):
        return {
            'i': self.i,
            'n': self.n,
            'xs': self.xs
        }
        
    def _listen_merge_request_from_peer(self):
        print(f"######Listening for Server No.{self.i}")
        # receive merge request from peer.
        while(True):
            syncReq = self.receiver.recv_json()
            # print("Input Received: ", syncReq)
            self.merge(syncReq)

    def sync_to_peer(self, zmq_peer_port):
        peer = f"tcp://127.0.0.1:{zmq_peer_port}"
        print(f"Syncing to peer:{peer}")
        # send merge request to peer.
        sender = self.context.socket(zmq.PUSH)
        sender.bind(peer)
        sender.send_json(self.to_dict())
        sleep(1)
