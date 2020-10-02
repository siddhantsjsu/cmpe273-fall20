
import sys
import time
import zmq
import math

context = zmq.Context()

# Socket to receive messages on
receiver = context.socket(zmq.PULL)
receiver.connect("tcp://localhost:5557")

# Socket to send messages to
sender = context.socket(zmq.PUSH)
sender.connect("tcp://localhost:5558")

print("Worker Ready")
# Process tasks forever
while True:
    s = receiver.recv()

    # Simple progress indicator for the viewer
    print("Input Received: ", s.decode())

    # Do the work
    result = math.sqrt(int(s))
    print("Result: ",result)

    # Send results to sink
    sender.send(str(result).encode())