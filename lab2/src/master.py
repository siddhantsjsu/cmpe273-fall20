import zmq
import time

try:
    raw_input
except NameError:
    # Python 3
    raw_input = input

context = zmq.Context()

# Socket to send messages on
sender = context.socket(zmq.PUSH)
sender.bind("tcp://*:5557")

# Socket with direct access to the sink: used to synchronize start of batch
sink = context.socket(zmq.PUSH)
sink.connect("tcp://localhost:5558")

print("Press Enter when the workers are ready: ")
_ = raw_input()
print("Sending tasks to workers...")

# The first message is "0" and signals start of batch
sink.send(b'0')

# Send 10001 tasks
for task_nbr in range(10001):

    # Number from range 0 to 10000
    sender.send_string(u'%i' % task_nbr)

# Give 0MQ time to deliver
time.sleep(1)