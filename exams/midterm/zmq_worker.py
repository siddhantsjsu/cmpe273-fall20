import zmq
from  multiprocessing import Process

def voting_station_worker():
    context = zmq.Context()
    receiver = context.socket(zmq.PULL)
    receiver.connect("tcp://127.0.0.1:4000")
    
    result_sender = context.socket(zmq.PUSH)
    result_sender.connect("tcp://127.0.0.1:3000")
    
    msg = receiver.recv_json()
    region = msg['region']
    print(f'region={region} to count votes')
    result = {}
    filename = "votes/"
    # scan file and count votes
    print(f'Counting {region}...')
    filename = filename + region + ".csv"
    file = open(filename,'r')
    lines = file.readlines()
    if len(lines) > 0:
        result['region'] = region
        for line in lines:
            if line.startswith('x'):
                result['x'] = result.get('x',0) + 1
            elif line.startswith('y'):
                result['y'] = result.get('y',0) + 1
    else:
        result = {
            'region': region,
            'x': 0,
            'y': 0
        }
    
    print(f'result={result}')
    result_sender.send_json(result)
    print('Finished the worker')
    
    
if __name__ == "__main__":
    voting_station_worker()