import zmq
import time
import sys
from itertools import cycle
import consistent_hashing as ch
import hrw

def create_clients(servers):
    producers = {}
    context = zmq.Context()
    for server in servers:
        print(f"Creating a server connection to {server}...")
        producer_conn = context.socket(zmq.PUSH)
        producer_conn.bind(server)
        producers[server] = producer_conn
    return producers
    

def generate_data_round_robin(servers):
    print("Starting Round Robin...")
    producers = create_clients(servers)
    pool = cycle(producers.values())
    for num in range(10):
        data = { 'key': f'rr_key-{num}', 'value': f'rr_value-{num}' }
        print(f"Sending data:{data}")
        next(pool).send_json(data)
        time.sleep(1)
    print("Done")


def generate_data_consistent_hashing(servers):
    print("Starting Consistent Hashing...")
    consistentHashRing = ch.ConsistentHashRing(servers, 100)
    producers = create_clients(servers)
    for num in range(100):
        data = { 'key': f'key-{num}', 'value': f'value-{num}' }
        print(f"Sending data:{data}")
        server = consistentHashRing.assignServer(f'key-{num}')
        producers[server].send_json(data)
        # time.sleep(1)
    print("Done")
    
def generate_data_hrw_hashing(servers):
    print("Starting HRW Hashing...")
    rendezvousHashRing = hrw.RendezvousHashRing(servers)
    producers = create_clients(servers)
    for num in range(100):
        data = { 'key': f'key-{num}', 'value': f'value-{num}' }
        print(f"Sending data:{data}")
        server = rendezvousHashRing.assignServer(f'key-{num}')
        producers[server].send_json(data)
        # time.sleep(1)
    print("Done")
    
    
if __name__ == "__main__":
    servers = []
    num_server = 1
    if len(sys.argv) > 1:
        num_server = int(sys.argv[1])
        print(f"num_server={num_server}")
        
    for each_server in range(num_server):
        server_port = "200{}".format(each_server)
        servers.append(f'tcp://127.0.0.1:{server_port}')
        
    print("Servers:", servers)
    generate_data_round_robin(servers)
    generate_data_consistent_hashing(servers)
    generate_data_hrw_hashing(servers)
    
