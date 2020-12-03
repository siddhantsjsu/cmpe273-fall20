import zmq
import time
import sys
from itertools import cycle
import consistent_hashing as ch
import hrw
import consul

def create_clients(servers):
    producers = {}
    context = zmq.Context()
    for server in servers:
        print(f"Creating a server connection to {server}...")
        producer_conn = context.socket(zmq.REQ)
        producer_conn.connect(server)
        producers[server] = producer_conn
    return producers
    

def generate_data_round_robin(servers):
    print("Starting Round Robin...")
    producers = create_clients(servers)
    pool = cycle(producers.values())
    for num in range(10):
        data = { 'op': 'PUT', 'key': f'key-{num}', 'value': f'value-{num}' }
        print(f"Sending data:{data}")
        current = next(pool)
        current.send_json(data)
        res = current.recv_json()
        print(res)
        time.sleep(1)
    current_node = next(pool)
    data = { 'op': 'GET_ALL' }
    print(f"Sending data:{data}")
    current_node.send_json(data)
    print(current_node.recv_json())
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

def consulListener(consulClient, numServers):
    time.sleep(10)
    print("Listening for cluster updates from Consul...")
    while True:
        index, data = consulClient.kv.get('python/servers/count')
        consulNumServers = int(data.get("Value"))
        print("Cluster Size: ", numServers)
        print("Services Registered on Consul: ", len(consulClient.agent.services().keys()))
        time.sleep(30)
    
    
if __name__ == "__main__":
    servers = []
    num_server = 1
    consulClient = consul.Consul()
    registeredServers = consulClient.agent.services().values()
    # print(registeredServers)
    numServers = len(registeredServers)
    for regServer in registeredServers:
        servers.append("tcp://" + regServer["Address"] + ":" + str(regServer["Port"]))
    # Process(target=consulListener, args=(consulClient,numServers,)).start()  
    # for each_server in range(num_server):
    #     server_port = "200{}".format(each_server)
    #     servers.append(f'tcp://127.0.0.1:{server_port}')
    
    print("Servers:", servers)
    generate_data_round_robin(servers)
    # generate_data_consistent_hashing(servers)
    # generate_data_hrw_hashing(servers)
    
