import zmq
import time
import sys
from itertools import cycle
import consistent_hashing as ch
import hrw
import consul
from  multiprocessing import Process

def create_clients(servers):
    producers = {}
    context = zmq.Context()
    for server in servers:
        print(f"Creating a server connection to {server}...")
        producer_conn = context.socket(zmq.REQ)
        producer_conn.connect(server)
        producers[server] = producer_conn
    return producers
    

def generate_data_round_robin(servers, producers):
    print("Generting and Sending Key Value Pairs....")
    pool = cycle(producers.values())
    for num in range(10):
        data = { 'op': 'PUT', 'key': f'key-{num}', 'value': f'value-{num}' }
        print(f"Sending data:{data}")
        current = next(pool)
        current.send_json(data)
        res = current.recv_json()
        print(res)
        time.sleep(1)
    print("Done")

def demoRoundRobin(servers):
    print("Press Enter to start Round Robin Demo: ")
    _ = input()
    print("Starting Round Robin...")
    producers = create_clients(servers)
    generate_data_round_robin(servers, producers)
   
    print("Press Enter to Display Data on Servers ")
    _ = input()
    for server in servers:
        getAllDataFromServer(server,producers)
   
    print("Press Enter to end Round Robin Demo: ")
    _ = input()
    print("Deleting all data on servers....")
    for server in servers:
        deleteAllDataOnServer(server,producers)


def getAllDataFromServer(server, producers):
    producer = producers.get(server)
    msg = { 'op': 'GET_ALL' }
    producer.send_json(msg)
    data = producer.recv_json()
    print(f"Data on Server - {server} - Size: {len(data['collections'])}")
    print(data)

def deleteAllDataOnServer(server,producers):
    producer = producers.get(server)
    msg = { 'op': 'DELETE_ALL' }
    producer.send_json(msg)
    producer.recv_json()

def generate_data_hrw_hashing(servers, producers):
    print("Starting HRW Hashing...")
    rendezvousHashRing = hrw.RendezvousHashRing(servers)
    for num in range(100):
        data = { 'op': 'PUT', 'key': f'key-{num}', 'value': f'value-{num}' }
        print(f"Sending data:{data}")
        server = rendezvousHashRing.assignServer(f'key-{num}')
        producers[server].send_json(data)
        res =  producers[server].recv_json()
        print(res)
        # time.sleep(1)
    print("Done")

def demoHRW(servers):
    print("Press Enter to start HRW Demo: ")
    _ = input()
    print("Starting HRW...")
    producers = create_clients(servers)
    generate_data_hrw_hashing(servers, producers)
    
    print("Press Enter to Display Data on Servers ")
    _ = input()
    for server in servers:
        getAllDataFromServer(server,producers)
   
    print("Press Enter to end HRW Demo: ")
    _ = input()
    print("Deleting all data on servers....")
    for server in servers:
        deleteAllDataOnServer(server,producers)

def generate_data_consistent_hashing(servers, producers):
    print("Starting Consistent Hashing...")
    consistentHashRing = ch.ConsistentHashRing(servers, producers, 100)
    for num in range(100):
        data = { 'op': 'PUT', 'key': f'key-{num}', 'value': f'value-{num}' }
        print(f"Sending data:{data}")
        server = consistentHashRing.assignServer(f'key-{num}')
        producers[server].send_json(data)
        res =  producers[server].recv_json()
        print(res)
        # time.sleep(1)
    print("Done")
    return consistentHashRing

def demoCH(servers):
    print("Press Enter to start Consistent Hashing Demo: ")
    _ = input()
    print("Starting Consistent Hashing demo...")
    producers = create_clients(servers)
    consistentHashRing = generate_data_consistent_hashing(servers, producers)
    
    print("Press Enter to Display Data on Servers ")
    _ = input()
    for server in servers:
        getAllDataFromServer(server,consistentHashRing._producers)
    
    print("Press Enter to Add node:")
    _ = input()
    newServerAddress = "tcp://127.0.0.1:2004"
    print("Registering Server to Consul with Address=127.0.0.1 and Port=2004")
    consulClient.agent.service.register("Server-4", service_id="2004", address="127.0.0.1", port=2004)
    context = zmq.Context()
    producer_conn = context.socket(zmq.REQ)
    producer_conn.connect(newServerAddress)
    consistentHashRing._producers[newServerAddress] = producer_conn
    servers.append(newServerAddress)
    time.sleep(10)
    add_node_for_consistent_hashing(consistentHashRing, newServerAddress)
    
    print("Press Enter to Display Data on Servers ")
    _ = input()
    for server in servers:
        getAllDataFromServer(server,consistentHashRing._producers)

    print("Press Enter to Delete node:")
    _ = input()
    consulClient.agent.service.deregister(service_id="2004")
    
    print("Press Enter to end Consistent Hashing Demo: ")
    _ = input()
    print("Deleting all data on servers....")
    for server in servers:
        deleteAllDataOnServer(server,consistentHashRing._producers)

def add_node_for_consistent_hashing(consistentHashRing,additionalServers):
    consistentHashRing.addNode(additionalServers)
    
    
if __name__ == "__main__":
    servers = []
    num_server = 1
    consulClient = consul.Consul()
    registeredServers = consulClient.agent.services().values()
    for regServer in registeredServers:
        address = "tcp://" + regServer["Address"] + ":" + str(regServer["Port"])
        servers.append(address)
    
    print("Servers:", servers)
    # demoRoundRobin(servers)
    # demoHRW(servers)
    demoCH(servers)
    
