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
    data = { 'op': 'GET_ALL' }
    producer.send_json(data)
    print(f"Data on Server - {server}")
    print(producer.recv_json())

def deleteAllDataOnServer(server,producers):
    producer = producers.get(server)
    data = { 'op': 'DELETE_ALL' }
    producer.send_json(data)
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

def generate_data_consistent_hashing(servers):
    print("Starting Consistent Hashing...")
    consistentHashRing = ch.ConsistentHashRing(servers, 100)
    producers = create_clients(servers)
    for num in range(100):
        data = { 'op': 'PUT', 'key': f'key-{num}', 'value': f'value-{num}' }
        print(f"Sending data:{data}")
        server = consistentHashRing.assignServer(f'key-{num}')
        producers[server].send_json(data)
        res =  producers[server].recv_json()
        print(res)
        # time.sleep(1)
    print("Done")

def add_node_for_consistent_hashing(additionalServers):
    print(additionalServers)
    for server in additionalServers.values():
        pass
    
def consulListener(consulClient, serverAddressMap):
    time.sleep(10)
    print("Listening for cluster updates from Consul...")
    numInitialServers = len(serverAddressMap.keys())
    while True:
        # index, data = consulClient.kv.get('python/servers/count')
        # consulNumServers = int(data.get("Value"))
        # # print("Cluster Size: ", consulNumServers)
        consulServers = consulClient.agent.services().values()
        print("Servers Registered on Consul: ", len(consulServers))
        if numInitialServers < len(consulServers):
            print("****New Servers Discovered on Consul****")
            additionalServers = getAdditionalServers(serverAddressMap, consulServers)
            add_node_for_consistent_hashing(additionalServers)
        elif numInitialServers > len(consulServers):
            print("****Servers Removed from Consul****")
            pass
        time.sleep(30)

def getAdditionalServers(oldServerAddressMap, newConsulServers):
    newServerAddressMap = {}
    for regServer in newConsulServers:
        address = "tcp://" + regServer["Address"] + ":" + str(regServer["Port"])
        newServerAddressMap[regServer["ID"]] = address
    additionalServers = { x:newServerAddressMap[x] for x in newServerAddressMap if x not in oldServerAddressMap }
    return additionalServers
    
    
if __name__ == "__main__":
    servers = []
    num_server = 1
    consulClient = consul.Consul()
    registeredServers = consulClient.agent.services().values()
    serverAddressMap = {}
    for regServer in registeredServers:
        address = "tcp://" + regServer["Address"] + ":" + str(regServer["Port"])
        servers.append(address)
        serverAddressMap[regServer["ID"]] = address
    # Process(target=consulListener, args=(consulClient,serverAddressMap,)).start()
    
    print("Servers:", servers)
    demoRoundRobin(servers)
    demoHRW(servers)
    # generate_data_hrw_hashing(servers)
    
