import zmq
import sys
from  multiprocessing import Process
import requests
import time
import base64
import consul

def server(consulClient,port,id):
    print(f"Server ID:{id} at Port:{port} Initialized")
    name = "Server-{}".format(id)
    consulClient.agent.service.register(name, service_id=str(id), port=port)
    # context = zmq.Context()
    # consumer = context.socket(zmq.PULL)
    # consumer.connect(f"tcp://127.0.0.1:{port}")
    # storage = {}
    # count = 0
    # while True:
    #     raw = consumer.recv_json()
    #     key, value = raw['key'], raw['value']
    #     print(f"Server_port={port}:key={key},value={value}")
    #     storage[key] = value
    #     print(f"Storage Snapshot for Port:{port} Count:{len(storage.keys())}") #Printing count of keys in dictionary to validate algorithm

def consulListener(consulClient):
    time.sleep(10)
    print("Listening for cluster updates from Consul...")
    while True:
        index, data = consulClient.kv.get('python/servers/count')
        numServers = int(data.get("Value"))
        print("Cluster Size: ", numServers)
        print("Services Registered on Consul: ", len(consulClient.agent.services().keys()))
        time.sleep(10)

if __name__ == "__main__":
    consulClient = consul.Consul()
    index, data = consulClient.kv.get('python/servers/count')
    if(len(data) < 1):
        print("Cluster Size not found")
    else:
        numServers = int(data.get("Value"))
        Process(target=consulListener, args=(consulClient,)).start()
        for eachServer in range(numServers):
            port = "200{}".format(eachServer)
            print(f"Starting a server at:{port}...")
            Process(target=server, args=(consulClient,port,eachServer,)).start()
    