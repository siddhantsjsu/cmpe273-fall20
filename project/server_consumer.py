import zmq
import sys
from  multiprocessing import Process
import requests
import time
import base64
import consul

def server(consulClient,port):
    print(f"Server ID:{port} at Port:{port} Initialized")
    name = "Server-{}".format(port)
    consulClient.agent.service.register(name, service_id=port, address= "127.0.0.1", port=int(port))
    context = zmq.Context()
    consumer = context.socket(zmq.REP)
    consumer.bind(f"tcp://127.0.0.1:{port}")
    storage = {}
    count = 0
    while True:
        message = consumer.recv_json()
        op = message['op']
        if op == 'PUT':
            key, value = message['key'], message['value']
            print(f"Server_port={port},op={op},key={key},value={value}")
            storage[key] = value
            consumer.send_json({'op': 'PUT', 'message': 'Success'})
        elif op == 'GET_ONE':
            key =  message['key']
            print(f"Server_port={port},op={op},key={key}")
            value = storage.get(key)
            consumer.send_json({'key': key, 'value': value})
        elif op == 'GET_ALL':
            print(f"Server_port={port},op={op}")
            value = { 'collections': [{k:v} for k,v in storage.items()] }
            consumer.send_json(value)
        elif op == 'DELETE_ALL':
            print(f"Server_port={port},op={op}")
            storage = {}
            consumer.send_json({'op': 'DELETE_ALL', 'message': 'Success'})
        # print(f"Storage Snapshot for Port:{port} Count:{len(storage.keys())}") #Printing count of keys in dictionary to validate algorithm

def consulListener(consulClient):
    time.sleep(10)
    print("Listening for cluster updates from Consul...")
    while True:
        index, data = consulClient.kv.get('python/servers/count')
        numServers = int(data.get("Value"))
        print("Cluster Size: ", numServers)
        print("Services Registered on Consul: ", len(consulClient.agent.services().keys()))
        time.sleep(30)

if __name__ == "__main__":
    consulClient = consul.Consul()
    index, data = consulClient.kv.get('python/servers/count')
    if(len(data) < 1):
        print("Cluster Size not found")
    else:
        numServers = int(data.get("Value"))
        # Process(target=consulListener, args=(consulClient,)).start()
        for eachServer in range(numServers):
            port = "200{}".format(eachServer)
            print(f"Starting a server at:{port}...")
            Process(target=server, args=(consulClient,port,)).start()
    