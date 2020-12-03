import zmq
import sys
from  multiprocessing import Process
import requests
import time
import base64
import consul

def bootServer(consulClient,address,port,isRegistered):
    print(f"Server ID:{port} at Port:{port} Initialized")
    name = "Server-{}".format(port)
    if not isRegistered:
        consulClient.agent.service.register(name, service_id=port, address= "127.0.0.1", port=int(port))
    context = zmq.Context()
    consumer = context.socket(zmq.REP)
    consumer.bind(f"tcp://{address}:{port}")
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

def consulListener(numInitialServers, consulClient, serverSet):
    time.sleep(10)
    print("Listening for cluster updates from Consul...")
    numCurrentServers = numInitialServers
    currentServerSet = serverSet
    while True:
        consulServers = consulClient.agent.services().values()
        print("Servers Registered on Consul: ", len(consulServers))
        if numCurrentServers < len(consulServers):
            print("****New Servers Discovered on Consul****")
            additionalServers = getAdditionalServers(currentServerSet, consulServers)
            for server in additionalServers:
                print(f"Booting Server - {server}")
                address, port = server.split(":")
                Process(target=bootServer, args=(consulClient,address,port,True)).start()
                numCurrentServers += 1
                currentServerSet.add(server)
        elif numCurrentServers > len(consulServers):
            print("****Servers Removed from Consul****")
            numCurrentServers = len(consulServers)
            pass
        time.sleep(10)

def getAdditionalServers(oldServerSet, newConsulServers):
    newServerSet = set()
    for regServer in newConsulServers:
        address = regServer["Address"] + ":" + str(regServer["Port"])
        newServerSet.add(address)
    additionalServers = newServerSet.difference(oldServerSet)
    return additionalServers

if __name__ == "__main__":
    consulClient = consul.Consul()
    index, data = consulClient.kv.get('python/servers/count')
    if(len(data) < 1):
        print("Cluster Size not found")
    else:
        initialServerSet = set()
        numServers = int(data.get("Value"))
        for eachServer in range(numServers):
            address = "127.0.0.1"
            port = "200{}".format(eachServer)
            print(f"Starting a server at:{port}...")
            initialServerSet.add(f"{address}:{port}")
            Process(target=bootServer, args=(consulClient,address,port,False,)).start()
        Process(target=consulListener, args=(numServers,consulClient,initialServerSet)).start()
    