import hashlib
import bisect
import zmq

class ConsistentHashRing:
    """
    Class Design based on following technical article - https://levelup.gitconnected.com/consistent-hashing-27636286a8a9
    """

    def __init__(self, servers:list, producers:dict, total_slots=128):
        """
        Initiate the hashring with a list of server nodes
        """
        self._servers = []
        self._keys = []
        self._total_slots = total_slots
        self._producers = producers
        for server in servers:
            key = self.generateHash(server)
            index = bisect.bisect(self._keys, key)
            self._servers.insert(index, server)
            self._keys.insert(index, key)


    def generateHash(self, key:str) -> int:
        """
        Hash function using sha512
        """
        # print("Hashing Key = ", key)
        newHash = hashlib.sha512()
        newHash.update(bytes(key.encode('utf-8')))
        return int(newHash.hexdigest(), 16) % self._total_slots

    def assignServer(self, item):
        """
        Assign a server for given key
        """
        key = self.generateHash(item)
        index = bisect.bisect_right(self._keys, key) % len(self._keys)
        return self._servers[index]

    def addNode(self, server):

        if len(self._keys) == self._total_slots:
            raise Exception("Hash Space is Full.")
        
        key = self.generateHash(server)
        index = bisect.bisect(self._keys, key)

        if index > 0 and self._keys[index] == key:
            raise Exception("Collision occured")
        
        nextServer = self.assignServer(server)
        data = self.fetchDataOfServer(nextServer)

        self._servers.insert(index,server)
        self._keys.insert(index,key)
        
        self.rebalanceDataForAdd(data)

    def fetchDataOfServer(self, nextServer):
        ## Rebalancing Logic
        getMsg = { 'op': 'GET_ALL' }
        producer = self._producers.get(nextServer)
        producer.send_json(getMsg)
        data = producer.recv_json()
        deleteMsg = { 'op': 'DELETE_ALL' }
        producer.send_json(deleteMsg)
        producer.recv_json()
        return data['collections']

    def rebalanceDataForAdd(self, data):
        for obj in data:
            key = list(obj.keys())[0]
            value = list(obj.values())[0]
            data = { 'op': 'PUT', 'key': key, 'value': value }
            server = self.assignServer(key)
            print(f"Sending data:{data} to {server}")
            self._producers[server].send_json(data)
            res =  self._producers[server].recv_json()
            print(res)
        print("Rebalancing complete")

    def removeNode(self, server):

        if len(self._keys) == 0:
            raise Exception("Hash Space is Empty.")
        
        key = self.generateHash(server)
        index = bisect.bisect_left(self._keys, key)

        if index >= len(self._keys) or self._keys[index] != key:
            raise Exception("Node does not exist")
        
        data = self.fetchDataOfServer(server)
        nextServer = self.assignServer(server)
        self.rebalanceDataForDel(data,nextServer)

        self._servers.pop(index)
        self._keys.pop(index)
        self._producers.pop(server)

        
    def rebalanceDataForDel(self, data, nextServer):
        for obj in data:
            key = list(obj.keys())[0]
            value = list(obj.values())[0]
            data = { 'op': 'PUT', 'key': key, 'value': value }
            print(f"Sending data:{data} to {nextServer}")
            self._producers[nextServer].send_json(data)
            res =  self._producers[nextServer].recv_json()
            print(res)
        print("Rebalancing complete")