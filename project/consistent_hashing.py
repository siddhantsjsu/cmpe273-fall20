import hashlib
import bisect

class ConsistentHashRing:
    """
    Class Design based on following technical article - https://levelup.gitconnected.com/consistent-hashing-27636286a8a9
    """
    
    def __init__(self, servers:list, total_slots=128):
        """
        Initiate the hashring with a list of server nodes
        """
        self._servers = []
        self._keys = []
        self._total_slots = total_slots
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

    
