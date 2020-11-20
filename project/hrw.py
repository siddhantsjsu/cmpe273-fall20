import mmh3
import bisect

class RendezvousHashRing:
    """
    Class Design based on following references - https://github.com/clohfink/RendezvousHash , https://pypi.org/project/clandestined/
    """

    def __init__(self, servers:list):
        """
        Initiate the hashring with a list of server nodes
        """
        self._servers = servers
        self._serverIntMap = {}

    def generateHash(self, key:str, seed=None) -> int:
        """
        Hash function using Murmur Hash 3
        """
        return mmh3.hash(key, seed, signed = False)

    def convertIpToInt(self, server:str) -> int:
        """
        Convert Server IP to Int to be used as seed in Murmur Hash
        """
        proto_str, ip_str, port_str = server.split(":")
        ip_byte1, ip_byte2, ip_byte3, ip_byte4 = ip_str[2:].split(".")
        return int(ip_byte1) * int(port_str) + int(ip_byte2) * int(port_str) + int(ip_byte3) * int(port_str) + int(ip_byte4) * int(port_str)


    def assignServer(self, item):
        """
        Assign a server for given key
        """
        high_score = float("-inf")
        winner = None
        for server in self._servers:
            if server in self._serverIntMap:
                seed = self._serverIntMap.get(server)
            else:
                seed = self.convertIpToInt(server)
                self._serverIntMap[server] = seed
            score = self.generateHash(item, seed)
            if score >= high_score:
                (high_score, winner) = (score, server)
        return winner

    
