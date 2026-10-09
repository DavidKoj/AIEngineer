from uuid import uuid4
import asyncio
from cluster_manager import GeoClusterManager

class IpMemory:

    """
    Manages the in-memory state of system data and active IP addresses,
    featuring an asynchronous lock mechanism to prevent race conditions.
    """

    data = {} 
    UsedIps = set()
    memLock = asyncio.Lock()

    @classmethod
    async def aquireTheLock(cls, func, *args):
        """Wraps operations in an async lock to guarantee thread-safe memory access."""
        async with cls.memLock:
            value = await func(*args)
        return value

    @classmethod
    async def getData(cls):
        """Returns the full data dictionary."""
        return cls.data

    @classmethod
    async def lengthofData(cls):
        """Returns the total number of items stored in the system."""
        return len(cls.data)
    
    @classmethod
    async def doesKeyExist(cls, key):
        """Checks whether a specific key exists in the data dictionary."""
        return key in cls.data

    @classmethod
    async def doesIpExist(cls, ip):
        """Checks whether an IP address already exists in the active IPs set."""
        return ip in cls.UsedIps

    @classmethod
    async def addIpAndGeoData(cls, requestData):
        """Adds a new record with an 8-character truncated UUID and registers the IP address."""
        newId = str(uuid4())[:8]
        cls.data[newId] = requestData
        cls.UsedIps.add(requestData["ipAddress"])
        return newId
    
    @classmethod
    async def deleteKeyValuePair(cls, key):
        """Deletes a record by key and removes its IP address from the active set."""
        record = cls.data.get(key)
        if record:
            ip = record.get("ipAddress")
            if ip in cls.UsedIps:
                cls.UsedIps.remove(ip) 
        del cls.data[key]

    @classmethod
    async def generateClusters(cls):
        """Executes the clustering algorithm on the current data dictionary."""
        return GeoClusterManager.generate_clusters(cls.data)