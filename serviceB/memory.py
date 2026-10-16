from uuid import uuid4
import asyncio
from cluster_manager import GeoClusterManager

class IpMemory:
    """
    Manages the in-memory state of system data (IPs and Users separately),
    featuring asynchronous locks to prevent race conditions.
    """

    data = {}        # Strictly for IP addresses and geographical data (used for clusters)
    users_data = {}  # Strictly for user sign-in events coming from the Kafka consumer
    UsedIps = set()
    
    memLock = asyncio.Lock()

    @classmethod
    async def aquireTheLock(cls, func, *args):
        """Wraps operations in an async lock to guarantee thread-safe memory access."""
        async with cls.memLock:
            value = await func(*args)
        return value

    # --- IP & GEO DATA METHODS ---
    @classmethod
    async def getData(cls):
        """Returns the full IP data dictionary."""
        return cls.data

    @classmethod
    async def lengthofData(cls):
        """Returns the total number of IP items stored in the system."""
        return len(cls.data)
    
    @classmethod
    async def doesKeyExist(cls, key):
        """Checks whether a specific key exists in the IP data dictionary."""
        return key in cls.data

    @classmethod
    async def doesIpExist(cls, ip):
        """Checks whether an IP address already exists in the active IPs set."""
        return ip in cls.UsedIps

    @classmethod
    async def addIpAndGeoData(cls, requestData):
        """Adds a new IP record with an 8-character truncated UUID and registers the IP address."""
        newId = str(uuid4())[:8]
        cls.data[newId] = requestData
        cls.UsedIps.add(requestData["ipAddress"])
        return newId
    
    @classmethod
    async def deleteKeyValuePair(cls, key):
        """Deletes an IP record by key and removes its IP address from the active set."""
        record = cls.data.get(key)
        if record:
            ip = record.get("ipAddress")
            if ip in cls.UsedIps:
                cls.UsedIps.remove(ip) 
        del cls.data[key]

    @classmethod
    async def generateClusters(cls):
        """Executes the clustering algorithm exclusively on the clean IP data dictionary."""
        return GeoClusterManager.generate_clusters(cls.data)

    # --- USER EVENTS METHODS ---
    @classmethod
    async def addUserEvent(cls, eventData):
        """Stores a user sign-in event in the isolated users storage with a unique ID."""
        userId = str(uuid4())[:8]
        cls.users_data[userId] = eventData
        return userId

    @classmethod
    async def getUsersData(cls):
        """Returns all stored user sign-in events."""
        return cls.users_data