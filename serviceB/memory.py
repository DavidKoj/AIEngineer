from uuid import uuid4
import asyncio
from cluster_manager import GeoClusterManager

class IpMemory:
    data = {} 
    UsedIps = set()
    memLock = asyncio.Lock()

    @classmethod
    async def aquireTheLock(cls, func, *args):
        async with cls.memLock:
            value = await func(*args)
        return value

    @classmethod
    async def getData(cls):
        return cls.data

    @classmethod
    async def lengthofData(cls):
        return len(cls.data)
    
    @classmethod
    async def doesKeyExist(cls, key):
        return key in cls.data

    @classmethod
    async def doesIpExist(cls, ip):
        return ip in cls.UsedIps

    @classmethod
    async def addIpAndGeoData(cls, requestData):
        newId = str(uuid4())[:8]
        cls.data[newId] = requestData
        cls.UsedIps.add(requestData["ipAddress"])
        return newId
    
    @classmethod
    async def deleteKeyValuePair(cls, key):
        record = cls.data.get(key)
        if record:
            ip = record.get("ipAddress")
            if ip in cls.UsedIps:
                cls.UsedIps.remove(ip) 
        del cls.data[key]

    @classmethod
    async def generateClusters(cls):
        return GeoClusterManager.generate_clusters(cls.data)