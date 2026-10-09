from fastapi import FastAPI
from memory import IpMemory

app = FastAPI()


@app.get("/getAll")
async def GetData():
    """
    Returns all system data stored in memory under a thread-safe async cluster lock.
    """
    try:
        length = await IpMemory.aquireTheLock(IpMemory.lengthofData)
        print(f"START: Get Data. Length: {length}")
        return await IpMemory.aquireTheLock(IpMemory.getData)
    except Exception as e:
        print(f"[ERROR] ServiceB GetData failed: {str(e)}")
        raise


@app.post("/AddIp")
async def AddIp(request_data: dict):
    """
    Adds IP and geographical information to the system.
    Ensures the IP does not already exist, generates a unique ID, and stores it in memory.
    """
    print("START: Add IP")
    try:
        ip = request_data.get("ipAddress")

        if not ip:
            print("[ERROR] AddIp failed: Missing or empty ipAddress in request data")
            return {"success": False, "message": "Missing or empty ipAddress in request data"}

        myBool = await IpMemory.aquireTheLock(IpMemory.doesIpExist, ip)
        print(f"ip exists {myBool}")
        if myBool: 
            print(f"[ERROR] AddIp failed: IP address {ip} already exists")
            return {"success": False, "message": "IP address already exists"}

        newId = await IpMemory.aquireTheLock(IpMemory.addIpAndGeoData, request_data)
        return {"success": True, "new_id": newId, "data_length": await IpMemory.aquireTheLock(IpMemory.lengthofData)}
    except Exception as e:
        print(f"[ERROR] ServiceB AddIp encountered exception: {str(e)}")
        raise


@app.get("/generate-geo-clusters")
async def GenerateGeoClusters():
    """
    Triggers the geographical cluster generation algorithm across all system data.
    """
    print(f"START: Generate Geo Clusters")
    try:
        return await IpMemory.aquireTheLock(IpMemory.generateClusters)
    except Exception as e:
        print(f"[ERROR] ServiceB GenerateGeoClusters failed: {str(e)}")
        raise


@app.delete("/delete/{iid}")
async def delete(iid):
    """
    Deletes a key-value pair from memory by identifier (iid) and removes the IP from active tracking.
    """
    print(f"START: delete {iid}")
    try:
        if not await IpMemory.aquireTheLock(IpMemory.doesKeyExist, iid):
            print(f"[ERROR] Delete failed: ID {iid} does not exist")
            return {"success": False, "message": "IP address does not exists"}

        await IpMemory.aquireTheLock(IpMemory.deleteKeyValuePair, iid)
        return {
            "success": True,
            "data_length": await IpMemory.aquireTheLock(IpMemory.lengthofData)
        }
    except Exception as e:
        print(f"[ERROR] ServiceB delete failed for ID {iid}: {str(e)}")
        raise