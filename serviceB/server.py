from fastapi import FastAPI
from memory import IpMemory

app = FastAPI()


@app.get("/getAll")
async def GetData():
    """
    Returns all system IP data stored in memory under a thread-safe async cluster lock.
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
    Adds IP and geographical information to the system IP storage.
    Ensures the IP does not already exist, generates a unique ID, and stores it.
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


@app.post("/AddUser")
async def AddUser(user_event: dict):
    """
    Receives user sign-in events from the Kafka consumer and stores them 
    in the isolated users storage dictionary.
    """
    print("START: Add User Event")
    try:
        userId = await IpMemory.aquireTheLock(IpMemory.addUserEvent, user_event)
        print(f"User event successfully stored with ID: {userId}")
        return {"success": True, "user_id": userId}
    except Exception as e:
        print(f"[ERROR] ServiceB AddUser encountered exception: {str(e)}")
        raise


@app.get("/getUsers")
async def GetUsers():
    """
    Returns all stored user sign-in events.
    """
    try:
        return await IpMemory.aquireTheLock(IpMemory.getUsersData)
    except Exception as e:
        print(f"[ERROR] ServiceB GetUsers failed: {str(e)}")
        raise


@app.get("/generate-geo-clusters")
async def GenerateGeoClusters():
    """
    Triggers the geographical cluster generation algorithm exclusively on the IP data dictionary.
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
    Deletes an IP record from memory by identifier (iid) and removes the IP from active tracking.
    """
    print(f"START: delete {iid}")
    try:
        if not await IpMemory.aquireTheLock(IpMemory.doesKeyExist, iid):
            print(f"[ERROR] Delete failed: ID {iid} does not exist")
            return {"success": False, "message": "IP address does not exist"}

        await IpMemory.aquireTheLock(IpMemory.deleteKeyValuePair, iid)
        return {
            "success": True,
            "data_length": await IpMemory.aquireTheLock(IpMemory.lengthofData)
        }
    except Exception as e:
        print(f"[ERROR] ServiceB delete failed for ID {iid}: {str(e)}")
        raise