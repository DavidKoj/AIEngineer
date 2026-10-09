from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import connector

app = FastAPI()
BaseUrlB = "http://svc-b-cont:8080"
BaseUrlC = "http://svc-c-cont:8080"  


class IpListDTO(BaseModel):
    ips: List[str]


@app.get("/")
async def GetData():
    """
    Retrieves all stored data from the system by forwarding a request to Service B.
    Returns the complete data dictionary.
    """
    print(f"START: Get Data")
    try:
        return await connector.MakeRequest(
            BaseUrlB + "/getAll", 2, "ServiceB-GetAll"
        )
    except Exception as e:
        print(f"[ERROR] ServiceA GetData failed: {str(e)}")
        raise


@app.get("/favicon.ico")
def favicon():
    """
    Handles browser favicon requests to avoid unnecessary 404 errors in logs.
    """
    print(f"START: favicon")
    return "ok"



@app.get("/resolve-ip/{ip}")
async def AddIp(ip: str):
    """
    Receives a single IP address, validates it, fetches geographical data from Service C,
    and stores the result in Service B.
    """
    print(f"START: Add IP {ip}")
    try:
        ip = connector.ValidateIp(ip)

        geo_data = await connector.MakeRequest(
            f"{BaseUrlC}/fetch-geo/{ip}", 11, "ServiceC-FetchGeo"
        )
        
        print(f"ServiceC responded with geo data")

        return await connector.MakeRequest(
            BaseUrlB + "/AddIp", 8, "ServiceB-AddIp", meth='POST',
            JsonData=geo_data
        )
    except Exception as e:
        print(f"[ERROR] ServiceA AddIp failed for IP {ip}: {str(e)}")
        raise



@app.post("/resolve-ip-list")
async def ResolveIpList(payload: IpListDTO):
    """
    Receives a list of IP addresses and processes them in a batch loop.
    For each IP, validates, fetches geo info, saves to Service B, and returns
    a detailed result array indicating success or error status per IP.
    """
    print(f"START: Resolve IP List with {len(payload.ips)} IPs")
    
    results = []
    
    for ip_str in payload.ips:
        try:
            validated_ip = connector.ValidateIp(ip_str)
            
            geo_data = await connector.MakeRequest(
                f"{BaseUrlC}/fetch-geo/{validated_ip}", 11, "ServiceC-FetchGeo"
            )
            
            save_response = await connector.MakeRequest(
                BaseUrlB + "/AddIp", 8, "ServiceB-AddIp", meth='POST',
                JsonData=geo_data
            )
            
            results.append({"ip": ip_str, "success": True, "result": save_response})
            
        except Exception as e:
            error_detail = getattr(e, 'detail', str(e))
            print(f"[ERROR] ServiceA ResolveIpList failed for IP {ip_str}: {error_detail}")
            results.append({"ip": ip_str, "success": False, "error": error_detail})

    return {"processed_ips": results}




@app.get("/delete/{iid}")
async def delete(iid):
    """
    Deletes a record from the system by unique identifier (iid) via Service B.
    """
    print(f"START: delete {iid}")
    try:
        return await connector.MakeRequest(
            BaseUrlB + f"/delete/{iid}", 2, "ServiceB-Delete", meth='DELETE'
        )
    except Exception as e:
        print(f"[ERROR] ServiceA delete failed for ID {iid}: {str(e)}")
        raise