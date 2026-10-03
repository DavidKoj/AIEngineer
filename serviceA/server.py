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
    print(f"START: Get Data")
    return await connector.MakeRequest(
        BaseUrlB + "/getAll", 2, "ServiceB-GetAll"
    )

@app.get("/favicon.ico")
def favicon():
    print(f"START: favicon")
    return "ok"


@app.get("/resolve-ip/{ip}")
async def AddIp(ip: str):
    print(f"START: Add IP {ip}")
    ip = connector.ValidateIp(ip)

    geo_data = await connector.MakeRequest(
        f"{BaseUrlC}/fetch-geo/{ip}", 4, "ServiceC-FetchGeo"
    )
    
    print(f"ServiceC responded with geo data")

    return await connector.MakeRequest(
        BaseUrlB + "/AddIp", 8, "ServiceB-AddIp", meth='POST',
        JsonData=geo_data
    )


@app.post("/resolve-ip-list")
async def ResolveIpList(payload: IpListDTO):
    print(f"START: Resolve IP List with {len(payload.ips)} IPs")
    
    results = []
    
    for ip_str in payload.ips:
        try:
            validated_ip = connector.ValidateIp(ip_str)
            
            geo_data = await connector.MakeRequest(
                f"{BaseUrlC}/fetch-geo/{validated_ip}", 4, "ServiceC-FetchGeo"
            )
            
            save_response = await connector.MakeRequest(
                BaseUrlB + "/AddIp", 8, "ServiceB-AddIp", meth='POST',
                JsonData=geo_data
            )
            
            results.append({"ip": ip_str, "success": True, "result": save_response})
            
        except Exception as e:
            error_detail = getattr(e, 'detail', str(e))
            results.append({"ip": ip_str, "success": False, "error": error_detail})

    return {"processed_ips": results}


@app.get("/delete/{iid}")
async def delete(iid):
    print(f"START: delete {iid}")
    return await connector.MakeRequest(
        BaseUrlB + f"/delete/{iid}", 2, "ServiceB-Delete", meth='DELETE'
    )
