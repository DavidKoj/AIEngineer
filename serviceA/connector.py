import httpx
from pydantic import BaseModel, IPvAnyAddress
from fastapi import HTTPException

class ValidIpDTO(BaseModel):
    ip: IPvAnyAddress


def ValidateIp(ip: str):
    """
    Validates that a given string is a legitimate IP address (IPv4 or IPv6) using Pydantic.
    Throws a 400 HTTPException and logs the error upon failure.
    """
    try: 
        validated = ValidIpDTO(ip=ip)
    except Exception as e:
        print(f"[ERROR] Validation failed for IP '{ip}': {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

    print("validation passed: ", validated.ip)
    return validated.ip



async def MakeRequest(url, tout, svsName, meth='GET', JsonData=None):
    """
    Performs an asynchronous HTTP request (GET, POST, DELETE) to other services using httpx.
    Includes handling for timeouts, network errors, and non-200 statuses, with console logging.
    """
    
    async with httpx.AsyncClient() as client:
        HttpMethod, kwargs = client.get, {"timeout": tout}
        if meth == 'POST':
            HttpMethod, kwargs['json'] = client.post, JsonData
        if meth == 'DELETE': 
            HttpMethod = client.delete

        try: 
            response = await HttpMethod(url, **kwargs)
        except httpx.TimeoutException as te:
            print(f"[ERROR] {svsName} timed out while requesting {url}: {str(te)}")
            raise HTTPException(status_code=504, detail=f"{svsName} timed out")
        except httpx.RequestError as re:
            print(f"[ERROR] {svsName} network error while requesting {url}: {str(re)}")
            raise HTTPException(status_code=503, detail=f"{svsName} network error: {str(re)}")

    if response.status_code != 200:
        print(f"[ERROR] {svsName} failed with status {response.status_code} from {url}. Response body: {response.text}")
        raise HTTPException(status_code=500, detail=f"{svsName} failed: {response.status_code}")
    return response.json()
