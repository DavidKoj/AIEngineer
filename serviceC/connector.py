import httpx
from fastapi import HTTPException
import time

ip_api_rate_limit_state = {
    "blocked_until": 0.0
}

free_ip_api_rate_limit_state = {
    "blocked_until": 0.0,
    "request_timestamps": []  
}



async def MakeRequest(url, tout, svsName, meth='GET', JsonData=None):
    """
    Performs an HTTP request to external geolocation providers with network error and timeout handling.
    """
    
    async with httpx.AsyncClient() as client:
        try: 
            response = await client.get(url, timeout=tout)
        except httpx.TimeoutException as te:
            print(f"[ERROR] {svsName} timed out for URL {url}: {str(te)}")
            raise HTTPException(status_code=504, detail=f"{svsName} timed out")
        except httpx.RequestError as re:
            print(f"[ERROR] {svsName} network error for URL {url}: {str(re)}")
            raise HTTPException(status_code=503, detail=f"{svsName} network error: {str(re)}")

    if response.status_code != 200:
        print(f"[ERROR] {svsName} failed with status {response.status_code} from {url}. Response body: {response.text}")
        raise HTTPException(
            status_code=response.status_code, 
            detail=f"{svsName} failed with status {response.status_code}"
        )
    return response


def UnifiedDataMapper(provider_name: str, raw_data: dict) -> dict:
    """
    Maps and normalizes raw JSON data from any external provider into a uniform system-wide format.
    """
    result = {
        "ipAddress": None,
        "latitude": None,
        "longitude": None,
        "countryName": None,
        "cityName": None,
        "asn": None,
        "asnOrganization": None,
        "isProxy": False
    }

    if provider_name == "free-ip-api":
        result["ipAddress"] = raw_data.get("ipAddress")
        result["latitude"] = raw_data.get("latitude")
        result["longitude"] = raw_data.get("longitude")
        result["countryName"] = raw_data.get("countryName")
        result["cityName"] = raw_data.get("cityName")
        result["asn"] = raw_data.get("asn")
        result["asnOrganization"] = raw_data.get("asnOrganization")
        result["isProxy"] = raw_data.get("isProxy", False)

    elif provider_name == "ip-api":
        result["ipAddress"] = raw_data.get("query")
        result["latitude"] = raw_data.get("lat")
        result["longitude"] = raw_data.get("lon")
        result["countryName"] = raw_data.get("countryName") or raw_data.get("country")
        result["cityName"] = raw_data.get("city")
        
        as_str = raw_data.get("as", "")
        if as_str:
            parts = as_str.split(" ", 1)
            result["asn"] = parts[0]
            result["asnOrganization"] = parts[1] if len(parts) > 1 else ""
            
        result["isProxy"] = raw_data.get("proxy", False)

    return result