import httpx
from fastapi import HTTPException
import time

# זיכרון מקומי למעקב אחר Rate Limit של הספק החדש (ip-api)
ip_api_rate_limit_state = {
    "blocked_until": 0.0
}

async def MakeRequest(url, tout, svsName, meth='GET', JsonData=None):
    async with httpx.AsyncClient() as client:
        try: 
            response = await client.get(url, timeout=tout)
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail=f"{svsName} timed out")
        except httpx.RequestError as e:
            raise HTTPException(status_code=503, detail=f"{svsName} network error: {str(e)}")

    # טיפול מקיף בשגיאות HTTP הנדרשות
    if response.status_code != 200:
        raise HTTPException(
            status_code=response.status_code, 
            detail=f"{svsName} failed with status {response.status_code}"
        )
    return response


def UnifiedDataMapper(provider_name: str, raw_data: dict) -> dict:
    """
    ממיר את הנתונים משני הספקים השונים למבנה אחיד שהמערכת שלנו מצפה לו:
    {ipAddress, latitude, longitude, countryName, cityName, asn, asnOrganization, isProxy}
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
        # המבנה של freeipapi
        result["ipAddress"] = raw_data.get("ipAddress")
        result["latitude"] = raw_data.get("latitude")
        result["longitude"] = raw_data.get("longitude")
        result["countryName"] = raw_data.get("countryName")
        result["cityName"] = raw_data.get("cityName")
        result["asn"] = raw_data.get("asn")
        result["asnOrganization"] = raw_data.get("asnOrganization")
        result["isProxy"] = raw_data.get("isProxy", False)

    elif provider_name == "ip-api":
        # המבנה של ip-api.com (לדוגמה: query, lat, lon, country, city, as, proxy)
        result["ipAddress"] = raw_data.get("query")
        result["latitude"] = raw_data.get("lat")
        result["longitude"] = raw_data.get("lon")
        result["countryName"] = raw_data.get("countryName") or raw_data.get("country")
        result["cityName"] = raw_data.get("city")
        
        as_str = raw_data.get("as", "")
        # שדה ה־as מגיע בדרך כלל בפורמט כמו "AS15169 Google LLC", נפרק אותו במידת הצורך
        if as_str:
            parts = as_str.split(" ", 1)
            result["asn"] = parts[0]
            result["asnOrganization"] = parts[1] if len(parts) > 1 else ""
            
        result["isProxy"] = raw_data.get("proxy", False)

    return result