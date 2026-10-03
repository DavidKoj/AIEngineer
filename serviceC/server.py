from fastapi import FastAPI, HTTPException
import time
import httpx
import connector

app = FastAPI()

@app.get("/fetch-geo/{ip}")
async def FetchGeoData(ip: str):
    print(f"START: Fetch Geo Data for IP {ip}")
    
    current_time = time.time()
    errors_log = []

    # --- שלב 1: ניסיון שימוש בספק החדש (ip-api.com) ---
    if current_time < connector.ip_api_rate_limit_state["blocked_until"]:
        print(f"[IP-API] Rate limit active. Skipping to fallback provider.")
    else:
        try:
            url_ip_api = f"http://ip-api.com/json/{ip}"
            
            # שליחה דרך ה־connector (כולל טיפול בשגיאות Non-HTTP כמו Timeout ו־RequestError)
            response = await connector.MakeRequest(url_ip_api, 4, "IP-API-Provider")
            
            # בדיקת כותרות X-Rl ו־X-Ttl
            x_rl = response.headers.get("X-Rl")
            x_ttl = response.headers.get("X-Ttl")
            print(f"[IP-API Headers] X-Rl: {x_rl}, X-Ttl: {x_ttl}")
            
            if x_rl is not None:
                remaining_requests = int(x_rl)
                if remaining_requests == 0 and x_ttl is not None:
                    ttl_seconds = int(x_ttl)
                    connector.ip_api_rate_limit_state["blocked_until"] = time.time() + ttl_seconds
                    print(f"[IP-API] X-Rl is 0! Blocking requests for {ttl_seconds} seconds.")

            # טיפול מפורש בכל קודי שגיאות ה־HTTP הנדרשים (429, 503, 500, 400, 403)
            status = response.status_code
            if status in [400, 403, 429, 500, 503]:
                if status == 429:
                    # חסימה לשעה עקב הגעה למגבלה מתמדת בהתאם להנחיות
                    connector.ip_api_rate_limit_state["blocked_until"] = time.time() + 3600
                    print(f"[IP-API] HTTP 429 received. Throttled/Banned for 1 hour.")
                raise HTTPException(status_code=status, detail=f"IP-API returned HTTP error {status}")

            raw_json = response.json()
            if raw_json.get("status") == "fail":
                raise HTTPException(status_code=400, detail=f"IP-API failed: {raw_json.get('message')}")

            unified_data = connector.UnifiedDataMapper("ip-api", raw_json)
            print(f"[IP-API] Success! Unified data: {unified_data}")
            return unified_data

        except HTTPException as he:
            errors_log.append(f"ip-api HTTP/Validation error {he.status_code}: {he.detail}")
        except (httpx.TimeoutException, httpx.RequestError) as non_http_err:
            errors_log.append(f"ip-api Non-HTTP error (Network/Timeout): {str(non_http_err)}")
        except Exception as e:
            errors_log.append(f"ip-api unknown error: {str(e)}")

    # --- שלב 2: Fallback לספק הישן (freeipapi) ---
    print(f"Falling back to FreeIpAPI...")
    try:
        url_free_api = f"https://free.freeipapi.com/api/v1/json/{ip}"
        response = await connector.MakeRequest(url_free_api, 4, "FreeIpAPI-Provider")

        status = response.status_code
        if status in [400, 403, 429, 500, 503]:
            raise HTTPException(status_code=status, detail=f"FreeIpAPI returned HTTP error {status}")

        raw_json = response.json()
        unified_data = connector.UnifiedDataMapper("free-ip-api", raw_json)
        print(f"FreeIpAPI fallback success! Unified data: {unified_data}")
        return unified_data

    except HTTPException as he:
        errors_log.append(f"FreeIpAPI HTTP error {he.status_code}: {he.detail}")
    except (httpx.TimeoutException, httpx.RequestError) as non_http_err:
        errors_log.append(f"FreeIpAPI Non-HTTP error (Network/Timeout): {str(non_http_err)}")
    except Exception as e:
        errors_log.append(f"FreeIpAPI unknown error: {str(e)}")

    # אם כל הספקים נכשלו מכל סיבה שהיא (HTTP או Non-HTTP)
    raise HTTPException(
        status_code=503, 
        detail=f"All geo providers failed. Errors encountered: {errors_log}"
    )