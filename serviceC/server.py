from fastapi import FastAPI, HTTPException
import time
import httpx
import asyncio
import connector

app = FastAPI()

#Dedicated asynchronous lock to protect shared state and rate-limiting variables across concurrent requests
geo_lock = asyncio.Lock()

@app.get("/fetch-geo/{ip}")
async def FetchGeoData(ip: str):
    print(f"START: Fetch Geo Data for IP {ip}")
    
    errors_log = []
    failed_on_ip_providers = set()  # Providers that permanently failed or had network errors for this specific IP

    while True:
        sleep_duration = 0
        
        #Protected section for reading and updating shared rate limit states ---
        async with geo_lock:
            current_time = time.time()
            
            #Check rate limit block status for external providers
            ip_api_blocked = current_time < connector.ip_api_rate_limit_state["blocked_until"]
            
            #Manage the 10 requests per 10 seconds sliding window for free-ip-api
            free_timestamps = connector.free_ip_api_rate_limit_state["request_timestamps"]
            connector.free_ip_api_rate_limit_state["request_timestamps"] = [
                t for t in free_timestamps if current_time - t < 10.0
            ]
            free_api_rate_limited = len(connector.free_ip_api_rate_limit_state["request_timestamps"]) >= 10

            #Stop condition: If all available providers failed on this IP
            if len(failed_on_ip_providers) >= 2:
                final_err_msg = f"All geo providers failed for IP {ip}. Errors: {errors_log}"
                print(f"[CRITICAL ERROR] {final_err_msg}")
                raise HTTPException(status_code=503, detail=final_err_msg)

            chosen_provider = None

            #Select the next available provider
            if not ip_api_blocked and "ip-api" not in failed_on_ip_providers:
                chosen_provider = "ip-api"
            elif not free_api_rate_limited and "free-ip-api" not in failed_on_ip_providers:
                chosen_provider = "free-ip-api"
            else:
                #Calculate required sleep duration if both providers are busy
                ip_api_wait = connector.ip_api_rate_limit_state["blocked_until"] - current_time if ip_api_blocked else 0
                
                oldest_free_t = (
                    connector.free_ip_api_rate_limit_state["request_timestamps"][0] 
                    if connector.free_ip_api_rate_limit_state["request_timestamps"] 
                    else current_time
                )
                free_api_wait = max(0, (oldest_free_t + 10.0) - current_time) if free_api_rate_limited else 0

                if "ip-api" in failed_on_ip_providers:
                    sleep_duration = max(0, free_api_wait)
                elif "free-ip-api" in failed_on_ip_providers:
                    sleep_duration = max(0, ip_api_wait)
                else:
                    sleep_duration = max(0, min(ip_api_wait, free_api_wait))

        #---Lock is released here, allowing other requests to access state during sleep ---

        if sleep_duration > 0:
            print(f"[Sync] Providers busy. Waiting: {sleep_duration:.2f}s")
            await asyncio.sleep(sleep_duration)  # Asynchronous wait without blocking the event loop/other requests
            continue

        # --- EXECUTE EXTERNAL HTTP REQUEST (Outside the lock to prevent blocking concurrent calls) ---
        if chosen_provider == "ip-api":
            try:
                url_ip_api = f"http://ip-api.com/json/{ip}"
                response = await connector.MakeRequest(url_ip_api, 4, "IP-API-Provider")
                
                # Re-acquire lock briefly to safely update shared rate limit state based on response headers/status
                async with geo_lock:
                    x_rl = response.headers.get("X-Rl")
                    x_ttl = response.headers.get("X-Ttl")

                    # Task 1 requirement: Print the values of X-Rl and X-Ttl each time this provider is used
                    print(f"[IP-API] X-Rl: {x_rl}, X-Ttl: {x_ttl}")

                    status = response.status_code

                    if status == 429:
                        ttl_seconds = int(x_ttl) if x_ttl and x_ttl.isdigit() else 60
                        if ttl_seconds > 60:
                            print("blocked for 1 hour")
                        connector.ip_api_rate_limit_state["blocked_until"] = time.time() + ttl_seconds
                        errors_log.append("ip-api rate limited (429)")
                        continue

                    if status in [400, 403, 500, 503]:
                        failed_on_ip_providers.add("ip-api")
                        errors_log.append(f"ip-api HTTP error {status}")
                        continue

                    raw_json = response.json()
                    if raw_json.get("status") == "fail":
                        failed_on_ip_providers.add("ip-api")
                        errors_log.append(f"ip-api validation failed: {raw_json.get('message')}")
                        continue

                # Success! Map and return unified data
                unified_data = connector.UnifiedDataMapper("ip-api", raw_json)
                return unified_data

            except Exception as e:
                async with geo_lock:
                    failed_on_ip_providers.add("ip-api")
                    errors_log.append(f"ip-api error: {str(e)}")

        elif chosen_provider == "free-ip-api":
            try:
                url_free_api = f"https://free.freeipapi.com/api/v1/json/{ip}"
                response = await connector.MakeRequest(url_free_api, 4, "FreeIpAPI-Provider")

                async with geo_lock:
                    status = response.status_code
                    if status in [400, 403, 429, 500, 503]:
                        failed_on_ip_providers.add("free-ip-api")
                        errors_log.append(f"FreeIpAPI HTTP error {status}")
                        continue

                    # Record timestamp for the rate limit sliding window
                    connector.free_ip_api_rate_limit_state["request_timestamps"].append(time.time())

                raw_json = response.json()
                unified_data = connector.UnifiedDataMapper("free-ip-api", raw_json)
                return unified_data

            except Exception as e:
                async with geo_lock:
                    failed_on_ip_providers.add("free-ip-api")
                    errors_log.append(f"FreeIpAPI error: {str(e)}")