from fastapi import FastAPI, HTTPException
import time
import httpx
import asyncio
import connector

app = FastAPI()

@app.get("/fetch-geo/{ip}")
async def FetchGeoData(ip: str):
    print(f"START: Fetch Geo Data for IP {ip}")
    
    errors_log = []
    failed_on_ip_providers = set()  # Providers that permanently failed on this IP or had network errors

    while True:
        current_time = time.time()
        
        # 1. Check rate limit block status for providers
        ip_api_blocked = current_time < connector.ip_api_rate_limit_state["blocked_until"]
        
        # Manage the 10 requests per 10 seconds window for free-ip-api
        free_timestamps = connector.free_ip_api_rate_limit_state["request_timestamps"]
        connector.free_ip_api_rate_limit_state["request_timestamps"] = [t for t in free_timestamps if current_time - t < 10.0]
        free_api_rate_limited = len(connector.free_ip_api_rate_limit_state["request_timestamps"]) >= 10
        
        # Check if free-ip-api is blocked due to time or request count limits
        free_api_blocked = (current_time < connector.free_ip_api_rate_limit_state["blocked_until"]) or free_api_rate_limited

        # 2. Stop condition: If both providers failed on this IP, throw 503 error
        if len(failed_on_ip_providers) >= 2:
            final_err_msg = f"All geo providers failed for IP {ip}. Errors: {errors_log}"
            print(f"[CRITICAL ERROR] {final_err_msg}")
            raise HTTPException(status_code=503, detail=final_err_msg)

        chosen_provider = None

        # 3. Select the next available provider
        if not ip_api_blocked and "ip-api" not in failed_on_ip_providers:
            chosen_provider = "ip-api"
        elif not free_api_blocked and "free-ip-api" not in failed_on_ip_providers:
            chosen_provider = "free-ip-api"
        else:
            # Both providers are unavailable (either rate-limited, or one failed and the other is rate-limited)
            
            ip_api_wait = connector.ip_api_rate_limit_state["blocked_until"] - current_time if ip_api_blocked else 0
            
            oldest_free_t = connector.free_ip_api_rate_limit_state["request_timestamps"][0] if connector.free_ip_api_rate_limit_state["request_timestamps"] else current_time
            free_api_wait = max(
                connector.free_ip_api_rate_limit_state["blocked_until"] - current_time,
                (oldest_free_t + 10.0) - current_time
            ) if free_api_blocked else 0

            # If ip-api failed on this IP, ignore it and wait only for free-ip-api rate limit
            if "ip-api" in failed_on_ip_providers:
                sleep_duration = max(0, free_api_wait)
                print(f"[Sync] ip-api failed on IP. Waiting for free-ip-api rate limit: {sleep_duration:.2f}s")
            # If free-ip-api failed on this IP, ignore it and wait only for ip-api rate limit
            elif "free-ip-api" in failed_on_ip_providers:
                sleep_duration = max(0, ip_api_wait)
                print(f"[Sync] free-ip-api failed on IP. Waiting for ip-api rate limit: {sleep_duration:.2f}s")
            else:
                # Both are rate-limited -> wait for the one that frees up the quickest
                sleep_duration = max(0, min(ip_api_wait, free_api_wait))
                print(f"[Sync] Both providers busy. Waiting for the quickest available: {sleep_duration:.2f}s")

            if sleep_duration > 0:
                await asyncio.sleep(sleep_duration)
            continue

        # --- Execute request with the chosen provider ---
        if chosen_provider == "ip-api":
            try:
                url_ip_api = f"http://ip-api.com/json/{ip}"
                response = await connector.MakeRequest(url_ip_api, 4, "IP-API-Provider")
                
                # Track headers as required
                x_rl = response.headers.get("X-Rl")
                x_ttl = response.headers.get("X-Ttl")
                print(f"[IP-API Headers] X-Rl: {x_rl}, X-Ttl: {x_ttl}")
                
                if x_rl is not None and int(x_rl) == 0 and x_ttl is not None:
                    ttl_seconds = int(x_ttl)
                    connector.ip_api_rate_limit_state["blocked_until"] = time.time() + ttl_seconds
                    print(f"[IP-API] X-Rl is 0! Blocking for {ttl_seconds}s.")

                status = response.status_code
                
                # Handle HTTP 429 (Rate Limit) - Temporary block, do not add to failed_on_ip_providers
                if status == 429:
                    connector.ip_api_rate_limit_state["blocked_until"] = time.time() + 3600
                    err_msg = "ip-api rate limited (429)"
                    print(f"[IP-API] HTTP 429 received. Banned for 1 hour.")
                    errors_log.append(err_msg)
                    continue

                # Handle server/client errors requiring immediate failover
                if status in [400, 403, 500, 503]:
                    failed_on_ip_providers.add("ip-api")
                    err_msg = f"ip-api HTTP error {status}"
                    print(f"[ERROR] {err_msg}")
                    errors_log.append(err_msg)
                    continue

                raw_json = response.json()
                if raw_json.get("status") == "fail":
                    msg = raw_json.get('message')
                    failed_on_ip_providers.add("ip-api")
                    err_msg = f"ip-api validation failed: {msg}"
                    print(f"[IP-API ERROR] {err_msg}")
                    errors_log.append(err_msg)
                    continue

                unified_data = connector.UnifiedDataMapper("ip-api", raw_json)
                print(f"[IP-API] Success! Unified data: {unified_data}")
                return unified_data

            except (httpx.TimeoutException, httpx.RequestError) as non_http_err:
                failed_on_ip_providers.add("ip-api")
                err_msg = f"ip-api network error: {str(non_http_err)}"
                print(f"[ERROR] {err_msg}")
                errors_log.append(err_msg)
            except Exception as e:
                failed_on_ip_providers.add("ip-api")
                err_msg = f"ip-api unknown error: {str(e)}"
                print(f"[ERROR] {err_msg}")
                errors_log.append(err_msg)

        elif chosen_provider == "free-ip-api":
            try:
                url_free_api = f"https://free.freeipapi.com/api/v1/json/{ip}"
                response = await connector.MakeRequest(url_free_api, 4, "FreeIpAPI-Provider")

                status = response.status_code
                
                # Handle server/client errors and rate limits for free-ip-api
                if status in [400, 403, 429, 500, 503]:
                    failed_on_ip_providers.add("free-ip-api")
                    err_msg = f"FreeIpAPI HTTP error {status}"
                    print(f"[ERROR] {err_msg}")
                    errors_log.append(err_msg)
                    continue

                # Record timestamp for 10 requests per 10 seconds limit window
                connector.free_ip_api_rate_limit_state["request_timestamps"].append(time.time())

                raw_json = response.json()
                unified_data = connector.UnifiedDataMapper("free-ip-api", raw_json)
                print(f"FreeIpAPI success! Unified data: {unified_data}")
                return unified_data

            except (httpx.TimeoutException, httpx.RequestError) as non_http_err:
                failed_on_ip_providers.add("free-ip-api")
                err_msg = f"FreeIpAPI network error: {str(non_http_err)}"
                print(f"[ERROR] {err_msg}")
                errors_log.append(err_msg)
            except Exception as e:
                failed_on_ip_providers.add("free-ip-api")
                err_msg = f"FreeIpAPI unknown error: {str(e)}"
                print(f"[ERROR] {err_msg}")
                errors_log.append(err_msg)