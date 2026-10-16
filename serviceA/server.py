from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, EmailStr, Field
from typing import List, Optional, Dict
import time
import json
import os
from aiokafka import AIOKafkaProducer
import connector
import asyncio

app = FastAPI()
BaseUrlB = "http://svc-b-cont:8080"
BaseUrlC = "http://svc-c-cont:8080"  

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = "user-gateway"

producer: Optional[AIOKafkaProducer] = None
last_signin_timestamps: Dict[str, float] = {}

rate_limit_lock=asyncio.Lock()

@app.on_event("startup")
async def startup_event():
    """Initialize Kafka producer on application startup with retry mechanism."""
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    
    max_retries = 5
    retry_delay = 3
    
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[Kafka] Attempting to connect to Kafka broker (Attempt {attempt}/{max_retries})...")
            await producer.start()
            print("[Kafka] Successfully connected to Kafka broker!")
            break
        except Exception as e:
            print(f"[Kafka Warning] Connection failed: {str(e)}")
            if attempt == max_retries:
                print("[Kafka Error] Max retries reached. Could not connect to Kafka.")
                raise e
            await asyncio.sleep(retry_delay)

@app.on_event("shutdown")
async def shutdown_event():
    """Close Kafka producer on application shutdown."""
    if producer:
        await producer.stop()

class IpListDTO(BaseModel):
    ips: List[str]


class UserSigninDTO(BaseModel):
    """Pydantic model for validating user signin payload."""
    email: EmailStr = Field(..., description="User email (required)")
    password: Optional[str] = Field(None, description="User password (optional)")
    ip_address: str = Field(..., description="Client IP address (required)")



async def is_ip_rate_limited(ip_address: str) -> bool:
    """
    Check if the given IP address has made a request within the last 60 seconds.
    Returns True if the IP is rate-limited, False otherwise.
    """
    async with rate_limit_lock:
        current_time = time.time()
        if ip_address in last_signin_timestamps:
            if current_time - last_signin_timestamps[ip_address] < 60:
                return True
        last_signin_timestamps[ip_address] = current_time 
    return False


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

@app.get("/users")
async def get_users():
    """
    Retrieves all stored user sign-in events by forwarding a request to Service B.
    """
    print(f"START: Get Users")
    try:
        return await connector.MakeRequest(
            BaseUrlB + "/getUsers", 2, "ServiceB-GetUsers"
        )
    except Exception as e:
        print(f"[ERROR] ServiceA get_users failed: {str(e)}")
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


@app.post("/signin", status_code=status.HTTP_200_OK)
async def user_signin(payload: UserSigninDTO):
    """
    Handles user signin, validates payload via Pydantic, checks 60s IP window rule,
    publishes an event to Kafka, and returns a confirmation message.
    """
    print(f"START: User signin for email {payload.email} from IP {payload.ip_address}")
    
    # 1. Check 60-second restriction for the same IP address
    if await is_ip_rate_limited(payload.ip_address): 
        print(f"[ERROR] Signin rate limited for IP: {payload.ip_address}")
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Requests from this IP address are restricted to once every 60 seconds."
        )
    
    # 2. Prepare event payload for Kafka
    event_data = {
        "email": payload.email,
        "has_password": payload.password is not None,
        "ip_address": payload.ip_address,
        "timestamp": time.time()
    }
    
    # 3. Publish event to Kafka topic
    try:
        if producer:
            respons=await producer.send_and_wait(
                KAFKA_TOPIC,
                json.dumps(event_data).encode("utf-8")
            )
            print(f"[Kafka] Successfully published signin event {respons} to topic '{KAFKA_TOPIC}'")
    except Exception as e:
        print(f"[ERROR] Failed to publish message to Kafka: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to publish event to Kafka: {str(e)}"
        )
        
    # 4. Return simple JSON response
    return {"message": f"your request was received ,{respons}"}


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