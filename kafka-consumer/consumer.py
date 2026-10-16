import os
import json
import time
import requests
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC_NAME = os.getenv("KAFKA_TOPIC")
GROUP_ID = os.getenv("KAFKA_GROUP_ID")
SERVICE_B_URL = os.getenv("SERVICE_B_URL")

def process_user_event(email: str, ip_address: str):
    """
    Communicates with Service B to check if the user already exists in the isolated users storage, 
    and adds the user if it's a new sign-in case.
    """
    try:
        # Check existing users from Service B's isolated users endpoint
        check_response = requests.get(f"{SERVICE_B_URL}/getUsers", timeout=5)
        
        if check_response.status_code == 200:
            users_data = check_response.json()
            
            # Check if the user's email already exists in the stored user events
            user_exists = any(
                record.get("email") == email 
                for record in users_data.values()
            )
            
            if not user_exists:
                payload = {
                    "ipAddress": ip_address,
                    "email": email
                }
                # Post the new user event to Service B's dedicated /AddUser endpoint
                add_response = requests.post(f"{SERVICE_B_URL}/AddUser", json=payload, timeout=5)
                print(f"[Consumer] New user registered in Service B: {email} (Status: {add_response.status_code})")
            else:
                print(f"[Consumer] User already exists in Service B users memory: {email}")
        else:
            print(f"[Consumer Error] Failed to fetch users from Service B. Status: {check_response.status_code}")
            
    except requests.exceptions.RequestException as e:
        print(f"[Consumer Error] Connection error while communicating with Service B: {e}")

def start_consumer():
    """
    Initializes the Kafka consumer and continuously listens to messages from the topic.
    """
    print(f"[Consumer] Starting Kafka Consumer on topic '{TOPIC_NAME}'...")
    
    # Brief delay to ensure Kafka broker and services are fully stable and ready
    time.sleep(5)
    
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    print("[Consumer] Successfully connected to Kafka and waiting for messages...")
    
    # Infinite loop ensuring the consumer continuously listens for incoming Kafka events
    for message in consumer:
        event_data = message.value
        print(f"[Consumer] Event consumed from Kafka: {event_data}")
        
        email = event_data.get("email")
        ip_address = event_data.get("ip_address") or event_data.get("ipAddress")
        
        if email and ip_address:
            process_user_event(email, ip_address)
        else:
            print(f"[Consumer Warning] Invalid event payload received: {event_data}")

if __name__ == "__main__":
    start_consumer()