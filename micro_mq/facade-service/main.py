import uuid
import time
import os
import random
import logging
import json
from datetime import datetime
import httpx
import hazelcast
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER", "http://config-server:8000")
MY_ADDRESS = os.getenv("MY_ADDRESS", "facade-service:8080")
HZ_ADDRESSES_ENV = os.getenv("HZ_ADDRESSES", "hz1:5701")
HZ_ADDRESSES = [addr.strip() for addr in HZ_ADDRESSES_ENV.split(",")]

logging_time_total = 0.0
counter_time_total = 0.0
http_client = None
hz_client = None
counter_queue = None

async def register_at_config_server():
    try:
        await http_client.post(f"{CONFIG_SERVER_URL}/register", json={
            "service_name": "facade-service",
            "address": MY_ADDRESS
        })
        logger.info("FACADE: Registered at config-server")
    except Exception as e:
        logger.error(f"FACADE: Failed to register at config-server: {e}")

async def get_service_addresses(service_name: str) -> list:
    try:
        resp = await http_client.get(f"{CONFIG_SERVER_URL}/services/{service_name}")
        return resp.json().get("addresses", [])
    except Exception as e:
        logger.error(f"FACADE: Failed to fetch {service_name} addresses: {e}")
        return []

async def get_service_url(service_name: str) -> str:
    addresses = await get_service_addresses(service_name)
    if not addresses:
        raise HTTPException(status_code=503, detail=f"No instances available for {service_name}")
    return f"http://{random.choice(addresses)}"

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client, hz_client, counter_queue
    limits = httpx.Limits(max_keepalive_connections=100, max_connections=200)
    http_client = httpx.AsyncClient(limits=limits, timeout=15.0)
    
    try:
        hz_client = hazelcast.HazelcastClient(
            cluster_name="dev",  
            cluster_members=HZ_ADDRESSES,
            reconnect_mode=hazelcast.config.ReconnectMode.ON
        )
        counter_queue = hz_client.get_queue("counter-queue").blocking()
        logger.info("FACADE: Connected to Hazelcast Queue")
    except Exception as e:
        logger.error(f"FACADE: Hazelcast connection failed: {e}")

    await register_at_config_server()
    yield
    await http_client.aclose()
    if hz_client:
        hz_client.shutdown()

app = FastAPI(title="Facade Service", lifespan=lifespan)

class ClientRequest(BaseModel):
    user_Id: str
    amount: float

async def send_to_logging_with_failover(payload: dict) -> str:
    global logging_time_total
    
    raw_addresses = await get_service_addresses("logging-service")
    if not raw_addresses:
        raise HTTPException(status_code=503, detail="No Logging Services registered")
        
    available_servers = [f"http://{addr}" for addr in raw_addresses]
    random.shuffle(available_servers)
    
    for server_url in available_servers:
        start_log_time = time.time()
        try:
            await http_client.post(f"{server_url}/log", json=payload)
            logging_time_total += (time.time() - start_log_time)
            return server_url
        except Exception as e:
            logger.warning(f"FACADE_WARN: Server {server_url} failed. Error: {e}")
            continue
            
    raise HTTPException(status_code=503, detail="All Logging Services are unavailable")

@app.post("/")
async def process_transaction(req: ClientRequest):
    global counter_time_total

    transaction_id = str(uuid.uuid4())
    current_timestamp = datetime.utcnow().isoformat()
    
    payload = {
        "transaction_Id": transaction_id,
        "timestamp": current_timestamp,
        "user_Id": req.user_Id,
        "amount": req.amount
    }
    
    used_logger = await send_to_logging_with_failover(payload)
    logger.info(f"FACADE_LOG: transaction saved by {used_logger}")
    
    start_count_time = time.time()
    try:
        if counter_queue:
            counter_queue.offer(json.dumps(payload))
            logger.info("FACADE: Message sent to Hazelcast Queue for counter-service")
        else:
            raise Exception("Queue not initialized")
    except Exception as e:
        logger.error(f"FACADE_ERROR: Failed to send to MQ: {e}")
        raise HTTPException(status_code=500, detail="Failed to queue transaction")
    
    counter_time_total += (time.time() - start_count_time)
        
    return {
        "transaction_Id": transaction_id, 
        "timestamp": current_timestamp, 
        "status": "queued_successfully"
    }

@app.get("/user/{user_Id}")
async def get_user_data(user_Id: str):
    try:
        counter_url = await get_service_url("counter-service")
        balance_resp = await http_client.get(f"{counter_url}/balance/{user_Id}")
        balance_data = balance_resp.json().get("balance", None)
    except Exception as e:
        logger.error(f"FACADE_ERROR: Counter Service GET failed: {e}")
        balance_data = None 

    all_logs = {}
    raw_addresses = await get_service_addresses("logging-service")
    available_servers = [f"http://{addr}" for addr in raw_addresses]
    random.shuffle(available_servers)
    
    logs_fetched = False
    for server_url in available_servers:
        try:
            logs_resp = await http_client.get(f"{server_url}/logs")
            all_logs = logs_resp.json()
            logs_fetched = True
            logger.info(f"FACADE_LOG: Logs fetched from {server_url}")
            break
        except Exception as e:
            logger.warning(f"FACADE_WARN: Failed to fetch logs from {server_url}: {e}")
            continue

    if logs_fetched:
        user_transactions = [log for log in all_logs.values() if log.get("user_Id") == user_Id]
    else:
        user_transactions = ["Error retrieving logs"]
        
    return {
        "balance": balance_data, 
        "transactions": user_transactions
    }

@app.get("/accounts")
async def get_all_accounts():
    try:
        counter_url = await get_service_url("counter-service")
        resp = await http_client.get(f"{counter_url}/balances")
        return resp.json()
    except Exception as e:
        logger.error(f"FACADE_ERROR: Failed to fetch all balances: {e}")
        raise HTTPException(status_code=500, detail=f"Counter Service Error: {e}")

@app.get("/metrics")
async def get_metrics():
    return {
        "logging_time_total_seconds": logging_time_total,
        "counter_time_total_seconds": counter_time_total
    }

@app.post("/metrics/reset")
async def reset_metrics():
    global logging_time_total, counter_time_total
    logging_time_total = 0.0
    counter_time_total = 0.0
    logger.info("FACADE_METRICS: Metrics reset successful")
    return {"status": "Metrics reset successful"}