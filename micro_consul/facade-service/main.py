import uuid
import time
import os
import random
import logging
import json
import asyncio
from datetime import datetime
import httpx
import hazelcast
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CONSUL_URL = os.getenv("CONSUL_URL", "http://consul:8500")
MY_HOST = os.getenv("MY_HOST", "facade-service")
MY_PORT = int(os.getenv("MY_PORT", "8080"))
INSTANCE_ID = os.getenv("MY_HOST", "default-service")

logging_time_total = 0.0
counter_time_total = 0.0
http_client = None
hz_client = None
counter_queue = None

async def fetch_config_from_consul(key: str) -> str:
    try:
        resp = await http_client.get(f"{CONSUL_URL}/v1/kv/{key}?raw")
        if resp.status_code == 200: return resp.text.strip()
    except Exception as e:
        logger.error(f"FACADE: Consul configuration read error: {e}")
    return "hazelcast1:5701"

async def register_in_consul():
    payload = {
        "ID": INSTANCE_ID,
        "Name": "facade-service",
        "Address": MY_HOST,
        "Port": MY_PORT,
        "Check": {
            "HTTP": f"http://{MY_HOST}:{MY_PORT}/health",
            "Interval": "10s"
        }
    }
    await http_client.put(f"{CONSUL_URL}/v1/agent/service/register", json=payload)

async def get_healthy_instances(service_name: str) -> list:
    try:
        # Питаємо Consul тільки про працездатні інстанси (passing=true)
        resp = await http_client.get(f"{CONSUL_URL}/v1/health/service/{service_name}?passing=true")
        data = resp.json()
        addresses = []
        for entry in data:
            srv = entry.get("Service", {})
            addresses.append(f"{srv.get('Address')}:{srv.get('Port')}")
        return addresses
    except Exception as e:
        logger.error(f"FACADE_DISCOVERY_ERROR: Failed to discover '{service_name}': {e}")
        return []

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client, hz_client, counter_queue
    http_client = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=100, max_connections=200), timeout=5.0)
    
    await register_in_consul()
    
    hz_addresses_str = await fetch_config_from_consul("config/shared/hz_addresses")
    hz_addresses = [addr.strip() for addr in hz_addresses_str.split(",")]
    
    try:
        hz_client = hazelcast.HazelcastClient(cluster_name="dev", cluster_members=hz_addresses)
        counter_queue = hz_client.get_queue("counter-queue").blocking()
    except Exception as e:
        logger.error(f"FACADE: Hazelcast connection error: {e}")

    yield
    await http_client.aclose()
    if hz_client: hz_client.shutdown()

app = FastAPI(title="Facade Service", lifespan=lifespan)

class ClientRequest(BaseModel):
    user_Id: str
    amount: float

@app.get("/health")
async def health(): return {"status": "UP"}

@app.post("/")
async def process_transaction(req: ClientRequest):
    global counter_time_total, logging_time_total

    transaction_id = str(uuid.uuid4())
    payload = {
        "transaction_Id": transaction_id,
        "timestamp": datetime.utcnow().isoformat(),
        "user_Id": req.user_Id,
        "amount": req.amount
    }
    
    # Динамічний пошук та Failover для logging-service
    raw_addresses = await get_healthy_instances("logging-service")
    if not raw_addresses:
        raise HTTPException(status_code=503, detail="No active logging services found in Consul")
    
    random.shuffle(raw_addresses)
    logged_successfully = False
    
    for addr in raw_addresses:
        start_log_time = time.time()
        try:
            await http_client.post(f"http://{addr}/log", json=payload)
            logging_time_total += (time.time() - start_log_time)
            logged_successfully = True
            break
        except Exception as e:
            logger.warning(f"FACADE_FAILOVER: Node {addr} failed, attempting next. Error: {e}")
            continue

    if not logged_successfully:
        raise HTTPException(status_code=503, detail="All discovered Logging instances failed.")

    # Відправка в MQ
    start_count_time = time.time()
    if counter_queue:
        await asyncio.to_thread(counter_queue.offer, json.dumps(payload))
    counter_time_total += (time.time() - start_count_time)
        
    return {"transaction_Id": transaction_id, "status": "queued_successfully"}

@app.get("/user/{user_Id}")
async def get_user_data(user_Id: str):
    # Динамічний пошук counter-service
    counter_instances = await get_healthy_instances("counter-service")
    if not counter_instances: raise HTTPException(status_code=503, detail="Counter service offline")
    
    counter_url = f"http://{random.choice(counter_instances)}"
    balance_resp = await http_client.get(f"{counter_url}/balance/{user_Id}")
    
    # Збір логів з випадкового живого логера
    logger_instances = await get_healthy_instances("logging-service")
    all_logs = {}
    if logger_instances:
        try:
            logs_resp = await http_client.get(f"http://{random.choice(logger_instances)}/logs")
            all_logs = logs_resp.json()
        except Exception: pass

    user_transactions = [log for log in all_logs.values() if log.get("user_Id") == user_Id]
    return {"balance": balance_resp.json().get("balance"), "transactions": user_transactions}

@app.get("/metrics")
async def get_metrics():
    return {"logging_time_total_seconds": logging_time_total, "counter_time_total_seconds": counter_time_total}