import uuid
import time
import os
import random
import logging
from datetime import datetime
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

LOGGING_SERVICES_ENV = os.getenv("LOGGING_SERVICES", "http://logging-service:8000")
LOGGING_SERVICES = [url.strip() for url in LOGGING_SERVICES_ENV.split(",")]
COUNTER_SERVICE_URL = os.getenv("COUNTER_SERVICE", "http://counter-service:8000")
logging_time_total = 0.0
counter_time_total = 0.0
http_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    limits = httpx.Limits(max_keepalive_connections=100, max_connections=200)
    http_client = httpx.AsyncClient(limits=limits, timeout=15.0)
    logger.info(f"FACADE_INIT: counter_url={COUNTER_SERVICE_URL}, logging_urls={LOGGING_SERVICES}")
    yield
    await http_client.aclose()

app = FastAPI(title="Facade Service", lifespan=lifespan)

class ClientRequest(BaseModel):
    user_Id: str
    amount: float

async def send_to_logging_with_failover(payload: dict) -> str:
    global logging_time_total
    available_servers = list(LOGGING_SERVICES)
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
        count_response = await http_client.post(f"{COUNTER_SERVICE_URL}/transaction", json=payload)
        count_data = count_response.json()
        balance = count_data.get("current_balance")
    except Exception as e:
        logger.error(f"FACADE_ERROR: Counter Service failed: {e}")
        raise HTTPException(status_code=500, detail=f"Counter Service Connection Error: {e}")
    
    counter_time_total += (time.time() - start_count_time)
        
    return {
        "transaction_Id": transaction_id, 
        "timestamp": current_timestamp, 
        "balance": balance
    }

@app.get("/user/{user_Id}")
async def get_user_data(user_Id: str):
    try:
        balance_resp = await http_client.get(f"{COUNTER_SERVICE_URL}/balance/{user_Id}")
        balance_data = balance_resp.json().get("balance", 0.0)
    except Exception as e:
        logger.error(f"FACADE_ERROR: Failed to fetch balance for user={user_Id}: {e}")
        balance_data = "Error retrieving balance"

    all_logs = {}
    available_servers = list(LOGGING_SERVICES)
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
        resp = await http_client.get(f"{COUNTER_SERVICE_URL}/balances")
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