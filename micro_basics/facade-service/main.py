import uuid
import time
from datetime import datetime
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager

LOGGING_SERVICE_URL = "http://logging-service:8000"
COUNTER_SERVICE_URL = "http://counter-service:8000"
logging_time_total = 0.0
counter_time_total = 0.0
http_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    limits = httpx.Limits(max_keepalive_connections=100, max_connections=200)
    http_client = httpx.AsyncClient(limits=limits, timeout=15.0)
    yield
    await http_client.aclose()

app = FastAPI(title="Facade Service", lifespan=lifespan)

class ClientRequest(BaseModel):
    user_Id: str
    amount: float

@app.post("/")
async def process_transaction(req: ClientRequest):

    global logging_time_total, counter_time_total

    transaction_id = str(uuid.uuid4())
    current_timestamp = datetime.utcnow().isoformat()
    
    payload = {
        "transaction_Id": transaction_id,
        "timestamp": current_timestamp,
        "user_Id": req.user_Id,
        "amount": req.amount
    }
    
    start_log_time = time.time()
    try:
        await http_client.post(f"{LOGGING_SERVICE_URL}/log", json=payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Logging Service Connection Error: {e}")
    logging_time_total += (time.time() - start_log_time)
    
    start_count_time = time.time()
    try:
        count_response = await http_client.post(f"{COUNTER_SERVICE_URL}/transaction", json=payload)
        count_data = count_response.json()
        balance = count_data.get("current_balance")
    except Exception as e:
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
    except Exception:
        balance_data = "Error retrieving balance"
        
    try:
        logs_resp = await http_client.get(f"{LOGGING_SERVICE_URL}/logs")
        all_logs = logs_resp.json()
        user_transactions = [log for log in all_logs.values() if log.get("user_Id") == user_Id]
    except Exception:
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
    return {"status": "Metrics reset successful"}