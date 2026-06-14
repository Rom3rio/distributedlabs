import os
import logging
import time
import httpx
import hazelcast
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

HZ_ADDRESSES_ENV = os.getenv("HZ_ADDRESSES", "hz1:5701")
HZ_ADDRESSES = [addr.strip() for addr in HZ_ADDRESSES_ENV.split(",")]
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER", "http://config-server:8000")
MY_ADDRESS = os.getenv("MY_ADDRESS", "logging-service:8000")

hz_client = None
distributed_logs_map = None

async def register_at_config_server():
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{CONFIG_SERVER_URL}/register", json={
                "service_name": "logging-service",
                "address": MY_ADDRESS
            })
            logger.info(f"LOGGING: Successfully registered at {CONFIG_SERVER_URL}")
        except Exception as e:
            logger.error(f"LOGGING: Registration failed: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client, distributed_logs_map
    logger.info(f"LOGGING_INIT: Connecting to Hazelcast cluster at {HZ_ADDRESSES}.")
    
    for attempt in range(1, 6):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_name="dev",  
                cluster_members=HZ_ADDRESSES,
                async_start=False,
                reconnect_mode=hazelcast.config.ReconnectMode.ON
            )
            distributed_logs_map = hz_client.get_map("distributed-transactions-logs").blocking()
            logger.info("LOGGING_INIT: Successfully connected to Hazelcast cluster.")
            break
        except Exception as e:
            logger.warning(f"Hazelcast connection attempt {attempt} failed. Retrying in 4s... Error: {e}")
            time.sleep(4)

    await register_at_config_server()
    yield

    if hz_client:
        logger.info("LOGGING_SHUTDOWN: Closing Hazelcast client.")
        hz_client.shutdown()

app = FastAPI(title="Logging Service", lifespan=lifespan)

class TransactionMessage(BaseModel):
    transaction_Id: str
    user_Id: str
    amount: float
    timestamp: str = None

@app.post("/log")
async def log_transaction(msg: TransactionMessage):
    if distributed_logs_map is None:
        raise HTTPException(status_code=503, detail="Hazelcast storage is unavailable")

    try:
        distributed_logs_map.put(msg.transaction_Id, msg.dict())
        logger.info(f"LOG_SAVED: tx={msg.transaction_Id}, user={msg.user_Id}, amount={msg.amount}")
        return {"status": "success", "transaction_Id": msg.transaction_Id}
    except Exception as e:
        logger.error(f"LOG_ERROR: Failed to write to Hazelcast: {e}")
        raise HTTPException(status_code=500, detail="Internal Cluster Storage Error")

@app.get("/logs")
async def get_logs():
    if distributed_logs_map is None:
        raise HTTPException(status_code=503, detail="Hazelcast storage is unavailable")

    try:
        entry_set = distributed_logs_map.entry_set()
        all_logs = {key: value for key, value in entry_set}
        return all_logs
    except Exception as e:
        logger.error(f"LOG_ERROR: Failed to read from Hazelcast: {e}")
        raise HTTPException(status_code=500, detail="Internal Cluster Storage Error")