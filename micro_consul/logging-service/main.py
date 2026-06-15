import os
import logging
import asyncio
import uuid
import httpx
import hazelcast
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CONSUL_URL = os.getenv("CONSUL_URL", "http://consul:8500")
MY_HOST = os.getenv("MY_HOST", "logging-service-1")
MY_PORT = int(os.getenv("MY_PORT", "8000"))
INSTANCE_ID = os.getenv("MY_HOST", "default-service")

hz_client = None
distributed_logs_map = None

async def fetch_config_from_consul(key: str) -> str:
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{CONSUL_URL}/v1/kv/{key}?raw")
            if resp.status_code == 200:
                return resp.text.strip()
            raise Exception(f"Consul status: {resp.status_code}")
        except Exception as e:
            logger.error(f"LOGGING: Failed to fetch key '{key}' from Consul: {e}")
            return "hazelcast1:5701" # Фолбек

async def register_in_consul():
    async with httpx.AsyncClient() as client:
        registration_payload = {
            "ID": INSTANCE_ID,
            "Name": "logging-service",
            "Address": MY_HOST,
            "Port": MY_PORT,
            "Check": {
                "HTTP": f"http://{MY_HOST}:{MY_PORT}/health",
                "Interval": "10s",
                "DeregisterCriticalServiceAfter": "30s"
            }
        }
        try:
            await client.put(f"{CONSUL_URL}/v1/agent/service/register", json=registration_payload)
            logger.info(f"LOGGING: Registered in Consul as {INSTANCE_ID}")
        except Exception as e:
            logger.error(f"LOGGING: Consul registration failed: {e}")

async def deregister_from_consul():
    async with httpx.AsyncClient() as client:
        try:
            await client.put(f"{CONSUL_URL}/v1/agent/service/deregister/{INSTANCE_ID}")
            logger.info(f"LOGGING: Deregistered {INSTANCE_ID} from Consul.")
        except Exception as e:
            logger.error(f"LOGGING: Deregistration failed: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global hz_client, distributed_logs_map
    
    hz_addresses_str = await fetch_config_from_consul("config/shared/hz_addresses")
    hz_addresses = [addr.strip() for addr in hz_addresses_str.split(",")]
    logger.info(f"LOGGING_INIT: Fetched HZ config from Consul -> {hz_addresses}")
    
    for attempt in range(1, 6):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_name="dev",  
                cluster_members=hz_addresses,
                async_start=False,
                reconnect_mode=hazelcast.config.ReconnectMode.ON
            )
            distributed_logs_map = hz_client.get_map("distributed-transactions-logs").blocking()
            logger.info("LOGGING_INIT: Successfully connected to Hazelcast cluster.")
            break
        except Exception as e:
            logger.warning(f"Hazelcast attempt {attempt} failed. Retrying... Error: {e}")
            await asyncio.sleep(4)

    await register_in_consul()
    yield
    await deregister_from_consul()
    if hz_client:
        hz_client.shutdown()

app = FastAPI(title="Logging Service", lifespan=lifespan)

class TransactionMessage(BaseModel):
    transaction_Id: str
    user_Id: str
    amount: float
    timestamp: str = None

@app.get("/health")
async def health_check():
    return {"status": "UP", "instance_id": INSTANCE_ID}

@app.post("/log")
async def log_transaction(msg: TransactionMessage):
    if distributed_logs_map is None:
        raise HTTPException(status_code=503, detail="Hazelcast storage is unavailable")
    try:
        distributed_logs_map.put(msg.transaction_Id, msg.dict())
        logger.info(f"LOG_SAVED: tx={msg.transaction_Id}, user={msg.user_Id}")
        return {"status": "success", "instance": INSTANCE_ID}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/logs")
async def get_logs():
    if distributed_logs_map is None:
        raise HTTPException(status_code=503, detail="Hazelcast storage is unavailable")
    try:
        entry_set = distributed_logs_map.entry_set()
        return {key: value for key, value in entry_set}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))