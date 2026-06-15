import os
import logging
import asyncio
import json
import uuid
import httpx
import hazelcast
import asyncpg
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_URL = os.getenv("DB_URL", "postgresql://user:password@postgres-db:5432/counter_db")
CONSUL_URL = os.getenv("CONSUL_URL", "http://consul:8500")
MY_HOST = os.getenv("MY_HOST", "counter-service")
MY_PORT = int(os.getenv("MY_PORT", "8000"))
INSTANCE_ID = os.getenv("MY_HOST", "default-service")

db_pool = None
hz_client = None

async def fetch_config_from_consul(key: str) -> str:
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{CONSUL_URL}/v1/kv/{key}?raw")
            if resp.status_code == 200:
                return resp.text.strip()
            logger.warning(f"COUNTER: Consul returned status {resp.status_code} for key {key}")
        except Exception as e:
            logger.error(f"COUNTER: Failed to fetch key '{key}' from Consul: {e}")
        return "hazelcast1:5701"  

async def register_in_consul():
    async with httpx.AsyncClient() as client:
        payload = {
            "ID": INSTANCE_ID,
            "Name": "counter-service",
            "Address": MY_HOST,
            "Port": MY_PORT,
            "Check": {
                "HTTP": f"http://{MY_HOST}:{MY_PORT}/health",
                "Interval": "10s",
                "DeregisterCriticalServiceAfter": "30s"
            }
        }
        try:
            await client.put(f"{CONSUL_URL}/v1/agent/service/register", json=payload)
            logger.info(f"COUNTER: Successfully registered in Consul as {INSTANCE_ID}")
        except Exception as e:
            logger.error(f"COUNTER: Consul registration failed: {e}")

async def deregister_from_consul():
    async with httpx.AsyncClient() as client:
        try:
            await client.put(f"{CONSUL_URL}/v1/agent/service/deregister/{INSTANCE_ID}")
            logger.info(f"COUNTER: Deregistered {INSTANCE_ID} from Consul.")
        except Exception as e:
            logger.error(f"COUNTER: Deregistration failed: {e}")

async def process_queue_message(msg: dict):
    if not db_pool:
        return
    try:
        async with db_pool.acquire() as conn:
            query = """
                INSERT INTO user_balances (user_id, balance)
                VALUES ($1, $2)
                ON CONFLICT (user_id)
                DO UPDATE SET balance = user_balances.balance + EXCLUDED.balance
                RETURNING balance;
            """
            current_balance = await conn.fetchval(query, msg['user_Id'], msg['amount'])
            logger.info(f"COUNTER_MQ_UPDATE: user={msg['user_Id']}, change={msg['amount']}, balance={current_balance}")
    except Exception as e:
        logger.error(f"COUNTER_MQ_ERROR: Failed to update DB from MQ: {e}")

async def queue_consumer_task():
    logger.info("COUNTER: Starting background MQ consumer.")
    try:
        queue = hz_client.get_queue("counter-queue").blocking()
        while True:
            msg_str = await asyncio.to_thread(queue.take)
            msg_data = json.loads(msg_str)
            logger.info(f"COUNTER: Received from MQ -> {msg_data}")
            await process_queue_message(msg_data)
    except asyncio.CancelledError:
        logger.info("COUNTER: MQ consumer task cancelled.")
    except Exception as e:
        logger.error(f"COUNTER: MQ consumer task failed: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, hz_client
    
    for attempt in range(1, 6):
        try:
            db_pool = await asyncpg.create_pool(dsn=DB_URL, min_size=5, max_size=20)
            async with db_pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS user_balances (
                        user_id VARCHAR(100) PRIMARY KEY,
                        balance NUMERIC(12, 2) NOT NULL DEFAULT 0.0
                    );
                """)
            logger.info("COUNTER: Database ready and connected.")
            break
        except Exception as e:
            logger.warning(f"DB connection attempt {attempt}/5 failed. Retrying in 4s. Error: {e}")
            await asyncio.sleep(4)
    else:
        logger.error("COUNTER: Database is unreachable. Exiting startup.")
        raise RuntimeError("Database connection failed")

    hz_addresses_str = await fetch_config_from_consul("config/shared/hz_addresses")
    hz_addresses = [addr.strip() for addr in hz_addresses_str.split(",")]
    logger.info(f"COUNTER: Initializing Hazelcast with members: {hz_addresses}")

    try:
        hz_client = hazelcast.HazelcastClient(
            cluster_name="dev", 
            cluster_members=hz_addresses,
            reconnect_mode=hazelcast.config.ReconnectMode.ON
        )
    except Exception as e:
        logger.error(f"COUNTER: Hazelcast connection failed: {e}")
        raise e

    await register_in_consul()
    
    consumer_task = asyncio.create_task(queue_consumer_task())
    
    yield

    consumer_task.cancel()
    await deregister_from_consul()
    if db_pool:
        await db_pool.close()
    if hz_client:
        hz_client.shutdown()

app = FastAPI(title="Counter Service", lifespan=lifespan)

@app.get("/health")
async def health_check():
    return {"status": "UP", "instance_id": INSTANCE_ID}

@app.get("/balance/{user_Id}")
async def get_user_balance(user_Id: str):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database pool not active")
    try:
        async with db_pool.acquire() as conn:
            query = "SELECT balance FROM user_balances WHERE user_id = $1;"
            balance = await conn.fetchval(query, user_Id)
            if balance is None:
                return {"user_Id": user_Id, "balance": 0.0}
            return {"user_Id": user_Id, "balance": float(balance)}
    except Exception as e:
        logger.error(f"COUNTER_API_ERROR: {e}")
        raise HTTPException(status_code=500, detail="Internal Database Error")