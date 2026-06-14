import os
import logging
import asyncio
import json
import httpx
import hazelcast
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncpg

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_URL = os.getenv("DB_URL", "postgresql://user:password@postgres-db:5432/counter_db")
if DB_URL.startswith("jdbc:"):
    DB_URL = DB_URL.replace("jdbc:", "")

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER", "http://config-server:8000")
MY_ADDRESS = os.getenv("MY_ADDRESS", "counter-service:8000")
HZ_ADDRESSES_ENV = os.getenv("HZ_ADDRESSES", "hz1:5701")
HZ_ADDRESSES = [addr.strip() for addr in HZ_ADDRESSES_ENV.split(",")]

db_pool = None
hz_client = None

async def register_at_config_server():
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{CONFIG_SERVER_URL}/register", json={
                "service_name": "counter-service",
                "address": MY_ADDRESS
            })
            logger.info("COUNTER: Registered at config-server")
        except Exception as e:
            logger.error(f"COUNTER: Failed to register at config-server: {e}")

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
            logger.info("COUNTER: Database ready.")
            break
        except Exception as e:
            logger.warning(f"DB connection failed. Retry {attempt}/5.")
            await asyncio.sleep(3)

    try:
        hz_client = hazelcast.HazelcastClient(
            cluster_name="dev", 
            cluster_members=HZ_ADDRESSES,
            reconnect_mode=hazelcast.config.ReconnectMode.ON
        )
    except Exception as e:
        logger.error(f"COUNTER: Hazelcast connection failed: {e}")

    await register_at_config_server()
    
    consumer_task = asyncio.create_task(queue_consumer_task())
    
    yield

    consumer_task.cancel()
    if db_pool:
        await db_pool.close()
    if hz_client:
        hz_client.shutdown()

app = FastAPI(title="Counter Service", lifespan=lifespan)

@app.get("/balance/{user_Id}")
async def get_user_balance(user_Id: str):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not ready")

    try:
        async with db_pool.acquire() as conn:
            query = "SELECT balance FROM user_balances WHERE user_id = $1;"
            balance = await conn.fetchval(query, user_Id)
            
            if balance is None:
                return {"user_Id": user_Id, "balance": 0.0}
            return {"user_Id": user_Id, "balance": float(balance)}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal Database Error")