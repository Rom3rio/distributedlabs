import os
import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
import asyncpg

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_URL = os.getenv("DB_URL", "postgresql://user:password@postgres-db:5432/counter_db")
if DB_URL.startswith("jdbc:"):
    DB_URL = DB_URL.replace("jdbc:", "")

db_pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: manages database connection pool with retries."""
    global db_pool
    logger.info("Connecting to PostgreSQL database...")
    
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
            logger.info("Database initialized successfully. Table 'user_balances' is ready.")
            break
        except Exception as e:
            logger.warning(f"Database connection attempt {attempt} failed. Retrying in 3s... Error: {e}")
            if attempt == 5:
                logger.error("Max database connection attempts reached. Exiting.")
                raise e
            await asyncio.sleep(3)

    yield

    if db_pool:
        logger.info("Closing database connection pool...")
        await db_pool.close()


app = FastAPI(title="Counter Service", lifespan=lifespan)

class TransactionMessage(BaseModel):
    user_Id: str
    amount: float
    transaction_Id: str = None   


@app.post("/transaction")
async def update_balance(msg: TransactionMessage):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database pool not initialized")

    try:
        async with db_pool.acquire() as conn:
            query = """
                INSERT INTO user_balances (user_id, balance)
                VALUES ($1, $2)
                ON CONFLICT (user_id)
                DO UPDATE SET balance = user_balances.balance + EXCLUDED.balance
                RETURNING balance;
            """
            current_balance = await conn.fetchval(query, msg.user_Id, msg.amount)
            logger.info(f"COUNTER_UPDATE: user={msg.user_Id}, change={msg.amount}, balance={current_balance}")
            
            return {
                "status": "success", 
                "user_Id": msg.user_Id, 
                "current_balance": float(current_balance)
            }
            
    except Exception as e:
        logger.error(f"Error updating balance: {e}")
        raise HTTPException(status_code=500, detail="Internal Database Error")


@app.get("/balance/{user_Id}")
async def get_user_balance(user_Id: str):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database pool not initialized")

    try:
        async with db_pool.acquire() as conn:
            query = "SELECT balance FROM user_balances WHERE user_id = $1;"
            balance = await conn.fetchval(query, user_Id)
            
            if balance is None:
                return {"user_Id": user_Id, "balance": 0.0}
                
            return {"user_Id": user_Id, "balance": float(balance)}
            
    except Exception as e:
        logger.error(f"Error fetching balance: {e}")
        raise HTTPException(status_code=500, detail="Internal Database Error")


@app.get("/balances")
async def get_all_balances():
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database pool not initialized")

    try:
        async with db_pool.acquire() as conn:
            query = "SELECT user_id, balance FROM user_balances;"
            rows = await conn.fetch(query)
            
            mapped_balances = {row["user_id"]: float(row["balance"]) for row in rows}
            return mapped_balances
            
    except Exception as e:
        logger.error(f"Error fetching all balances: {e}")
        raise HTTPException(status_code=500, detail="Internal Database Error")