from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Logging Service")

logs = {}

class TransactionMessage(BaseModel):
    transaction_Id: str
    user_Id: str
    amount: float
    timestamp: str = None

@app.post("/log")
async def log_transaction(msg: TransactionMessage):
    logs[msg.transaction_Id] = msg.dict()
    return {"status": "success", "transaction_Id": msg.transaction_Id}

@app.get("/logs")
async def get_logs():
    return logs