from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Counter Service")

balances = {}

class TransactionMessage(BaseModel):
    user_Id: str
    amount: float
    transaction_Id: str = None   

@app.post("/transaction")
async def update_balance(msg: TransactionMessage):

    if msg.user_Id not in balances:
        balances[msg.user_Id] = 0.0
        
    balances[msg.user_Id] += msg.amount 
    return {"status": "success", "user_Id": msg.user_Id, "current_balance": balances[msg.user_Id]}

@app.get("/balance/{user_Id}")
async def get_user_balance(user_Id: str):
    balance = balances.get(user_Id, 0.0)
    return {"user_Id": user_Id, "balance": balance}

@app.get("/balances")
async def get_all_balances():
    return balances