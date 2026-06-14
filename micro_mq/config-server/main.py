from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict

app = FastAPI(title="Config Server")
registry: Dict[str, List[str]] = {}

class ServiceInfo(BaseModel):
    service_name: str
    address: str

@app.post("/register")
async def register_service(info: ServiceInfo):
    if info.service_name not in registry:
        registry[info.service_name] = []
    
    if info.address not in registry[info.service_name]:
        registry[info.service_name].append(info.address)
        
    print(f"Registered {info.service_name} at {info.address}")
    return {"status": "registered", "registry": registry}

@app.get("/services/{service_name}")
async def get_services(service_name: str):
    return {"addresses": registry.get(service_name, [])}