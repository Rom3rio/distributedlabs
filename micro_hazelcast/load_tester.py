import time
import requests
from concurrent.futures import ThreadPoolExecutor

FACADE_URL = "http://localhost:8080"
NUM_CLIENTS = 10
REQUESTS_PER_CLIENT = 10000

def client_worker(user_id):
    payload = {"user_Id": user_id, "amount": 1.0}
    
    with requests.Session() as session:
        for _ in range(REQUESTS_PER_CLIENT):
            session.post(f"{FACADE_URL}/", json=payload)

def run_scenario(scenario_name, shared_account):
    print(f" Starting: {scenario_name}")
 
    requests.post(f"{FACADE_URL}/metrics/reset")
    
    print(f"Sending {NUM_CLIENTS * REQUESTS_PER_CLIENT} requests.")
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=NUM_CLIENTS) as executor:
        for i in range(NUM_CLIENTS):
            user_id = "shared_user" if shared_account else f"user_{i}"
            executor.submit(client_worker, user_id)
            
    total_time = time.time() - start_time
    total_requests = NUM_CLIENTS * REQUESTS_PER_CLIENT
    rps = total_requests / total_time

    metrics = requests.get(f"{FACADE_URL}/metrics").json()
    log_time = metrics.get("logging_time_total_seconds", 0)
    count_time = metrics.get("counter_time_total_seconds", 0)
    
    print("\nResults:")
    print(f"Total time:      {total_time:.2f} seconds")  
    print(f"Requests per second (RPS):       {rps:.2f} req/s")  
    print("-" * 50)
    print(f"logging-service contribution:    {log_time:.2f} seconds")  
    print(f"counter-service contribution:    {count_time:.2f} seconds")  

if __name__ == "__main__":
    run_scenario("Scenario #1: 10 clients -> 10 accounts", shared_account=False)
    run_scenario("Scenario #2: 10 clients -> 1 account", shared_account=True)