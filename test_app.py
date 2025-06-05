import os
import time
import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient

os.environ["RATE_LIMIT_INTERVAL"] = "0.1"

from app import RATE_LIMIT_INTERVAL, app, ingestion_requests, lock, priority_queue, global_sequence

@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c

@pytest.fixture(autouse=True)
def reset_state():
    global global_sequence
    with lock:
        priority_queue.clear()
        ingestion_requests.clear()
        global_sequence = 0  

def wait_for_status(client, ingestion_id, desired_status="completed", timeout=10):
    start = time.time()
    while time.time() - start < timeout:
        resp = client.get(f"/status/{ingestion_id}")
        data = resp.json()
        if data["status"] == desired_status:
            return data
        time.sleep(0.3)
    raise TimeoutError(f"Timeout waiting for status '{desired_status}' for ingestion_id: {ingestion_id}")

def test_ingest_success(client):
    response = client.post("/ingest", json={"ids": [1, 2, 3], "priority": "HIGH"})
    assert response.status_code == 200
    assert "ingestion_id" in response.json()

def test_status_lifecycle(client):
    ingest_response = client.post("/ingest", json={"ids": [1, 2], "priority": "MEDIUM"})
    ingestion_id = ingest_response.json()["ingestion_id"]

    status_response = client.get(f"/status/{ingestion_id}")
    data = status_response.json()
    assert data["status"] == "yet_to_start"
    assert all(b["status"] == "yet_to_start" for b in data["batches"])

    data = wait_for_status(client, ingestion_id)
    assert data["status"] == "completed"
    assert all(b["status"] == "completed" for b in data["batches"])

def test_priority_handling(client):
    med_response = client.post("/ingest", json={"ids": [1, 2, 3, 4, 5], "priority": "MEDIUM"})
    med_id = med_response.json()["ingestion_id"]

    time.sleep(1)  
    high_response = client.post("/ingest", json={"ids": [6, 7, 8, 9], "priority": "HIGH"})
    high_id = high_response.json()["ingestion_id"]

    high_data = wait_for_status(client, high_id)
    assert any(b["status"] == "completed" for b in high_data["batches"])

    med_data = client.get(f"/status/{med_id}").json()
    assert any(b["status"] in ["triggered", "yet_to_start", "completed"] for b in med_data["batches"])
    
    med_data = wait_for_status(client, med_id)
    assert all(b["status"] == "completed" for b in med_data["batches"])

def test_rate_limiting(client):
    client.post("/ingest", json={"ids": [1, 2, 3], "priority": "HIGH"})
    client.post("/ingest", json={"ids": [4, 5, 6], "priority": "HIGH"})

    start_time = time.time()
    time.sleep(RATE_LIMIT_INTERVAL * 2 + 0.5)
    assert time.time() - start_time >= RATE_LIMIT_INTERVAL * 2

def test_batch_combination(client):
    client.post("/ingest", json={"ids": [1, 2], "priority": "MEDIUM"})
    client.post("/ingest", json={"ids": [3], "priority": "HIGH"})

    time.sleep(RATE_LIMIT_INTERVAL + 0.5)

    with lock:
        for req in ingestion_requests.values():
            for batch in req["batches"].values():
                assert batch["status"] == "completed"