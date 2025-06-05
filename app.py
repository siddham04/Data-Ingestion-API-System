import heapq
import threading
import time
import uuid
import os
import logging
from enum import Enum
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, conlist

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

RATE_LIMIT_INTERVAL = float(os.getenv("RATE_LIMIT_INTERVAL", 5))
BATCH_SIZE = 3

lock = threading.Lock()
priority_queue = []
ingestion_requests = {}
global_sequence = 0
PRIORITY_MAP = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}

class PriorityEnum(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

class IngestRequest(BaseModel):
    ids: conlist(int, min_length=1, max_length=1000)
    priority: PriorityEnum

app = FastAPI()

@app.post("/ingest")
def ingest(data: IngestRequest):
    global global_sequence
    ingestion_id = str(uuid.uuid4())
    batches = []

    for i in range(0, len(data.ids), BATCH_SIZE):
        batch_ids = data.ids[i : i + BATCH_SIZE]
        batch_id = str(uuid.uuid4())
        batches.append((batch_id, batch_ids))

    with lock:
        current_time = time.time()
        ingestion_requests[ingestion_id] = {
            "priority": data.priority,
            "created_time": current_time,
            "batches": {},
        }

        for batch_id, batch_ids in batches:
            ingestion_requests[ingestion_id]["batches"][batch_id] = {
                "ids": batch_ids,
                "status": "yet_to_start",
                "remaining_ids": set(batch_ids),
                "processed_ids": set(),
            }

            for id_val in batch_ids:
                heapq.heappush(priority_queue, (
                    PRIORITY_MAP[data.priority],
                    current_time,
                    global_sequence,
                    id_val,
                    ingestion_id,
                    batch_id,
                ))
                global_sequence += 1

        logger.debug(f"Ingestion created: {ingestion_id} with {len(batches)} batches")

    return {"ingestion_id": ingestion_id}

@app.get("/status/{ingestion_id}")
def get_status(ingestion_id: str):
    with lock:
        if ingestion_id not in ingestion_requests:
            raise HTTPException(status_code=404, detail="Ingestion ID not found")

        req = ingestion_requests[ingestion_id]
        status_counts = {"yet_to_start": 0, "triggered": 0, "completed": 0}
        batches_info = []

        for batch_id, batch in req["batches"].items():
            status = batch["status"]
            status_counts[status] += 1
            batches_info.append({
                "batch_id": batch_id,
                "ids": batch["ids"],
                "status": status,
                "progress": f"{len(batch['processed_ids'])}/{len(batch['ids'])}",
            })

        if status_counts["completed"] == len(req["batches"]):
            overall_status = "completed"
        elif status_counts["triggered"] > 0 or status_counts["completed"] > 0:
            overall_status = "in_progress"
        else:
            overall_status = "yet_to_start"

    return {
        "ingestion_id": ingestion_id,
        "status": overall_status,
        "batches": batches_info,
    }

def process_batch(batch_items):
    if not batch_items:
        return
    batch_groups = {}
    for item in batch_items:
        _, _, _, id_val, ing_id, batch_id = item
        key = (ing_id, batch_id)
        batch_groups.setdefault(key, []).append(id_val)

    with lock:
        for (ing_id, batch_id), ids in batch_groups.items():
            batch = ingestion_requests[ing_id]["batches"][batch_id]
            if batch["status"] == "yet_to_start":
                batch["status"] = "triggered"
                logger.debug(f"Batch {batch_id} triggered")
    time.sleep(0.1)
    with lock:
        for (ing_id, batch_id), ids in batch_groups.items():
            batch = ingestion_requests[ing_id]["batches"][batch_id]
            batch["processed_ids"].update(ids)
            batch["remaining_ids"].difference_update(ids)

            if not batch["remaining_ids"]:
                batch["status"] = "completed"
                logger.debug(f"Batch {batch_id} completed")

def worker():
    while True:
        batch_items = []

        with lock:
            while priority_queue and len(batch_items) < BATCH_SIZE:
                item = heapq.heappop(priority_queue)
                batch_items.append(item)
                logger.debug(f"Processing item: {item}")

        if batch_items:
            start_time = time.time()
            logger.debug(f"Processing batch of {len(batch_items)} items")
            process_batch(batch_items)
            elapsed = time.time() - start_time
            sleep_time = max(0, RATE_LIMIT_INTERVAL - elapsed)

            if sleep_time > 0:
                logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f}s")
                time.sleep(sleep_time)
        else:
            time.sleep(0.1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    worker_thread = threading.Thread(target=worker, daemon=True, name="batch_worker")
    worker_thread.start()
    logger.info("Worker thread started")
    yield
    logger.info("Shutting down worker thread")

app.router.lifespan_context = lifespan
