import asyncio, cbor2, logging
from fastapi import FastAPI
from app.db import connect_db, close_db, insert_telemetry, insert_dlq
from app.mqtt_client import MQTTIngestor
from app.schemas import TelemetryMessage
from app.routers.telemetry import router as telemetry_router

app = FastAPI(title="Telemetry Ingest Service")

queue: asyncio.Queue = asyncio.Queue()
mqtt: MQTTIngestor | None = None
consumer_task: asyncio.Task | None = None

async def consumer():
    while True:
        payload_bytes, topic = await queue.get()
        try:
            obj = cbor2.loads(payload_bytes)
            msg = TelemetryMessage(**obj)  # validate schema
            await insert_telemetry(msg.as_db_dict, topic)
        except Exception as e:
            # Pass raw bytes to DLQ on failure
            await insert_dlq(payload_bytes, topic, str(e))
            logging.exception("DLQ insert")
        finally:
            queue.task_done()

@app.on_event("startup")
async def on_startup():
    global mqtt, consumer_task
    logging.basicConfig(level=logging.INFO)
    await connect_db()
    consumer_task = asyncio.create_task(consumer())
    mqtt = MQTTIngestor(queue)
    mqtt.start()

@app.on_event("shutdown")
async def on_shutdown():
    if mqtt:
        logging.info("Stopping MQTT client...")
        mqtt.stop()

    logging.info("Waiting for consumer queue to empty...")
    await queue.join() # Wait for all items to be processed

    if consumer_task:
        logging.info("Cancelling consumer task...")
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            logging.info("Consumer task cancelled successfully.")
    await close_db()

@app.get("/healthz")
async def healthz():
    return {"ok": True}

app.include_router(telemetry_router)
