from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

from app.database import init_db, save_event, close_db
from app.processor import redact_pii
from app.worker import DeliveryWorker


class WebhookPayload(BaseModel):
    event_id: str
    tenant_id: str
    content: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    worker = DeliveryWorker()
    app.state.worker = worker
    await worker.start()
    yield
    await worker.stop()
    await close_db()


app = FastAPI(title="Webhook Ingestion Service", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/webhooks/ingest", status_code=202)
async def ingest_webhook(
    payload: WebhookPayload,
    request: Request,
    x_idempotency_key: str | None = Header(default=None),
):
    if not x_idempotency_key:
        raise HTTPException(status_code=400, detail="Missing X-Idempotency-Key header")

    cleaned_text = redact_pii(payload.content)  # microseconds, safe in the handler
    created = await save_event(
        x_idempotency_key, payload.event_id, payload.tenant_id, cleaned_text
    )
    if created:
        request.app.state.worker.wake()

    return {
        "status": "accepted" if created else "duplicate",
        "event_id": payload.event_id,
    }