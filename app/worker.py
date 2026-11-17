import asyncio
import logging
import os
import random
from concurrent.futures import ProcessPoolExecutor

from app import database
from app.downstream import (
    PermanentDeliveryError,
    forward_event,
    make_client,
)
from app.processor import compute_fingerprint

log = logging.getLogger("webhook.worker")

NUM_WORKERS = 3        # events handled at the same time
MAX_ATTEMPTS = 40      # after this many failures an event becomes 'failed'
BASE_DELAY = 0.1       # backoff: 0.1s, 0.2s, 0.4s ... capped
MAX_DELAY = 5.0
IDLE_POLL = 0.25       # how long an idle worker sleeps before checking the DB again


class DeliveryWorker:
    def __init__(self):
        self._wakeup = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._pool: ProcessPoolExecutor | None = None
        self._client = None

    async def start(self):
        size = max(1, min(4, (os.cpu_count() or 2) - 1))
        self._pool = ProcessPoolExecutor(max_workers=size)
        self._client = make_client()
        self._tasks = [asyncio.create_task(self._run()) for _ in range(NUM_WORKERS)]

    async def stop(self):
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._client:
            await self._client.aclose()
        if self._pool:
            self._pool.shutdown(wait=True, cancel_futures=True)

    def wake(self):
        """Called by the request handler after saving a NEW event."""
        self._wakeup.set()

    async def _run(self):
        while True:
            try:
                event = await database.claim_next_event()
                if event is None:
                    self._wakeup.clear()
                    try:
                        await asyncio.wait_for(self._wakeup.wait(), IDLE_POLL)
                    except asyncio.TimeoutError:
                        pass
                    continue
                await self._process(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("worker loop error")
                await asyncio.sleep(0.5)

    async def _process(self, event: dict):
        try:
            fingerprint = event["fingerprint"]
            if fingerprint is None:
                loop = asyncio.get_running_loop()
                fingerprint = await loop.run_in_executor(
                    self._pool, compute_fingerprint, event["cleaned_text"]
                )
                await database.save_fingerprint(event["id"], fingerprint)

            await forward_event(
                self._client, event["event_id"], event["cleaned_text"], fingerprint
            )
        except PermanentDeliveryError as exc:
            log.error("event %s failed permanently: %s", event["event_id"], exc)
            await database.mark_failed(event["id"], str(exc))
            return
        except Exception as exc:  # transient downstream problem or anything unexpected
            attempts = event["attempts"] + 1
            if attempts >= MAX_ATTEMPTS:
                log.error("event %s gave up after %d attempts", event["event_id"], attempts)
                await database.mark_failed(event["id"], str(exc))
            else:
                delay = min(MAX_DELAY, BASE_DELAY * 2**attempts) * random.uniform(0.5, 1.0)
                log.warning(
                    "event %s attempt %d failed (%s); retry in %.2fs",
                    event["event_id"], attempts, exc, delay,
                )
                await database.schedule_retry(event["id"], attempts, delay, str(exc))
            return

        await database.mark_delivered(event["event_id"])