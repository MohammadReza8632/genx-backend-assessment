import os

import httpx

DOWNSTREAM_URL = os.getenv("DOWNSTREAM_URL", "http://127.0.0.1:9001/events")


class TransientDeliveryError(Exception):
    """Worth retrying: 5xx, 429, timeouts, connection problems."""


class PermanentDeliveryError(Exception):
    """Retrying will not help: 4xx responses."""


def make_client() -> httpx.AsyncClient:
    # ONE shared client (connection reuse) with a hard timeout so a hung downstream can't hang us
    return httpx.AsyncClient(timeout=httpx.Timeout(5.0))


async def forward_event(
    client: httpx.AsyncClient, event_id: str, cleaned_text: str, fingerprint: str
) -> dict:
    try:
        response = await client.post(
            DOWNSTREAM_URL,
            json={"event_id": event_id, "text": cleaned_text, "fingerprint": fingerprint},
            headers={"Idempotency-Key": event_id},
        )
    except httpx.TransportError as exc:
        raise TransientDeliveryError(f"transport error: {exc!r}") from exc

    if response.status_code >= 500 or response.status_code == 429:
        raise TransientDeliveryError(f"downstream returned {response.status_code}")
    if response.status_code >= 400:
        raise PermanentDeliveryError(f"downstream returned {response.status_code}")

    try:
        return response.json()
    except ValueError:
        return {}