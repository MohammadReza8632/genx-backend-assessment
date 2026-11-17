import asyncio
import os
import time

import aiosqlite

DB_PATH = os.getenv("WEBHOOK_DB_PATH", "webhooks.db")

_db: aiosqlite.Connection | None = None   # ONE shared connection for the whole app
_lock = asyncio.Lock()                    # FIFO queue for every DB operation


async def init_db():
    global _db
    path = os.getenv("WEBHOOK_DB_PATH", DB_PATH)
    _db = await aiosqlite.connect(path, timeout=10)
    _db.row_factory = aiosqlite.Row
    await _db.execute("PRAGMA journal_mode = WAL")
    await _db.execute("PRAGMA synchronous = NORMAL")   # no disk-sync on every commit
    await _db.execute("PRAGMA busy_timeout = 10000")
    await _db.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            idempotency_key TEXT NOT NULL UNIQUE,
            event_id TEXT NOT NULL UNIQUE,
            tenant_id TEXT NOT NULL,
            cleaned_text TEXT NOT NULL,
            fingerprint TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt_at REAL NOT NULL DEFAULT 0,
            last_error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Crash recovery: anything left "processing" goes back in the queue
    await _db.execute("UPDATE events SET status = 'pending' WHERE status = 'processing'")
    await _db.commit()


async def close_db():
    global _db
    if _db is not None:
        await _db.close()
        _db = None


async def _write(sql: str, params: tuple = ()) -> int:
    """Run one write + commit while holding the lock. Returns rowcount."""
    async with _lock:
        cur = await _db.execute(sql, params)
        rowcount = cur.rowcount
        await _db.commit()
        return rowcount


async def save_event(
    idempotency_key: str, event_id: str, tenant_id: str, cleaned_text: str
) -> bool:
    """True if a NEW row was created, False if it was a duplicate."""
    rowcount = await _write(
        """
        INSERT INTO events (idempotency_key, event_id, tenant_id, cleaned_text, status)
        VALUES (?, ?, ?, ?, 'pending')
        ON CONFLICT DO NOTHING
        """,
        (idempotency_key, event_id, tenant_id, cleaned_text),
    )
    return rowcount == 1


async def claim_next_event() -> dict | None:
    async with _lock:
        cur = await _db.execute(
            """
            UPDATE events SET status = 'processing'
            WHERE id = (
                SELECT id FROM events
                WHERE status = 'pending' AND next_attempt_at <= ?
                ORDER BY id LIMIT 1
            )
            RETURNING id, event_id, cleaned_text, fingerprint, attempts
            """,
            (time.time(),),
        )
        rows = await cur.fetchall()
        await _db.commit()
        return dict(rows[0]) if rows else None


async def save_fingerprint(row_id: int, fingerprint: str):
    await _write("UPDATE events SET fingerprint = ? WHERE id = ?", (fingerprint, row_id))


async def schedule_retry(row_id: int, attempts: int, delay: float, error: str):
    await _write(
        """
        UPDATE events
        SET status = 'pending', attempts = ?, next_attempt_at = ?, last_error = ?
        WHERE id = ?
        """,
        (attempts, time.time() + delay, error[:500], row_id),
    )


async def mark_failed(row_id: int, error: str):
    await _write(
        "UPDATE events SET status = 'failed', last_error = ? WHERE id = ?",
        (error[:500], row_id),
    )


async def mark_delivered(event_id: str):
    await _write("UPDATE events SET status = 'delivered' WHERE event_id = ?", (event_id,))