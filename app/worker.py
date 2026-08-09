import asyncio
import json
import time
import uuid

import httpx

from app import analytics, cache, db, storage
from app.config import settings
from app.queue import connect


async def _load_job(delivery_id: str):
    pool = await db.get_pool()
    return await pool.fetchrow(
        """SELECT d.id AS delivery_id, d.event_id, d.endpoint_id, d.attempts,
                  e.body_key, e.headers, ep.destination_url
             FROM deliveries d
             JOIN events e   ON e.id = d.event_id
             JOIN endpoints ep ON ep.id = d.endpoint_id
            WHERE d.id = $1""",
        uuid.UUID(delivery_id),
    )


async def _attempt_delivery(job) -> tuple[bool, int, int, str]:
    body = await storage.get_payload(job["body_key"]) if job["body_key"] else b""
    headers = json.loads(job["headers"]) if job["headers"] else {}
    fwd = {k: v for k, v in headers.items()
           if k.lower() not in ("host", "content-length", "connection")}

    start = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=settings.delivery_timeout_seconds) as client:
            resp = await client.post(job["destination_url"], content=body, headers=fwd)
        latency_ms = int((time.perf_counter() - start) * 1000)
        return (200 <= resp.status_code < 300, resp.status_code, latency_ms, "")
    except Exception as exc:  # noqa: BLE001
        latency_ms = int((time.perf_counter() - start) * 1000)
        return (False, 0, latency_ms, str(exc)[:200])


async def handle(msg) -> None:
    pool = await db.get_pool()
    data = json.loads(msg.data.decode())
    delivery_id = data["delivery_id"]
    job = await _load_job(delivery_id)
    if job is None:
        await msg.ack()
        return

    endpoint_id = str(job["endpoint_id"])
    attempt_no = job["attempts"] + 1
    success, status_code, latency_ms, error = await _attempt_delivery(job)

    await pool.execute(
        """UPDATE deliveries
              SET attempts = $2, last_status_code = $3, last_error = $4,
                  last_attempt_at = now(),
                  status = CASE WHEN $5 THEN 'success' ELSE status END
            WHERE id = $1""",
        uuid.UUID(delivery_id), attempt_no, status_code or None, error or None, success,
    )
    # best-effort: a ClickHouse hiccup must never break delivery
    try:
        await asyncio.to_thread(
            analytics.record_attempt,
            endpoint_id=endpoint_id, event_id=str(job["event_id"]), delivery_id=delivery_id,
            attempt_no=attempt_no, status_code=status_code, latency_ms=latency_ms,
            success=success, error=error,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[worker] analytics insert failed (non-fatal): {exc}", flush=True)

    if success:
        await cache.bump(endpoint_id, "delivered")
        await msg.ack()
        return

    if attempt_no >= settings.max_attempts:
        await pool.execute("UPDATE deliveries SET status='dead' WHERE id=$1", uuid.UUID(delivery_id))
        await cache.bump(endpoint_id, "dead")
        await msg.ack()
        return

    # retry: NAK with exponential backoff so JetStream redelivers
    await pool.execute("UPDATE deliveries SET status='failed' WHERE id=$1", uuid.UUID(delivery_id))
    await cache.bump(endpoint_id, "failed")
    delay = settings.base_backoff_seconds * (2 ** (attempt_no - 1))
    await msg.nak(delay=delay)


async def main() -> None:
    await db.init_schema()
    js = await connect()
    sub = await js.pull_subscribe(settings.delivery_subject, durable=settings.delivery_durable)
    print("[worker] delivery worker started, waiting for jobs…", flush=True)
    while True:
        try:
            msgs = await sub.fetch(batch=10, timeout=5)
        except Exception:
            continue
        # return_exceptions so one bad message can't kill the loop; it stays unacked and is redelivered
        results = await asyncio.gather(*(handle(m) for m in msgs), return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                print(f"[worker] handle error (message will be redelivered): {r}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
