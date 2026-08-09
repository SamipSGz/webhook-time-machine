import asyncio
import json
import secrets
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

from app import analytics, cache, db, queue, storage
from app.config import settings

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    # core deps: ingestion cannot work without these
    await db.init_schema()
    await queue.connect()
    # non-critical: never let analytics/storage init take down the API
    for label, coro in (("storage", storage.ensure_bucket()),
                        ("clickhouse", asyncio.to_thread(analytics.init_schema))):
        try:
            await coro
        except Exception as exc:  # noqa: BLE001
            print(f"[startup] {label} init failed (continuing): {exc}", flush=True)
    yield
    await queue.close()
    await db.close_pool()


app = FastAPI(title="Webhook Time Machine", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return {"ok": True}




async def create_endpoint(name: str, destination_url: str) -> dict:
    pool = await db.get_pool()
    token = secrets.token_urlsafe(12)
    row = await pool.fetchrow(
        "INSERT INTO endpoints (name, token, destination_url) VALUES ($1,$2,$3) RETURNING id, token",
        name, token, destination_url,
    )
    return {"id": str(row["id"]), "token": row["token"]}


@app.get("/", response_class=HTMLResponse)
async def home():
    dest = f"{settings.internal_base_url}/sample/receive"
    ep = await create_endpoint("demo", dest)
    return RedirectResponse(url=f"/d/{ep['id']}", status_code=303)


@app.post("/in/{token}")
async def ingest(token: str, request: Request):
    pool = await db.get_pool()
    ep = await pool.fetchrow("SELECT id FROM endpoints WHERE token = $1", token)
    if ep is None:
        return JSONResponse({"error": "unknown endpoint"}, status_code=404)
    endpoint_id = str(ep["id"])

    if not await cache.allow_rate(endpoint_id):
        return JSONResponse({"error": "rate limited"}, status_code=429)

    body = await request.body()
    headers = dict(request.headers)
    idem = headers.get("idempotency-key") or headers.get("x-idempotency-key")

    is_dup = False
    if idem:
        first_time = await cache.claim_idempotency(endpoint_id, idem)
        is_dup = not first_time

    await cache.bump(endpoint_id, "received")
    if is_dup:
        await cache.bump(endpoint_id, "duplicate")

    event_id = str(uuid.uuid4())
    body_key = f"{endpoint_id}/{event_id}"
    if body:
        await storage.put_payload(body_key, body, headers.get("content-type", "application/octet-stream"))

    await pool.execute(
        """INSERT INTO events (id, endpoint_id, idempotency_key, headers, body_key, body_size, is_duplicate)
           VALUES ($1,$2,$3,$4,$5,$6,$7)""",
        uuid.UUID(event_id), ep["id"], idem, json.dumps(headers), body_key, len(body), is_dup,
    )

    if is_dup:  # recorded but never delivered
        return JSONResponse({"status": "duplicate", "event_id": event_id}, status_code=200)

    delivery = await pool.fetchrow(
        "INSERT INTO deliveries (event_id, endpoint_id) VALUES ($1,$2) RETURNING id",
        uuid.UUID(event_id), ep["id"],
    )
    await queue.publish_delivery(str(delivery["id"]))
    return JSONResponse({"status": "accepted", "event_id": event_id}, status_code=202)


@app.get("/d/{endpoint_id}", response_class=HTMLResponse)
async def dashboard(endpoint_id: str, request: Request):
    pool = await db.get_pool()
    ep = await pool.fetchrow("SELECT id, name, token, destination_url FROM endpoints WHERE id = $1",
                             uuid.UUID(endpoint_id))
    if ep is None:
        return HTMLResponse("Unknown endpoint", status_code=404)
    base = str(request.base_url).rstrip("/")
    # behind Zerops' TLS proxy the scheme comes through as http; show https publicly
    if base.startswith("http://") and "127.0.0.1" not in base and "localhost" not in base:
        base = "https://" + base[len("http://"):]
    ingest_url = f"{base}/in/{ep['token']}"
    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "endpoint_id": endpoint_id, "name": ep["name"],
         "ingest_url": ingest_url, "destination_url": ep["destination_url"]},
    )


@app.get("/d/{endpoint_id}/stream")
async def stream(endpoint_id: str):
    zero_pct = {"attempts": 0, "p50": 0, "p95": 0, "p99": 0, "success_rate": 0.0}

    async def gen():  # SSE: push counters + latency percentiles ~1x/sec
        while True:
            stats = await cache.get_stats(endpoint_id)
            try:
                pct = await asyncio.to_thread(analytics.latency_percentiles, endpoint_id)
            except Exception:
                pct = zero_pct
            payload = json.dumps({"stats": stats, "latency": pct})
            yield f"data: {payload}\n\n"
            await asyncio.sleep(1)

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/api/{endpoint_id}/deliveries", response_class=HTMLResponse)
async def deliveries_table(endpoint_id: str, request: Request):
    pool = await db.get_pool()
    rows = await pool.fetch(
        """SELECT d.id, d.status, d.attempts, d.last_status_code, d.last_error, d.last_attempt_at,
                  e.idempotency_key
             FROM deliveries d JOIN events e ON e.id = d.event_id
            WHERE d.endpoint_id = $1
            ORDER BY d.created_at DESC LIMIT 50""",
        uuid.UUID(endpoint_id),
    )
    return templates.TemplateResponse(
        "_deliveries.html", {"request": request, "rows": rows}
    )


@app.post("/api/{endpoint_id}/replay-failures")
async def replay_failures(endpoint_id: str):
    pool = await db.get_pool()
    rows = await pool.fetch(
        "SELECT id FROM deliveries WHERE endpoint_id = $1 AND status IN ('failed','dead')",
        uuid.UUID(endpoint_id),
    )
    for r in rows:
        await pool.execute(
            "UPDATE deliveries SET status='pending' WHERE id=$1", r["id"]
        )
        await queue.publish_delivery(str(r["id"]))
        await cache.bump(endpoint_id, "retried")
    return JSONResponse({"requeued": len(rows)})


# flaky demo target: fails ~40% and sometimes stalls, until /sample/heal flips it healthy in Valkey
_sample_counter = {"n": 0}


@app.post("/sample/receive")
async def sample_receive():
    r = cache.get_redis()
    if await r.get("sample:healthy") == "1":
        return Response(status_code=200)
    n = _sample_counter["n"] = _sample_counter["n"] + 1
    if n % 7 == 0:
        await asyncio.sleep(3)
    if n % 5 in (0, 2):
        return Response(status_code=503)
    return Response(status_code=200)


@app.post("/sample/heal")
async def sample_heal():
    r = cache.get_redis()
    await r.set("sample:healthy", "1")
    return {"healthy": True}


@app.post("/sample/break")
async def sample_break():
    r = cache.get_redis()
    await r.delete("sample:healthy")
    return {"healthy": False}
