# ⏳ Webhook Time Machine

A durable webhook gateway that **captures every event, prevents duplicates, retries
failures with exponential backoff, dead-letters what won't deliver, and lets you
replay any failed delivery from a live timeline.**

Built for **The Zerops Challenge**. The point isn't the app code — it's that a
production webhook pipeline that normally needs a queue, a database, an analytics
store, a cache, object storage, worker autoscaling, and private networking ships as
**one Zerops project from a single import file**.

## Architecture

```
                POST /in/{token}
                       │
                 ┌─────▼─────┐   dedup / rate-limit (Valkey)
                 │    API     │   raw payload archive (S3)
                 │ (FastAPI)  │   event + delivery state (Postgres)
                 └─────┬─────┘
                       │ publish job
                 ┌─────▼─────┐
                 │    NATS     │  JetStream — durable, at-least-once
                 │  JetStream  │  retry = NAK with backoff delay
                 └─────┬─────┘
                       │ pull
                 ┌─────▼─────┐   POST → destination (httpx)
                 │   Worker   │   attempt log → ClickHouse (p50/p95/p99)
                 │  (scales)  │   success / failed / dead → Postgres + Valkey
                 └───────────┘

   Dashboard (HTMX + SSE) ← live counters (Valkey) + percentiles (ClickHouse)
```

**Zerops services used (all meaningful, none decorative):** PostgreSQL · NATS JetStream ·
Valkey · ClickHouse · S3 object storage · Python runtimes · autoscaling · private networking.

## Run locally

```bash
docker compose up -d                 # postgres, nats, clickhouse, valkey, minio
cp .env.example .env
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# terminal 1 — API + dashboard
uvicorn app.main:app --reload

# terminal 2 — delivery worker
python -m app.worker
```

Open http://localhost:8000 → you're redirected to a fresh demo workspace dashboard.
The dashboard shows your **ingest URL** (`/in/{token}`) forwarding to the built-in
flaky sample receiver.

## Deploy to Zerops

```bash
# 1. Provision the whole stack (5 managed services + 2 app services)
zcli project import zerops-project-import.yml

# 2. Push the code (builds api + worker per zerops.yaml)
zcli push

# 3. Enable public subdomain on the `api` service in the GUI (or it's preset).
```

`zerops.yaml` wires every managed-service credential in via `${service_var}`
references — nothing is hand-copied. Verify exact env-var names in the Zerops GUI
(Service → Environment variables) if your service versions differ.

## Layout

| Path | What |
|---|---|
| `app/main.py` | ingestion API + dashboard + SSE + sample receiver |
| `app/worker.py` | delivery worker: retries, backoff, dead-letter |
| `app/queue.py` | NATS JetStream connect/publish |
| `app/db.py` / `app/schema.sql` | Postgres pool + schema |
| `app/cache.py` | Valkey: dedup, rate limit, live counters |
| `app/analytics.py` | ClickHouse: attempt log + percentiles |
| `app/storage.py` | S3 payload archive |
| `scripts/loadgen.py` | burst generator for the demo |
| `zerops-project-import.yml` | one-shot stack provisioning |
| `zerops.yaml` | build/run for api + worker |
