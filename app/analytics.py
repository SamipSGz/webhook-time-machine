import json

import httpx

from app.config import settings

_COLUMNS = [
    "endpoint_id", "event_id", "delivery_id", "attempt_no",
    "status_code", "latency_ms", "success", "error",
]


def _post(sql: str, *, body: bytes | None = None, params: dict | None = None, use_db: bool = True) -> str:
    url = f"http://{settings.clickhouse_host}:{settings.clickhouse_port}/"
    p = {"database": settings.clickhouse_db} if use_db else {}
    if params:
        p.update(params)
    headers = {
        "X-ClickHouse-User": settings.clickhouse_user,
        "X-ClickHouse-Key": settings.clickhouse_password,
    }
    with httpx.Client(timeout=10) as c:
        if body is not None:
            p["query"] = sql
            r = c.post(url, params=p, content=body, headers=headers)
        else:
            r = c.post(url, params=p, content=sql, headers=headers)
    r.raise_for_status()
    return r.text


def init_schema() -> None:
    _post(
        """
        CREATE TABLE IF NOT EXISTS delivery_attempts (
            ts           DateTime64(3) DEFAULT now64(3),
            endpoint_id  String,
            event_id     String,
            delivery_id  String,
            attempt_no   UInt16,
            status_code  Int32,
            latency_ms   UInt32,
            success      UInt8,
            error        String
        ) ENGINE = MergeTree()
        ORDER BY (endpoint_id, ts)
        """
    )


def record_attempt(
    *,
    endpoint_id: str,
    event_id: str,
    delivery_id: str,
    attempt_no: int,
    status_code: int,
    latency_ms: int,
    success: bool,
    error: str = "",
) -> None:
    row = {
        "endpoint_id": endpoint_id, "event_id": event_id, "delivery_id": delivery_id,
        "attempt_no": attempt_no, "status_code": status_code, "latency_ms": latency_ms,
        "success": int(success), "error": error,
    }
    _post(
        f"INSERT INTO delivery_attempts ({','.join(_COLUMNS)}) FORMAT JSONEachRow",
        body=json.dumps(row).encode(),
    )


def latency_percentiles(endpoint_id: str) -> dict:
    text = _post(
        """
        SELECT count(),
               round(quantile(0.50)(latency_ms)),
               round(quantile(0.95)(latency_ms)),
               round(quantile(0.99)(latency_ms)),
               round(100 * avg(success), 1)
        FROM delivery_attempts
        WHERE endpoint_id = {eid:String}
        FORMAT TabSeparated
        """,
        params={"param_eid": endpoint_id},
    ).strip()

    zeros = {"attempts": 0, "p50": 0, "p95": 0, "p99": 0, "success_rate": 0.0}
    if not text:
        return zeros
    parts = text.split("\t")
    attempts = int(float(parts[0]))
    if attempts == 0:
        return zeros
    return {
        "attempts": attempts,
        "p50": int(float(parts[1])),
        "p95": int(float(parts[2])),
        "p99": int(float(parts[3])),
        "success_rate": float(parts[4]),
    }
