"""ClickHouse: append-only delivery-attempt log for high-volume history and
latency percentiles. Kept separate from Postgres (which holds current state)."""
import threading

import clickhouse_connect

from app.config import settings

# clickhouse-connect clients are NOT safe for concurrent queries within one
# session. record_attempt() runs from a pool of worker threads (via to_thread),
# so each thread gets its own client.
_local = threading.local()


def get_client():
    client = getattr(_local, "client", None)
    if client is None:
        client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_db,
        )
        _local.client = client
    return client


def init_schema() -> None:
    client = get_client()
    client.command(
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
    client = get_client()
    client.insert(
        "delivery_attempts",
        [[endpoint_id, event_id, delivery_id, attempt_no, status_code, latency_ms, int(success), error]],
        column_names=[
            "endpoint_id", "event_id", "delivery_id", "attempt_no",
            "status_code", "latency_ms", "success", "error",
        ],
    )


def latency_percentiles(endpoint_id: str) -> dict:
    client = get_client()
    row = client.query(
        """
        SELECT
            count() AS attempts,
            round(quantile(0.50)(latency_ms)) AS p50,
            round(quantile(0.95)(latency_ms)) AS p95,
            round(quantile(0.99)(latency_ms)) AS p99,
            round(100 * avg(success), 1) AS success_rate
        FROM delivery_attempts
        WHERE endpoint_id = {eid:String}
        """,
        parameters={"eid": endpoint_id},
    ).first_row
    if not row:
        return {"attempts": 0, "p50": 0, "p95": 0, "p99": 0, "success_rate": 0.0}
    return {"attempts": row[0], "p50": row[1], "p95": row[2], "p99": row[3], "success_rate": row[4]}
