"""Postgres access via a single asyncpg pool. Holds the source-of-truth state:
endpoints, events, and delivery lifecycle."""
from pathlib import Path

import asyncpg

from app.config import settings

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(settings.postgres_dsn, min_size=1, max_size=10)
    return _pool


async def init_schema() -> None:
    sql = (Path(__file__).parent / "schema.sql").read_text()
    pool = await get_pool()
    async with pool.acquire() as conn:
        # `CREATE TABLE IF NOT EXISTS` is not race-safe under concurrency (api and
        # worker init at the same time). A transaction-scoped advisory lock
        # serializes concurrent initializers so exactly one wins the race.
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(987654321)")
            await conn.execute(sql)


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
