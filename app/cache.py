import redis.asyncio as redis

from app.config import settings

_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.from_url(settings.redis_url, decode_responses=True)
    return _client


async def claim_idempotency(endpoint_id: str, key: str, ttl_seconds: int = 86400) -> bool:
    # SET NX: returns True only the first time this key is seen, so duplicates collapse
    r = get_redis()
    ok = await r.set(f"idem:{endpoint_id}:{key}", "1", nx=True, ex=ttl_seconds)
    return bool(ok)


async def allow_rate(endpoint_id: str, limit: int = 2000, window_seconds: int = 60) -> bool:
    r = get_redis()
    bucket = f"rate:{endpoint_id}"
    count = await r.incr(bucket)
    if count == 1:
        await r.expire(bucket, window_seconds)
    return count <= limit


async def bump(endpoint_id: str, field: str, by: int = 1) -> None:
    r = get_redis()
    await r.hincrby(f"stats:{endpoint_id}", field, by)


async def get_stats(endpoint_id: str) -> dict[str, int]:
    r = get_redis()
    raw = await r.hgetall(f"stats:{endpoint_id}")
    keys = ("received", "delivered", "failed", "dead", "duplicate", "retried")
    return {k: int(raw.get(k, 0)) for k in keys}
