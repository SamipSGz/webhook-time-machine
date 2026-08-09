import json

import nats
from nats.js.api import StreamConfig

from app.config import settings

_nc = None
_js = None


async def connect():
    global _nc, _js
    if _js is not None:
        return _js
    _nc = await nats.connect(settings.nats_url)
    _js = _nc.jetstream()
    try:
        await _js.add_stream(
            StreamConfig(name=settings.delivery_stream, subjects=[settings.delivery_subject])
        )
    except Exception:
        pass  # stream already exists
    return _js


async def publish_delivery(delivery_id: str) -> None:
    js = await connect()
    await js.publish(settings.delivery_subject, json.dumps({"delivery_id": delivery_id}).encode())


async def close() -> None:
    global _nc, _js
    if _nc is not None:
        await _nc.drain()
        _nc = None
        _js = None
