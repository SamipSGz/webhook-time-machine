"""Fire a burst of webhooks at an ingestion URL to drive the demo.

Usage:
    python scripts/loadgen.py http://localhost:8000/in/<token> --count 1000 --dupes 50
"""
import argparse
import asyncio
import json

import httpx


async def send(client, url, i, dup_key=None):
    headers = {"content-type": "application/json"}
    if dup_key:
        headers["Idempotency-Key"] = dup_key
    body = json.dumps({"event": "order.created", "seq": i, "amount": 100 + i})
    try:
        await client.post(url, content=body, headers=headers)
    except Exception as exc:  # noqa: BLE001
        print("send error:", exc)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("url")
    ap.add_argument("--count", type=int, default=1000)
    ap.add_argument("--dupes", type=int, default=50, help="extra duplicate sends reusing one key")
    ap.add_argument("--concurrency", type=int, default=50)
    args = ap.parse_args()

    sem = asyncio.Semaphore(args.concurrency)

    async with httpx.AsyncClient(timeout=15) as client:
        async def guarded(coro):
            async with sem:
                await coro

        tasks = [guarded(send(client, args.url, i)) for i in range(args.count)]
        # Duplicate storm: many sends sharing a single idempotency key.
        tasks += [guarded(send(client, args.url, -1, dup_key="dup-demo-key")) for _ in range(args.dupes)]
        await asyncio.gather(*tasks)

    print(f"sent {args.count} events + {args.dupes} duplicates")


if __name__ == "__main__":
    asyncio.run(main())
