import aioboto3

from app.config import settings

_session = aioboto3.Session()


def _client():
    return _session.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        region_name=settings.s3_region,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
    )


async def ensure_bucket() -> None:
    async with _client() as s3:
        try:
            await s3.head_bucket(Bucket=settings.s3_bucket)
        except Exception:
            await s3.create_bucket(Bucket=settings.s3_bucket)


async def put_payload(key: str, body: bytes, content_type: str = "application/octet-stream") -> None:
    async with _client() as s3:
        await s3.put_object(Bucket=settings.s3_bucket, Key=key, Body=body, ContentType=content_type)


async def get_payload(key: str) -> bytes:
    async with _client() as s3:
        obj = await s3.get_object(Bucket=settings.s3_bucket, Key=key)
        return await obj["Body"].read()
