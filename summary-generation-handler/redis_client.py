import os
from functools import lru_cache

import redis


@lru_cache(maxsize=1)
def get_redis_client() -> redis.Redis:
    """
    Returns a cached Redis client configured from environment variables.
    Defaults are suitable for docker-compose (host 'redis', db 0).
    """
    host = os.getenv("REDIS_HOST", "redis")
    port = int(os.getenv("REDIS_PORT", "6379"))
    db = int(os.getenv("REDIS_DB", "0"))
    return redis.Redis(host=host, port=port, db=db, decode_responses=False)

