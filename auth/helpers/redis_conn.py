from settings.conf import settings
from redis import asyncio as aioredis
import redis


"""
the auth and rate limiting module at the moment uses the same redis connection as the rest of the service
"""


def construct_redis_url():
    if settings.redis.password:
        return f'redis://{settings.redis.password}@{settings.redis.host}:{settings.redis.port}'\
               f'/{settings.redis.db}'
    else:
        return f'redis://{settings.redis.host}:{settings.redis.port}/{settings.redis.db}'


async def get_aioredis_pool(pool_size=200):
    return await aioredis.from_url(
        url=construct_redis_url(),
        retry_on_error=[redis.exceptions.ReadOnlyError],
        max_connections=pool_size,
    )


class RedisPoolCache:
    def __init__(self, pool_size=500):
        self._aioredis_pool = None
        self._pool_size = pool_size

    async def populate(self):
        if not self._aioredis_pool:
            self._aioredis_pool: aioredis.Redis = await get_aioredis_pool(self._pool_size)
