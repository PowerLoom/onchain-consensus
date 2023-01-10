import asyncio
import logging
import sys
from settings.conf import settings
from redis import asyncio as aioredis
from helpers.redis_keys import get_epoch_generator_last_epoch, get_epoch_generator_epoch_history
from epoch_generator import EpochGenerator
from utils.redis_conn import RedisPool

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

if not settings.test_redis:
    logger.error("test_redis not defined in settings")
    sys.exit(1)


async def cleanup_redis(redis_conn: aioredis.Redis):
    # Delete the keys used by the code
    await redis_conn.delete(get_epoch_generator_last_epoch())
    logger.info("Deleted key: %s", get_epoch_generator_last_epoch())
    await redis_conn.delete(get_epoch_generator_epoch_history())
    logger.info("Deleted key: %s", get_epoch_generator_epoch_history())


async def test_epoch_generation():
    # Create an instance of the RedisPool class and connect to the Redis instance
    redis_pool = RedisPool(writer_redis_conf=settings.test_redis)
    await redis_pool.populate()
    writer_redis_pool = redis_pool.writer_redis_pool
    reader_redis_pool = redis_pool.reader_redis_pool
    
    await cleanup_redis(writer_redis_pool)
    ticker_process = EpochGenerator(name="PowerLoom|EpochGenerator|LinearTest2", simulation_mode=True)
    # kwargs = dict()
    # await ticker_process.setup(**kwargs)
    await ticker_process.run(begin_block_epoch=16216611)

    # Get the current epoch end block height from Redis
    epoch_end_block_height = await reader_redis_pool.get(get_epoch_generator_last_epoch())
    if not epoch_end_block_height:
        assert False, "Epoch end block height not found in Redis"
    epoch_end_block_height = int(epoch_end_block_height.decode("utf-8"))
    logger.info("Current epoch end block height: %s", epoch_end_block_height)
    expected_simulation_epochs_num = 10
    assert epoch_end_block_height == 16216611 + settings.chain.epoch.height*expected_simulation_epochs_num - 1, \
        "Epoch end block height is not correct"


async def test_force_restart_with_redis_state_present():
    # Create an instance of the RedisPool class and connect to the Redis instance
    redis_pool = RedisPool(writer_redis_conf=settings.test_redis)
    await redis_pool.populate()
    writer_redis_pool = redis_pool.writer_redis_pool
    reader_redis_pool = redis_pool.reader_redis_pool
    
    ticker_process = EpochGenerator(name="PowerLoom|EpochGenerator|LinearTest3", simulation_mode=True)
    kwargs = dict()
    # Should not do anything
    await ticker_process.run(begin_block_epoch=16216611)

    # Get the current epoch end block height from Redis
    epoch_end_block_height = await reader_redis_pool.get(get_epoch_generator_last_epoch())
    if not epoch_end_block_height:
        assert False, "Epoch end block height not found in Redis"
    epoch_end_block_height = int(epoch_end_block_height.decode("utf-8"))
    logger.info("Current epoch end block height: %s", epoch_end_block_height)
    expected_simulation_epochs_num = 10
    assert epoch_end_block_height == 16216611 + settings.chain.epoch.height * expected_simulation_epochs_num - 1,\
        "Epoch end block height is not correct"
    # Cleanup redis
    await cleanup_redis(writer_redis_pool)


async def test_routines():
    await test_epoch_generation()
    await test_force_restart_with_redis_state_present()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # NOTE: run_until_complete should be used only once
    # since it closes the event loop after the Future execution is complete
    loop.run_until_complete(test_epoch_generation())
    loop.run_until_complete(test_force_restart_with_redis_state_present())
