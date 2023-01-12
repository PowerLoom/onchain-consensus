import asyncio
import json
import uuid
from functools import wraps

# TODO: upgrade to typer. click is too ancient
import click
from pydantic import ValidationError

from data_models import SnapshotterMetadata, UserStatusEnum
from helpers.redis_keys import get_snapshotter_info_allowed_snapshotters_key
from helpers.redis_keys import get_snapshotter_info_key
from helpers.redis_keys import get_snapshotter_info_snapshotter_mapping_key
from settings.conf import settings
from utils.redis_conn import RedisPool


# decorator to make it compatible with asyncio
# taken from https://github.com/pallets/click/issues/85#issuecomment-503464628
def coro(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


metadata_sample = SnapshotterMetadata(
    rate_limit=settings.rate_limit,
    active=UserStatusEnum.active,
    email='xcz@0bv.com',
    alias='HappySnapper',
    name='HappySnapper',
    uuid=uuid.UUID(int=0).__str__()
).json(exclude={'callsCount', 'throttledCount', 'next_reset_at', 'uuid'})


@click.command()
@coro
@click.option('--sample-metadata', is_flag=True, help='Show sample metadata in json format')
@click.argument('metadata', type=str, default=None, required=False)
async def add_snapshotter(metadata: str, sample_metadata: bool):
    """
    CLI to add new snapshotter.
    """
    if metadata is None:
        if sample_metadata:
            click.echo("Sample metadata: ")
            click.echo(metadata_sample)
            return
        else:
            click.echo("Please provide metadata in json format")
            click.echo("Sample metadata: ")
            click.echo(metadata_sample)
            return
    else:
        redis_pool = RedisPool(writer_redis_conf=settings.redis)
        await redis_pool.populate()

        # Get the writer Redis pool
        writer_redis_pool = redis_pool.writer_redis_pool
        reader_redis_pool = redis_pool.reader_redis_pool

        # Load metadata
        json_data = json.loads(metadata)
        try:
            metadata = SnapshotterMetadata(**json_data)
        except ValidationError:
            click.echo("Invalid metadata provided")
            click.echo("Sample metadata: ")
            click.echo(metadata_sample)
            return
        if not metadata.uuid:
            # Generate a new UUID
            metadata.uuid = str(uuid.uuid4())
        alias = metadata.alias

        # Check if alias already exists
        if await reader_redis_pool.exists(get_snapshotter_info_key(alias)):
            click.echo(f"Error: The alias {alias} already exists.")
            return
        # Add UUID to metadata and store it in Redis
        await writer_redis_pool.set(get_snapshotter_info_key(alias), json.dumps(metadata.dict()))

        # Add snapshotter's UUID and alias to the set of allowed snapshotters
        await writer_redis_pool.sadd(get_snapshotter_info_allowed_snapshotters_key(), metadata.uuid)
        await writer_redis_pool.hset(get_snapshotter_info_snapshotter_mapping_key(), metadata.uuid, metadata.alias)
        print("Snapshotter added successfully!")
        print("Snapshotter Metadata:")
        print(f"Name: {metadata.name}")
        print(f"Email: {metadata.email}")
        print(f"Alias: {metadata.alias}")
        print(f"UUID: {metadata.uuid}")
        print(f"Redis key: snapshotterInfo:{alias}")
        print(f"UUID-alias Mapping:' {metadata.uuid} - {metadata.alias}")
        print('Allotted rate limit: ', metadata.rate_limit)


if __name__ == '__main__':
    asyncio.run(add_snapshotter())
