import json
import uuid

import typer
from redis import Redis
from pydantic import ValidationError

from data_models import SnapshotterMetadata, UserStatusEnum
from helpers.redis_keys import get_snapshotter_info_allowed_snapshotters_key
from helpers.redis_keys import get_snapshotter_info_key
from helpers.redis_keys import get_snapshotter_info_snapshotter_mapping_key
from settings.conf import settings


metadata_sample = SnapshotterMetadata(
    rate_limit=settings.rate_limit,
    active=UserStatusEnum.active,
    email='xcz@0bv.com',
    alias='HappySnapper',
    name='HappySnapper',
    uuid=uuid.UUID(int=0).__str__()
).json(exclude={'callsCount', 'throttledCount', 'next_reset_at', 'uuid'})

app = typer.Typer()

@app.command()
def add_snapshotter(
    sample_metadata: bool = typer.Option(
        False, "--sample-metadata", help="Show sample metadata in json format",
    ),
    metadata: str = typer.Argument(None, help="Metadata in json format"),
):
    """
    CLI to add new snapshotter.
    """
    if metadata is None:
        if sample_metadata:
            typer.echo("Sample metadata: ")
            typer.echo(metadata_sample)
            return
        else:
            typer.echo("Please provide metadata in json format")
            typer.echo("Sample metadata: ")
            typer.echo(metadata_sample)
            return
    else:
        redis_conn = Redis(**settings.redis.dict())
        # Load metadata
        json_data = json.loads(metadata)
        try:
            user_metadata = SnapshotterMetadata(**json_data)
        except ValidationError:
            typer.echo("Invalid metadata provided")
            typer.echo("Sample metadata: ")
            typer.echo(metadata_sample)
            return
        if not user_metadata.uuid:
            # Generate a new UUID
            user_metadata.uuid = str(uuid.uuid4())
        alias = user_metadata.alias

        # Check if uuid already exists 
        if redis_conn.hexists(get_snapshotter_info_snapshotter_mapping_key(), user_metadata.uuid):
            typer.echo(f"Error: The uuid {user_metadata.uuid} already exists.")
            return

        # Check if alias already exists
        if redis_conn.exists(get_snapshotter_info_key(alias)):
            typer.echo(f"Error: The alias {alias} already exists.")
            return
        # Add UUID to metadata and store it in Redis
        redis_conn.set(
            get_snapshotter_info_key(alias),
            json.dumps(user_metadata.dict())
        )

        # Add snapshotter's UUID to the set of allowed snapshotters
        redis_conn.sadd(
            get_snapshotter_info_allowed_snapshotters_key(),
            user_metadata.uuid
        )
        redis_conn.hset(
            get_snapshotter_info_snapshotter_mapping_key(),
            user_metadata.uuid, user_metadata.alias
        )
        typer.echo("Snapshotter added successfully!")
        typer.echo("Snapshotter Metadata:")
        typer.echo(f"Name: {user_metadata.name}")
        typer.echo(f"Email: {user_metadata.email}")
        typer.echo(f"Alias: {user_metadata.alias}")
        typer.echo(f"UUID: {user_metadata.uuid}")
        typer.echo(f"Redis key: {get_snapshotter_info_key(alias)}")

@app.command()
def disable_snapshotter(alias: str):
    """
    CLI to remove snapshotter.
    """
    redis_conn = Redis(**settings.redis.dict())
    # Check if alias exists
    if not redis_conn.exists(get_snapshotter_info_key(alias)):
        typer.echo(f"Error: The alias {alias} does not exist.")
        return
    metadata = json.loads(redis_conn.get(get_snapshotter_info_key(alias)))
    
    user_metadata = SnapshotterMetadata(**metadata)
    user_metadata.active = UserStatusEnum.inactive
    redis_conn.set(get_snapshotter_info_key(alias), json.dumps(user_metadata.dict()))

    # remove snapshotter's UUID from the set of allowed snapshotters
    redis_conn.srem(get_snapshotter_info_allowed_snapshotters_key(), user_metadata.uuid)

    pattern = "projectID:*:centralizedConsensus:peers"
    cursor = "0"
    while cursor != 0:
        cursor, keys = redis_conn.scan(cursor=cursor, match=pattern, count=1000)
        for key in keys:
            redis_conn.srem(
                key,
                user_metadata.uuid
            )
            typer.echo(f"Removed {user_metadata.uuid} from {key}")


    typer.echo(f"Snapshotter with alias {alias} disabled successfully!")


@app.command()
def enable_snapshotter(alias: str):
    """
    CLI to enable snapshotter.
    """
    redis_conn = Redis(**settings.redis.dict())
    # Check if alias exists
    if not redis_conn.exists(get_snapshotter_info_key(alias)):
        typer.echo(f"Error: The alias {alias} does not exist.")
        return
    metadata = json.loads(redis_conn.get(get_snapshotter_info_key(alias)))
    
    user_metadata = SnapshotterMetadata(**metadata)
    user_metadata.active = UserStatusEnum.active
    redis_conn.set(get_snapshotter_info_key(alias), json.dumps(user_metadata.dict()))

    # Add snapshotter's UUID to the set of allowed snapshotters
    redis_conn.sadd(
        get_snapshotter_info_allowed_snapshotters_key(),
        user_metadata.uuid
    )


    typer.echo(f"Snapshotter with alias {alias} enabled successfully!")

if __name__ == "__main__":
    app()
