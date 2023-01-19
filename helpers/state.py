import asyncio

from data_models import (
    SubmissionDataStoreEntry, SnapshotSubmission, SubmissionSchedule, SubmissionAcceptanceStatus
)
from settings.conf import settings
from helpers.redis_keys import *
from typing import Tuple, Union, Optional
from redis import asyncio as aioredis
import time


async def get_submission_schedule(
        project_id,
        epoch_end,
        redis_conn: aioredis.Redis
):
    schedule = await redis_conn.get(
        get_epoch_submission_schedule_key(
            project_id=project_id,
            epoch_end=epoch_end
        )
    )
    if not schedule:
        return None
    else:
        return SubmissionSchedule.parse_raw(schedule)


async def prune_finalized_cids_htable(
        project_id: str,
        redis_conn: aioredis.Redis
):
    all_finalized = await redis_conn.hgetall(
        name=get_project_finalized_epoch_cids_htable(project_id)
    )
    to_be_del = list()
    for epoch_b, cid_b in all_finalized.items():
        epoch = int(epoch_b)
        schedule = await get_submission_schedule(project_id, epoch, redis_conn)
        if time.time() - schedule.end >= 86400:
            to_be_del.append(epoch)
    await redis_conn.hdel(get_project_finalized_epoch_cids_htable(project_id), *to_be_del)


async def set_submission_schedule(
        project_id,
        epoch_end,
        redis_conn: aioredis.Redis
):
    cur_ts = int(time.time())
    await redis_conn.set(
        name=get_epoch_submission_schedule_key(
            project_id=project_id,
            epoch_end=epoch_end
        ),
        value=SubmissionSchedule(begin=cur_ts, end=cur_ts+settings.consensus_service.submission_window).json(),
        ex=settings.consensus_service.keys_ttl
    )
    # loop.call_later(delay, callback, *args, context=None)¶
    asyncio.get_running_loop().call_later(
        settings.consensus_service.submission_window,
        check_consensus, project_id, epoch_end, redis_conn
    )


async def set_submission_accepted_peers(
        project_id,
        epoch_end,
        redis_conn: aioredis.Redis
):
    await redis_conn.copy(
        get_project_registered_peers_set_key(project_id),
        get_project_epoch_specific_accepted_peers_key(project_id, epoch_end)
    )
    await redis_conn.expire(
        get_project_epoch_specific_accepted_peers_key(project_id, epoch_end),
        settings.consensus_service.keys_ttl
    )


async def submission_delayed(project_id, epoch_end, auto_init_schedule, redis_conn: aioredis.Redis):
    schedule = await get_submission_schedule(project_id, epoch_end, redis_conn)
    if not schedule:
        if auto_init_schedule:
            await set_submission_accepted_peers(project_id, epoch_end, redis_conn)
            await set_submission_schedule(project_id, epoch_end, redis_conn)
        return False
    else:
        return int(time.time()) > schedule.end


async def check_consensus(
        project_id: str,
        epoch_end: int,
        redis_conn: aioredis.Redis
) -> Tuple[SubmissionAcceptanceStatus, Union[str, None]]:
    _ = await redis_conn.hget(get_project_finalized_epoch_cids_htable(project_id), epoch_end)
    if _:
        return SubmissionAcceptanceStatus.finalized, _.decode('utf-8')
    all_submissions = await redis_conn.hgetall(
        name=get_epoch_submissions_htable_key(
            project_id=project_id,
            epoch_end=epoch_end,
        )
    )
    cid_submission_map = dict()
    for instance_id_b, submission_b in all_submissions.items():
        sub_entry: SubmissionDataStoreEntry = SubmissionDataStoreEntry.parse_raw(submission_b)
        instance_id = instance_id_b.decode('utf-8')
        if sub_entry.snapshotCID not in cid_submission_map:
            cid_submission_map[sub_entry.snapshotCID] = [instance_id]
        else:
            cid_submission_map[sub_entry.snapshotCID].append(instance_id)
    epoch_schedule = await get_submission_schedule(project_id, epoch_end, redis_conn)
    num_submitted_peers = len(all_submissions)

    sub_count_map = {k: len(cid_submission_map[k]) for k in cid_submission_map.keys()}
    if int(time.time()) >= epoch_schedule.end:
        divisor = num_submitted_peers
    else:
        # when deadline is not over, consider conservative calculation against all expected peers
        divisor = await redis_conn.scard(get_project_epoch_specific_accepted_peers_key(project_id, epoch_end))
    for cid, sub_count in sub_count_map.items():
        # find one CID on which consensus has been reached
        if sub_count/divisor * 100 >= settings.consensus_criteria.percentage or \
                (sub_count == settings.consensus_criteria.min_snapshotter_count and int(time.time()) >= epoch_schedule.end):
            await redis_conn.hset(
                name=get_project_finalized_epoch_cids_htable(project_id),
                mapping={epoch_end: cid}
            )
            return SubmissionAcceptanceStatus.finalized, cid
    else:
        # find if deadline passed and yet no consensus reached
        return SubmissionAcceptanceStatus.indeterminate if int(time.time()) >= epoch_schedule.end else SubmissionAcceptanceStatus.accepted, None


async def register_submission(
        submission: SnapshotSubmission,
        cur_ts: int,
        redis_conn: aioredis.Redis
) -> Tuple[SubmissionAcceptanceStatus, Union[str, None]]:
    await redis_conn.hset(
        name=get_epoch_submissions_htable_key(
            project_id=submission.projectID,
            epoch_end=submission.epoch.end,
        ),
        key=submission.instanceID,
        value=SubmissionDataStoreEntry(snapshotCID=submission.snapshotCID, submittedTS=cur_ts).json()
    )
    if await redis_conn.ttl(name=get_epoch_submissions_htable_key(
            project_id=submission.projectID,
            epoch_end=submission.epoch.end,
    )) == -1:
        await redis_conn.expire(
            name=get_epoch_submissions_htable_key(
                project_id=submission.projectID,
                epoch_end=submission.epoch.end,
            ),
            time=settings.consensus_service.keys_ttl
        )
    return await check_consensus(submission.projectID, submission.epoch.end, redis_conn)
