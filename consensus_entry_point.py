from data_models import (
    SnapshotSubmission, SubmissionResponse, PeerRegistrationRequest, SubmissionAcceptanceStatus, SnapshotBase,
    EpochConsensusStatus, ProjectSpecificSnapshotters, Epoch, EpochData, EpochDataPage, Submission, SubmissionStatus,
    Message, EpochInfo,
    EpochStatus, EpochDetails, SnapshotterIssue, SnapshotterAliasIssue
)
from typing import List, Optional, Any, Dict, Union, Tuple
from fastapi.responses import JSONResponse
from settings.conf import settings
from helpers.state import submission_delayed, register_submission, check_consensus, prune_finalized_cids_htable
from helpers.redis_keys import *
from auth.utils.helpers import rate_limit_auth_check, inject_rate_limit_fail_response
from auth.utils.data_models import RateLimitAuthCheck, UserStatusEnum, SnapshotterMetadata
from utils.rate_limiter import load_rate_limiter_scripts
from utils.lru_cache import LRUCache
from utils.rpc import RpcHelper
from utils.transaction_utils import write_transaction_with_retry
from utils.transaction_utils import generate_account_from_uuid
from helpers.state import get_project_finalized_epoch_cids
from helpers.state import get_submission_schedule
from fastapi import FastAPI, Request, Response, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from functools import wraps
from utils.redis_conn import RedisPool
from redis import asyncio as aioredis
import sys
import json
import redis
import time
import uuid
import asyncio
from utils.default_logger import logger
from web3 import Web3

service_logger = logger.bind(service='PowerLoom|OffChainConsensus|ServiceEntry')


def acquire_bounded_semaphore(fn):
    @wraps(fn)
    async def wrapped(*args, **kwargs):
        sem: asyncio.BoundedSemaphore = kwargs['semaphore']
        await sem.acquire()
        result = None
        try:
            result = await fn(*args, **kwargs)
        except Exception as e:
            service_logger.opt(exception=True).error(f'Error in {fn.__name__}: {e}')
            pass
        finally:
            sem.release()
            return result

    return wrapped


# setup CORS origins stuff
origins = ["*"]

redis_lock = redis.Redis()

app = FastAPI()
app.logger = service_logger

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


@app.middleware('http')
async def request_middleware(request: Request, call_next: Any) -> Optional[Dict]:
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id

    with service_logger.contextualize(request_id=request_id):
        service_logger.info('Request started for: {}', request.url)
        try:
            response = await call_next(request)

        except Exception as ex:
            service_logger.opt(exception=True).error(f'Request failed: {ex}')

            response = JSONResponse(
                content={
                    'info':
                        {
                            'success': False,
                            'response': 'Internal Server Error',
                        },
                    'request_id': request_id,
                }, status_code=500,
            )

        finally:
            response.headers['X-Request-ID'] = request_id
            service_logger.info('Request ended')
            return response


async def periodic_finalized_cids_htable_cleanup():
    project_id_pattern = "projectID:*:centralizedConsensus:peers"
    while True:
        pruning_tasks = list()
        async for project_id in app.state.reader_redis_pool.scan_iter(match=project_id_pattern):
            project_id = project_id.decode("utf-8").split(":")[1]
            pruning_tasks.append(prune_finalized_cids_htable(project_id, app.state.reader_redis_pool))
        pruning_tasks.append(asyncio.sleep(3600))
        await asyncio.gather(
            *pruning_tasks, return_exceptions=True
        )


@app.on_event('startup')
async def startup_boilerplate():
    app.state.aioredis_pool = RedisPool(writer_redis_conf=settings.redis)
    await app.state.aioredis_pool.populate()
    app.state.reader_redis_pool = app.state.aioredis_pool.reader_redis_pool
    app.state.writer_redis_pool = app.state.aioredis_pool.writer_redis_pool
    app.state.rate_limit_lua_script_shas = await load_rate_limiter_scripts(app.state.writer_redis_pool)
    asyncio.ensure_future(periodic_finalized_cids_htable_cleanup())
    app.state.auth = dict()
    app.state.snapshotter_aliases = dict()
    # Better to use a LRC Cache sort of thing here for auto cleanup
    app.state.submission_schedule = LRUCache(1000)
    app.state.finalized_epoch_cids = LRUCache(1000)
    app.state.rpc_helper = RpcHelper()
    app.state.protocol_state_contract_address = settings.anchor_chain_rpc.protocol_state_address

    # load abi from json file and create contract object
    with open("utils/static/abi.json", "r") as f:
        abi = json.load(f)
    app.state.w3 = Web3(Web3.HTTPProvider(settings.anchor_chain_rpc.full_nodes[0].url))
    app.state.protocol_state_contract = app.state.w3.eth.contract(address=settings.anchor_chain_rpc.protocol_state_address, abi=abi)



@app.post('/registerProjectPeer')
async def register_peer_against_project(
        req_parsed: PeerRegistrationRequest,
        request: Request,
        response: Response,
        rate_limit_auth_dep: RateLimitAuthCheck = Depends(rate_limit_auth_check)
):
    if not (
            rate_limit_auth_dep.rate_limit_passed and
            rate_limit_auth_dep.authorized and
            rate_limit_auth_dep.owner.active == UserStatusEnum.active
    ):
        return inject_rate_limit_fail_response(rate_limit_auth_dep)
    
    account, apkey = generate_account_from_uuid(req_parsed.instanceID)

    # estimate gas then send 
    try:
        est_gas = app.state.protocol_state_contract.functions.registerPeer(
            account,
            req_parsed.projectID
        ).estimateGas({'from': settings.anchor_chain_rpc.owner_address})
        tx_hash = write_transaction_with_retry(
            settings.anchor_chain_rpc.owner_address,
            settings.anchor_chain_rpc.owner_private_key,
            request.app.state.protocol_state_contract,
            'registerPeer',
            account,
            req_parsed.projectID
            )
        service_logger.info("Peer Registration transaction sent! Transaction hash: {}", tx_hash)

    except Exception as E:
        if "Snapshotter already exists" not in str(E):
            return {
                'info': {
                    'success': False,
                    'response': f'Transaction will most likely fail with error {E}',
                }
            }
    
    
    await request.app.state.writer_redis_pool.sadd(
        get_project_registered_peers_set_key(req_parsed.projectID),
        req_parsed.instanceID
    )


@app.post('/submitSnapshot')
async def submit_snapshot(
        request: Request,
        req_parsed: SnapshotSubmission,
        response: Response,
        rate_limit_auth_dep: RateLimitAuthCheck = Depends(rate_limit_auth_check)
):
    if not (
            rate_limit_auth_dep.rate_limit_passed and
            rate_limit_auth_dep.authorized and
            rate_limit_auth_dep.owner.active == UserStatusEnum.active
    ):
        return inject_rate_limit_fail_response(rate_limit_auth_dep)
    
    account, apkey = generate_account_from_uuid(req_parsed.instanceID)
    # estimate gas then send 
    try:
        est_gas = app.state.protocol_state_contract.functions.commitRecord(
            req_parsed.snapshotCID,
            req_parsed.epoch.end,
            req_parsed.projectID,
            account
            ).estimateGas({'from': settings.anchor_chain_rpc.owner_address})
    except Exception as E:
        return {
            'info': {
                'success': False,
                'response': f'Transaction will most likely fail with error {E}',
            }
        }
    
    try:
        tx_hash = write_transaction_with_retry(
        settings.anchor_chain_rpc.owner_address,
        settings.anchor_chain_rpc.owner_private_key,
        request.app.state.protocol_state_contract,
        'commitRecord',
        req_parsed.snapshotCID,
        req_parsed.epoch.end,
        req_parsed.projectID,
        account
        )
    except Exception as E:
        logger.error("Snapshot submission transaction failed with error: {}", E)

    cur_ts = int(time.time())
    service_logger.debug('Snapshot for submission: {}', req_parsed)
    # get last accepted epoch?
    if await submission_delayed(
            project_id=req_parsed.projectID,
            epoch_end=req_parsed.epoch.end,
            auto_init_schedule=True,
            redis_conn=request.app.state.writer_redis_pool,
            request = request
    ):
        response_obj = SubmissionResponse(status=SubmissionAcceptanceStatus.accepted, delayedSubmission=True)
    else:
        response_obj = SubmissionResponse(status=SubmissionAcceptanceStatus.accepted, delayedSubmission=False)
    consensus_status, finalized_cid = await register_submission(req_parsed, cur_ts, request.app.state.writer_redis_pool, request=request)

    response_obj.status = consensus_status
    response_obj.finalizedSnapshotCID = finalized_cid
    response.body = response_obj
    return response_obj.dict()


@app.post('/checkForSnapshotConfirmation')
async def check_submission_status(
        request: Request,
        req_parsed: SnapshotSubmission,
        response: Response,
        rate_limit_auth_dep: RateLimitAuthCheck = Depends(rate_limit_auth_check)
):
    if not (
            rate_limit_auth_dep.rate_limit_passed and
            rate_limit_auth_dep.authorized and
            rate_limit_auth_dep.owner.active == UserStatusEnum.active
    ):
        return inject_rate_limit_fail_response(rate_limit_auth_dep)
    status, finalized_cid = await check_consensus(
        project_id=req_parsed.projectID, redis_conn=request.app.state.writer_redis_pool, epoch_end=req_parsed.epoch.end, request=request
    )
    if status == SubmissionAcceptanceStatus.notsubmitted:
        response.status_code = 400
        return SubmissionResponse(status=status, delayedSubmission=False, finalizedSnapshotCID=None).dict()
    else:
        return SubmissionResponse(
            status=status,
            delayedSubmission=await submission_delayed(
                req_parsed.projectID,
                epoch_end=req_parsed.epoch.end,
                auto_init_schedule=False,
                redis_conn=request.app.state.writer_redis_pool,
                request = request
            ),
            finalizedSnapshotCID=finalized_cid
        ).dict()


@app.post('/reportIssue')
async def report_issue(
        request: Request,
        req_parsed: SnapshotterIssue,
        response: Response,
        rate_limit_auth_dep: RateLimitAuthCheck = Depends(rate_limit_auth_check)
):
    if not (
            rate_limit_auth_dep.rate_limit_passed and
            rate_limit_auth_dep.authorized and
            rate_limit_auth_dep.owner.active == UserStatusEnum.active
    ):
        return inject_rate_limit_fail_response(rate_limit_auth_dep)

    # Updating time of reporting to avoid manual incorrect time manipulation
    req_parsed.timeOfReporting = int(time.time())
    await request.app.state.writer_redis_pool.zadd(
        name=get_snapshotter_issues_reported_key(snapshotter_id=req_parsed.instanceID),
        mapping={json.dumps(req_parsed.dict()): req_parsed.timeOfReporting})

    # pruning expired items
    await request.app.state.writer_redis_pool.zremrangebyscore(
        get_snapshotter_issues_reported_key(snapshotter_id=req_parsed.instanceID), 0,
        int(time.time()) - (7 * 24 * 60 * 60)
    )

    return JSONResponse(status_code=200, content={"message": f"Reported Issue."})


@app.get("/epochDetails", response_model=EpochDetails, responses={404: {"model": Message}})
async def epoch_details(
        request: Request,
        epoch: int = Query(default=0, gte=0),
        rate_limit_auth_dep: RateLimitAuthCheck = Depends(rate_limit_auth_check)
):
    if not (
            rate_limit_auth_dep.rate_limit_passed and
            rate_limit_auth_dep.authorized and
            rate_limit_auth_dep.owner.active == UserStatusEnum.active
    ):
        return inject_rate_limit_fail_response(rate_limit_auth_dep)
    if epoch == 0:
        epoch = int(await app.state.reader_redis_pool.get(get_epoch_generator_last_epoch()))

    epoch_release_time = await app.state.reader_redis_pool.zscore(
        get_epoch_generator_epoch_history(),
        json.dumps({"begin": epoch - settings.chain.epoch.height + 1, "end": epoch})
    )

    if not epoch_release_time:
        return JSONResponse(status_code=404, content={"message": f"No epoch found with Epoch End Time {epoch}"})

    epoch_release_time = int(epoch_release_time)

    project_keys = []
    finalized_projects_count = 0
    projectID_pattern = "projectID:*:centralizedConsensus:peers"
    async for project_id in request.app.state.reader_redis_pool.scan_iter(match=projectID_pattern):
        project_id = project_id.decode("utf-8").split(":")[1]
        project_keys.append(project_id)
        if await get_project_finalized_epoch_cids(project_id, epoch, request.app.state.reader_redis_pool, request):
            finalized_projects_count += 1

    total_projects = len(project_keys)

    if finalized_projects_count == total_projects:
        epoch_status_result = EpochStatus.finalized
    else:
        epoch_status_result = EpochStatus.in_progress

    return EpochDetails(
        epochEndHeight=epoch,
        releaseTime=epoch_release_time,
        status=epoch_status_result,
        totalProjects=total_projects,
        projectsFinalized=finalized_projects_count
    )


@app.post('/epochStatus')
async def epoch_status(
        request: Request,
        req_parsed: SnapshotBase,
        response: Response,
        rate_limit_auth_dep: RateLimitAuthCheck = Depends(rate_limit_auth_check)
):
    if not (
            rate_limit_auth_dep.rate_limit_passed and
            rate_limit_auth_dep.authorized and
            rate_limit_auth_dep.owner.active == UserStatusEnum.active
    ):
        return inject_rate_limit_fail_response(rate_limit_auth_dep)
    status, finalized_cid = await check_consensus(
        req_parsed.projectID, req_parsed.epoch.end, request.app.state.reader_redis_pool, request
    )
    if status != SubmissionAcceptanceStatus.finalized:
        status = EpochConsensusStatus.no_consensus
    else:
        status = EpochConsensusStatus.consensus_achieved
    return SubmissionResponse(status=status, delayedSubmission=False, finalizedSnapshotCID=finalized_cid).dict()


# List of projects tracked/registered '/metrics/projects' .
# Response will be the list of projectIDs that are being tracked for consensus.
@app.get("/metrics/projects", responses={404: {"model": Message}})
async def get_projects(
        request: Request,
        response: Response,
        rate_limit_auth_dep: RateLimitAuthCheck = Depends(rate_limit_auth_check)
):
    if not (
            rate_limit_auth_dep.rate_limit_passed and
            rate_limit_auth_dep.authorized and
            rate_limit_auth_dep.owner.active == UserStatusEnum.active
    ):
        return inject_rate_limit_fail_response(rate_limit_auth_dep)
    """
    Returns a list of project IDs that are being tracked for consensus.
    """
    projects = []

    projectID_pattern = "projectID:*:centralizedConsensus:peers"
    async for project_id in request.app.state.reader_redis_pool.scan_iter(match=projectID_pattern, count=100):
        projects.append(project_id.decode("utf-8").split(":")[1])

    return projects


# List of snapshotters registered for a project '/metrics/{projectid}/snapshotters'.
# Response will be the list of instance-IDs of the snapshotters that are participanting in consensus for this project.
@app.get("/metrics/{project_id}/snapshotters", response_model=ProjectSpecificSnapshotters,
         responses={404: {"model": Message}})
async def get_snapshotters(
        project_id: str,
        request: Request,
        response: Response,
        rate_limit_auth_dep: RateLimitAuthCheck = Depends(rate_limit_auth_check)
):
    if not (
            rate_limit_auth_dep.rate_limit_passed and
            rate_limit_auth_dep.authorized and
            rate_limit_auth_dep.owner.active == UserStatusEnum.active
    ):
        return inject_rate_limit_fail_response(rate_limit_auth_dep)
    """
    Returns a list of instance-IDs of snapshotters that are participating in consensus for the given project.
    """
    snapshotters = await request.app.state.reader_redis_pool.smembers(
        get_project_registered_peers_set_key(project_id)
    )
    # NOTE: Ideal way is to check if project exists first and then get the snapshotters.
    # But right now fetching project list is expensive. So we are doing it this way.
    if not snapshotters:
        return JSONResponse(status_code=404, content={
            "message": f"Either the project is not registered or there are no snapshotters for project {project_id}"})
    redis_conn: aioredis.Redis = request.app.state.writer_redis_pool
    snapshotter_aliases = await redis_conn.hmget(
        get_snapshotter_info_snapshotter_mapping_key(),
        *snapshotters
    )
    return ProjectSpecificSnapshotters(
        projectId=project_id,
        snapshotters=[k.decode('utf-8') for k in snapshotter_aliases]
    )


@acquire_bounded_semaphore
async def bound_check_epoch_finalization(
        project_id: str,
        epoch_end: int,
        # FIXED: redis_pool is of type aioredis.Redis, not RedisPool
        redis_pool: aioredis.Redis,
        semaphore: asyncio.BoundedSemaphore,
        request: Request
) -> Tuple[SubmissionAcceptanceStatus, Union[str, None]]:
    """Check consensus in a bounded way. Will run N paralell threads at once max."""
    consensus_status = await check_consensus(project_id, epoch_end, redis_pool, request)
    return consensus_status


# List of epochs submitted per project '/metrics/{project_id}/epochs' .
# Response will be the list of epochs whose state is currently available in consensus service.
@app.get("/metrics/{project_id}/epochs", response_model=EpochDataPage, responses={404: {"model": Message}})
async def get_epochs(
        project_id: str,
        request: Request,
        response: Response,
        page: int = Query(default=1, gte=0), limit: int = Query(default=100, lte=100),
        rate_limit_auth_dep: RateLimitAuthCheck = Depends(rate_limit_auth_check)
):
    if not (
            rate_limit_auth_dep.rate_limit_passed and
            rate_limit_auth_dep.authorized and
            rate_limit_auth_dep.owner.active == UserStatusEnum.active
    ):
        return inject_rate_limit_fail_response(rate_limit_auth_dep)
    """
    Returns a list of epochs whose state is currently available in the consensus service for the given project.
    """
    epoch_keys = []
    epoch_pattern = f"projectID:{project_id}:[0-9]*:centralizedConsensus:epochSubmissions"
    async for epoch_key in request.app.state.reader_redis_pool.scan_iter(match=epoch_pattern, count=500):
        epoch_keys.append(epoch_key)

    if not epoch_keys:
        return JSONResponse(
            status_code=404, content={
                "message": f"No epochs found for project {project_id}. "
                           f"Either project is not valid or was just added."
            }
        )

    epoch_ends = sorted(list(set([int(key.decode('utf-8').split(':')[2]) for key in epoch_keys])), reverse=True)
    if (page - 1) * limit < len(epoch_ends):
        epoch_ends_data = epoch_ends[(page - 1) * limit:page * limit]
    else:
        epoch_ends_data = []
    semaphore = asyncio.BoundedSemaphore(25)
    epochs = []
    epoch_status_tasks = [
        bound_check_epoch_finalization(project_id, epoch_end, request.app.state.reader_redis_pool, semaphore=semaphore, request=request)
        for epoch_end in epoch_ends_data
    ]
    epoch_status_task_results = await asyncio.gather(*epoch_status_tasks)

    for i in range(len(epoch_ends_data)):
        finalized = False
        if epoch_status_task_results[i][0] == SubmissionAcceptanceStatus.finalized:
            finalized = True
        epochs.append(Epoch(sourcechainEndheight=epoch_ends_data[i], finalized=finalized))

    data = EpochData(projectId=project_id, epochs=epochs)

    return {
        "total": len(epoch_ends),
        "next_page": None if page * limit >= len(
            epoch_ends) else f"/metrics/{project_id}/epochs?page={page + 1}&limit={limit}",
        "prev_page": None if page == 1 else f"/metrics/{project_id}/epochs?page={page - 1}&limit={limit}",
        "data": data
    }


@app.get(
    "/metrics/{snapshotter_alias}/issues",
    response_model=List[SnapshotterAliasIssue],
    responses={404: {"model": Message}}
)
async def get_snapshotter_issues(
        snapshotter_alias: str,
        request: Request,
        response: Response,
        rate_limit_auth_dep: RateLimitAuthCheck = Depends(rate_limit_auth_check)
):
    if not (
            rate_limit_auth_dep.rate_limit_passed and
            rate_limit_auth_dep.authorized and
            rate_limit_auth_dep.owner.active == UserStatusEnum.active
    ):
        return inject_rate_limit_fail_response(rate_limit_auth_dep)
    redis_conn: aioredis.Redis = request.app.state.reader_redis_pool
    snapshotter_details = await redis_conn.get(get_snapshotter_info_key(snapshotter_alias))
    if not snapshotter_details:
        response.status_code = 404
        return dict()
    else:
        snapshotter_data = SnapshotterMetadata.parse_raw(snapshotter_details)
    issues = await redis_conn.zrevrange(
        get_snapshotter_issues_reported_key(snapshotter_data.uuid), 0, -1, withscores=False
    )
    issues_reports = []
    for issue in issues:
        _: dict = json.loads(issue)
        _['alias'] = snapshotter_alias
        _.pop('instanceID')
        issues_reports.append(SnapshotterAliasIssue(**_))
    return issues_reports


# Submission details for an epoch '/metrics/{project_id}/{epoch}/submissionStatus' .
# This shall include whether consensus has been achieved along with final snapshotCID.
# The details of snapshot submissions snapshotterID and submissionTime along with snapshot submitted.
@app.get(
    "/metrics/{project_id}/{epoch}/submissionStatus",
    response_model=List[Submission], responses={404: {"model": Message}}
)
async def get_submission_status(
        project_id: str,
        epoch: str,
        request: Request,
        rate_limit_auth_dep: RateLimitAuthCheck = Depends(rate_limit_auth_check)
):
    if not (
            rate_limit_auth_dep.rate_limit_passed and
            rate_limit_auth_dep.authorized and
            rate_limit_auth_dep.owner.active == UserStatusEnum.active
    ):
        return inject_rate_limit_fail_response(rate_limit_auth_dep)
    """Returns the submission details for the given project and epoch, including whether consensus has been achieved 
    and the final snapshot CID. Also includes the details of snapshot submissions, such as snapshotter ID and 
    submission time. """

    submission_schedule = await get_submission_schedule(project_id, epoch, request.app.state.reader_redis_pool, request)
    
    if not submission_schedule:
        return JSONResponse(status_code=404, content={
            "message": f"Submission schedule for projectID {project_id} and epoch {epoch} not found"})

    submission_data = await request.app.state.reader_redis_pool.hgetall(
        get_epoch_submissions_htable_key(project_id, epoch)
    )

    if not submission_data:
        return JSONResponse(status_code=404,
                            content={"message": f"Project with projectID {project_id} and epoch {epoch} not found"})

    submissions = []
    for snapshotter_uuid, v in submission_data.items():
        snapshotter_uuid, v = snapshotter_uuid.decode("utf-8"), json.loads(v)
        if v["submittedTS"] < submission_schedule.end:
            submission_status = SubmissionStatus.within_schedule
        else:
            submission_status = SubmissionStatus.delayed
        snapshotter_name = await request.app.state.reader_redis_pool.hget(
            get_snapshotter_info_snapshotter_mapping_key(),
            snapshotter_uuid
        )
        submissions.append(
            Submission(
                snapshotterName=snapshotter_name.decode('utf-8'),
                submittedTS=v["submittedTS"],
                snapshotCID=v["snapshotCID"],
                submissionStatus=submission_status
            )
        )

    return submissions


@app.get("/currentEpoch", response_model=EpochInfo, responses={404: {"model": Message}})
async def get_current_epoch(
        request: Request,
        response: Response
):
    """
    Returns the current epoch information.

    Returns:
        dict: A dictionary with the following keys:
            "chain-id" (int): The chain ID.
            "epochStartBlockHeight" (int): The epoch start block height.
            "epochEndBlockHeight" (int): The epoch end block height.
    """
    # Get the current epoch end block height from Redis
    epoch_end_block_height = await request.app.state.writer_redis_pool.get(get_epoch_generator_last_epoch())

    if epoch_end_block_height is None:
        return JSONResponse(status_code=404,
                            content={"message": "Epoch not found! Make sure the system ticker is running."})
    epoch_end_block_height = int(epoch_end_block_height.decode("utf-8"))
    # Calculate the epoch start block height using the epoch length from the configuration
    epoch_start_block_height = epoch_end_block_height - settings.chain.epoch.height + 1

    # Return the current epoch information as a JSON response
    return {
        "chainId": settings.chain.chain_id,
        "epochStartBlockHeight": epoch_start_block_height,
        "epochEndBlockHeight": epoch_end_block_height
    }
