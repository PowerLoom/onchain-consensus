import asyncio
import json
import time
import uuid
from functools import wraps
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import redis
from fastapi import Depends
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from redis import asyncio as aioredis

from auth.utils.data_models import RateLimitAuthCheck
from auth.utils.data_models import UserStatusEnum
from auth.utils.helpers import inject_rate_limit_fail_response
from auth.utils.helpers import rate_limit_auth_check
from data_models import Message
from data_models import SnapshotterIssue
from helpers.redis_keys import get_snapshotter_issues_reported_key
from settings.conf import settings
from utils.default_logger import logger
from utils.rate_limiter import load_rate_limiter_scripts
from utils.redis_conn import RedisPool

service_logger = logger.bind(
    service='PowerLoom|OnChainConsensus|ServiceEntry',
)


def acquire_bounded_semaphore(fn):
    @wraps(fn)
    async def wrapped(*args, **kwargs):
        sem: asyncio.BoundedSemaphore = kwargs['semaphore']
        await sem.acquire()
        result = None
        try:
            result = await fn(*args, **kwargs)
        except Exception as e:
            service_logger.opt(exception=True).error(
                f'Error in {fn.__name__}: {e}',
            )
            pass
        finally:
            sem.release()
            return result

    return wrapped


# setup CORS origins stuff
origins = ['*']

redis_lock = redis.Redis()

app = FastAPI()
app.logger = service_logger

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
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


@app.on_event('startup')
async def startup_boilerplate():
    app.state.aioredis_pool = RedisPool(writer_redis_conf=settings.redis)
    await app.state.aioredis_pool.populate()
    app.state.reader_redis_pool = app.state.aioredis_pool.reader_redis_pool
    app.state.writer_redis_pool = app.state.aioredis_pool.writer_redis_pool
    app.state.rate_limit_lua_script_shas = await load_rate_limiter_scripts(app.state.writer_redis_pool)
    app.state.auth = dict()
    app.state.snapshotter_aliases = dict()


@app.post('/reportIssue')
async def report_issue(
        request: Request,
        req_parsed: SnapshotterIssue,
        response: Response,
        rate_limit_auth_dep: RateLimitAuthCheck = Depends(
            rate_limit_auth_check,
        ),
):
    if not (
            rate_limit_auth_dep.rate_limit_passed and
            rate_limit_auth_dep.authorized and
            rate_limit_auth_dep.owner.active == UserStatusEnum.active
    ):
        return inject_rate_limit_fail_response(rate_limit_auth_dep)

    # TODO: Basic ip based check for now, do a snapshotter ID check by getting snapshotters from contract if necessary

    # Updating time of reporting to avoid manual incorrect time manipulation
    req_parsed.timeOfReporting = int(time.time())
    await request.app.state.writer_redis_pool.zadd(
        name=get_snapshotter_issues_reported_key(
            snapshotter_id=req_parsed.instanceID,
        ),
        mapping={json.dumps(req_parsed.dict()): req_parsed.timeOfReporting},
    )

    # pruning expired items
    await request.app.state.writer_redis_pool.zremrangebyscore(
        get_snapshotter_issues_reported_key(
            snapshotter_id=req_parsed.instanceID,
        ), 0,
        int(time.time()) - (7 * 24 * 60 * 60),
    )

    return JSONResponse(status_code=200, content={'message': f'Reported Issue.'})


@app.get(
    '/metrics/{snapshotter_alias}/issues',
    response_model=List[SnapshotterIssue],
    responses={404: {'model': Message}},
)
async def get_snapshotter_issues(
        snapshotter_id: str,
        request: Request,
        response: Response,
        rate_limit_auth_dep: RateLimitAuthCheck = Depends(
            rate_limit_auth_check,
        ),
):
    if not (
            rate_limit_auth_dep.rate_limit_passed and
            rate_limit_auth_dep.authorized and
            rate_limit_auth_dep.owner.active == UserStatusEnum.active
    ):
        return inject_rate_limit_fail_response(rate_limit_auth_dep)
    redis_conn: aioredis.Redis = request.app.state.reader_redis_pool

    issues = await redis_conn.zrevrange(
        get_snapshotter_issues_reported_key(snapshotter_id), 0, -1, withscores=False,
    )
    issues_reports = []
    for issue in issues:
        issues_reports.append(SnapshotterIssue(**json.loads(issue)))
    return issues_reports
