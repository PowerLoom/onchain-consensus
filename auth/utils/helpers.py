import time
from datetime import datetime
from datetime import timedelta

import pydantic
from async_limits import parse_many
from fastapi import Depends
from fastapi import Request
from fastapi.responses import JSONResponse
from redis import asyncio as aioredis
from json.decoder import JSONDecodeError

from auth.utils.data_models import AuthCheck
from auth.utils.data_models import RateLimitAuthCheck
from auth.utils.data_models import UserStatusEnum
from auth.utils.redis_keys import user_details_htable
from data_models import PeerUUIDIncludedRequests, SnapshotterMetadata
from helpers import redis_keys
from settings.conf import settings as consensus_settings
from utils.rate_limiter import generic_rate_limiter

async def incr_success_calls_count(
        auth_redis_conn: aioredis.Redis,
        rate_limit_auth_dep: RateLimitAuthCheck,
):
    # on success
    await auth_redis_conn.hincrby(
        name=user_details_htable(rate_limit_auth_dep.owner.email),
        key='callsCount',
        amount=1,
    )


async def incr_throttled_calls_count(
        auth_redis_conn: aioredis.Redis,
        rate_limit_auth_dep: RateLimitAuthCheck,
):
    # on throttle
    await auth_redis_conn.hincrby(
        name=user_details_htable(rate_limit_auth_dep.owner.email),
        key='throttledCount',
        amount=1,
    )


def inject_rate_limit_fail_response(rate_limit_auth_check_dependency: RateLimitAuthCheck) -> JSONResponse:
    if rate_limit_auth_check_dependency.authorized:
        response_body = {
            'error': {
                'details': f'Rate limit exceeded: {rate_limit_auth_check_dependency.violated_limit}. '
                           'Check response body and headers for more details on backoff.',
                'data': {
                    'rate_violated': str(rate_limit_auth_check_dependency.violated_limit),
                    'retry_after': rate_limit_auth_check_dependency.retry_after,
                    'violating_domain': rate_limit_auth_check_dependency.current_limit,
                },
            },
        }
        response_headers = {
            'Retry-After': (datetime.now() + timedelta(rate_limit_auth_check_dependency.retry_after)).isoformat(),
        }
        response_status = 429
    else:
        response_headers = dict()
        response_body = {
            'error': {
                'details': rate_limit_auth_check_dependency.reason,
            },
        }
        if 'cache error' in rate_limit_auth_check_dependency.reason:
            response_status = 500
        else:  # return 401 for unauthorized access for every other reason
            response_status = 401
    return JSONResponse(content=response_body, status_code=response_status, headers=response_headers)


# TODO: cacheize user active statuses to avoid redis calls on every auth check
async def auth_check(
        request: Request,
) -> AuthCheck:
    auth_redis_conn: aioredis.Redis = request.app.state.writer_redis_pool
    try:
        uuid_included_request_body = PeerUUIDIncludedRequests.parse_obj(await request.json())
    except (pydantic.ValidationError, JSONDecodeError):
        uuid_included_request_body = False

    if not uuid_included_request_body:
        # public access. create owner based on IP address
        if 'CF-Connecting-IP' in request.headers:
            user_ip = request.headers['CF-Connecting-IP']
        elif 'X-Forwarded-For' in request.headers:
            proxy_data = request.headers['X-Forwarded-For']
            ip_list = proxy_data.split(',')
            user_ip = ip_list[0]  # first address in list is User IP
        else:
            user_ip = request.client.host  # For local development
        ip_user_dets_b = await auth_redis_conn.get(redis_keys.get_snapshotter_info_key(alias=user_ip))
        if not ip_user_dets_b:
            public_owner = SnapshotterMetadata(
                alias=user_ip,
                name=user_ip,
                email=user_ip,
                rate_limit=consensus_settings.rate_limit,
                active=UserStatusEnum.active,
                callsCount=0,
                throttledCount=0,
                next_reset_at=int(time.time()) + 86400,
            )
            await auth_redis_conn.set(redis_keys.get_snapshotter_info_key(alias=user_ip), public_owner.json())
        else:
            public_owner = SnapshotterMetadata.parse_raw(ip_user_dets_b)
        return AuthCheck(
            authorized=public_owner.active == UserStatusEnum.active,
            api_key='public',
            reason='',
            owner=public_owner
        )
    else:
        uuid_in_request = uuid_included_request_body.instanceID
        uuid_found = await auth_redis_conn.sismember(
            redis_keys.get_snapshotter_info_allowed_snapshotters_key(), uuid_in_request
        )
        if not uuid_found:
            return AuthCheck(
                authorized=uuid_found,
                api_key='dummy' if not uuid_found else uuid_in_request,
                reason='illegal peer/instance ID supplied' if not uuid_found else ''
            )
        else:
            snapshotter_alias = await auth_redis_conn.hget(
                redis_keys.get_snapshotter_info_snapshotter_mapping_key(),
                uuid_in_request
            )
            snapshotter_dets_b = await auth_redis_conn.get(redis_keys.get_snapshotter_info_key(
                alias=snapshotter_alias.decode('utf-8'))
            )
            snapshotter_details = SnapshotterMetadata.parse_raw(snapshotter_dets_b)
            return AuthCheck(
                authorized=snapshotter_details.active == UserStatusEnum.active,
                api_key=uuid_in_request,
                owner=snapshotter_details
            )


async def rate_limit_auth_check(
        request: Request,
        auth_check_dep: AuthCheck = Depends(auth_check),
) -> RateLimitAuthCheck:
    if auth_check_dep.authorized:
        auth_redis_conn: aioredis.Redis = request.app.state.writer_redis_pool
        try:
            passed, retry_after, violated_limit = await generic_rate_limiter(
                parsed_limits=parse_many(auth_check_dep.owner.rate_limit),
                key_bits=[
                    auth_check_dep.api_key,
                ],
                redis_conn=auth_redis_conn,
                rate_limit_lua_script_shas=request.app.state.rate_limit_lua_script_shas
            )
        except:
            auth_check_dep.authorized = False
            auth_check_dep.reason = 'internal cache error'
            return RateLimitAuthCheck(
                **auth_check_dep.dict(),
                rate_limit_passed=False,
                retry_after=1,
                violated_limit='',
                current_limit=auth_check_dep.owner.rate_limit,
            )
        else:
            ret = RateLimitAuthCheck(
                **auth_check_dep.dict(),
                rate_limit_passed=passed,
                retry_after=retry_after,
                violated_limit=violated_limit,
                current_limit=auth_check_dep.owner.rate_limit,
            )
            if not passed:
                await incr_throttled_calls_count(auth_redis_conn, ret)
            return ret
        finally:
            if auth_check_dep.owner.next_reset_at <= int(time.time()):
                owner_updated_obj = auth_check_dep.owner.copy(deep=True)
                owner_updated_obj.callsCount = 0
                owner_updated_obj.throttledCount = 0
                owner_updated_obj.next_reset_at = int(time.time()) + 86400
                await auth_redis_conn.set(redis_keys.get_snapshotter_info_key(alias=auth_check_dep.owner.alias), owner_updated_obj.json())
    else:
        return RateLimitAuthCheck(
            **auth_check_dep.dict(),
            rate_limit_passed=False,
            retry_after=1,
            violated_limit='',
            current_limit='',
        )
