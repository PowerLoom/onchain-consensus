import asyncio

from httpx import AsyncClient
from pydantic import BaseModel

from settings.conf import settings
from utils.default_logger import logger


logger = logger.bind(
    module='PowerLoom|OnChainConsensus|NotificationUtils',
)


def misc_notification_callback_result_handler(fut: asyncio.Future):
    try:
        r = fut.result()
    except Exception as e:
        logger.opt(exception=True).error(
            'Exception while sending callback or notification: {}', e,
        )
    else:
        logger.debug('Callback or notification result:{}', r)


async def send_failure_notifications(client: AsyncClient, message: BaseModel):
    if settings.reporting.service_url:
        f = asyncio.ensure_future(
            client.post(
                url=settings.reporting.service_url,
                json=message.dict(),
            ),
        )
        f.add_done_callback(misc_notification_callback_result_handler)

    if settings.reporting.slack_url:
        f = asyncio.ensure_future(
            client.post(
                url=settings.reporting.slack_url,
                json=message.dict(),
            ),
        )
        f.add_done_callback(misc_notification_callback_result_handler)
