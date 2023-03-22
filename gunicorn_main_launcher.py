import os
import logging
import sys
import resource
from gunicorn.app.base import BaseApplication
from gunicorn.glogging import Logger
from loguru import logger
from settings.conf import settings
from consensus_entry_point import app

def post_worker_init(worker):
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (settings.rlimit.file_descriptors, hard))


class StandaloneApplication(BaseApplication):
    """Our Gunicorn application."""

    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self):
        config = {
            key: value for key, value in self.options.items()
            if key in self.cfg.settings and value is not None
        }
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application


if __name__ == '__main__':


    options = {
        "bind": f"{settings.consensus_service.host}:{settings.consensus_service.port}",
        "keepalive": settings.consensus_service.keepalive_secs,
        "workers": settings.consensus_service.gunicorn_workers,
        "timeout": 120,
        "worker_class": "uvicorn.workers.UvicornWorker",
        "post_worker_init": post_worker_init
    }

    StandaloneApplication(app, options).run()
