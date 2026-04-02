import logging

from apscheduler.schedulers.background import BackgroundScheduler

from backend.core.config import DATA_DIR
from backend.core.scheduler import full_rebuild_job

logger = logging.getLogger(__name__)


def create_lifespan():
    scheduler = BackgroundScheduler()

    async def lifespan(app):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        logger.info("Starting APScheduler for full rebuild every 5 minutes")
        scheduler.add_job(
            full_rebuild_job,
            "interval",
            minutes=5,
            id="full_rebuild",
            replace_existing=True,
        )
        scheduler.start()
        yield
        scheduler.shutdown()
        logger.info("APScheduler shutdown complete")

    return lifespan
