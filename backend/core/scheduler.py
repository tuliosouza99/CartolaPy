import logging

logger = logging.getLogger(__name__)


def full_rebuild_job():
    import asyncio
    from backend.services.updater import Updater

    logger.info("Starting full rebuild job")

    async def run_rebuild():
        updater = Updater()
        await updater.full_rebuild()

    asyncio.run(run_rebuild())
    logger.info("Full rebuild job completed")
