from contextlib import asynccontextmanager

import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI

from backend.api.routes import router
from backend.core.config import DATA_DIR


def create_lifespan():
    scheduler = BackgroundScheduler()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        scheduler.add_job(
            "backend.core.scheduler.full_rebuild_job",
            "interval",
            minutes=5,
            id="full_rebuild",
            replace_existing=True,
        )
        scheduler.start()
        yield
        scheduler.shutdown()

    return lifespan


def create_app() -> FastAPI:
    app = FastAPI(
        title="CartolaPy API",
        description="Backend API for CartolaPy fantasy football analysis",
        lifespan=create_lifespan(),
    )
    app.include_router(router)
    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
