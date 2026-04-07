from fastapi import FastAPI

from .api.routes import router as api_router
from .lifespan import _lifespan


def get_app():
    app = FastAPI(
        title="CartolaPy API",
        description="Backend API for CartolaPy fantasy football analysis",
        lifespan=_lifespan,
    )

    app.include_router(api_router, prefix="/api")

    return app
