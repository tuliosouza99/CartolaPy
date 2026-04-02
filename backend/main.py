from fastapi import FastAPI

from .lifespan import _lifespan


def get_app():
    app = FastAPI(
        title="CartolaPy API",
        description="Backend API for CartolaPy fantasy football analysis",
        lifespan=_lifespan,
    )

    return app
