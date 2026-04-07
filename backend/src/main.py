from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from .api.auth import settings
from .api.routes import router as api_router, limiter
from .lifespan import _lifespan


def get_app():
    app = FastAPI(
        title="CartolaPy API",
        description="Backend API for CartolaPy fantasy football analysis",
        lifespan=_lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins.split(","),
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["X-API-Key", "X-Admin-API-Key", "Content-Type"],
    )

    app.add_middleware(SlowAPIMiddleware)

    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

    app.include_router(api_router, prefix="/api")

    @app.get("/health")
    async def health_check():
        return {"status": "healthy"}

    @app.options("/{full_path:path}")
    async def options_handler(full_path: str):
        return JSONResponse(status_code=200)

    return app
