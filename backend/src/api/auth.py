from typing import Annotated

from fastapi import Security
from fastapi.security import APIKeyHeader
from pydantic_settings import BaseSettings, SettingsConfigDict


class ApiSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", case_sensitive=False)

    api_key: str | None = None
    admin_api_key: str | None = None
    cors_origins: str = "http://localhost:5173,http://localhost:8501"
    environment: str = "development"

    @property
    def is_test(self) -> bool:
        return self.environment == "pytest"


settings = ApiSettings()

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
ADMIN_API_KEY_HEADER = APIKeyHeader(name="X-Admin-API-Key", auto_error=False)


async def verify_api_key(
    api_key: Annotated[str | None, Security(API_KEY_HEADER)] = None,
) -> str:
    if settings.is_test:
        return "test-api-key"
    if api_key is None:
        raise ValueError("Missing API key")
    if api_key != settings.api_key:
        raise ValueError("Invalid API key")
    return api_key


async def verify_admin_api_key(
    api_key: Annotated[str | None, Security(ADMIN_API_KEY_HEADER)] = None,
) -> str:
    if settings.is_test:
        return "test-admin-api-key"
    if api_key is None:
        raise ValueError("Missing admin API key")
    if api_key != settings.admin_api_key:
        raise ValueError("Invalid admin API key")
    return api_key
