# Taskiq + FastAPI Integration Reference

Based on the official taskiq-fastapi example: https://github.com/taskiq-python/examples/tree/master/fastapi-app

## Project Structure

```
my_project/
├── my_app/
│   ├── __init__.py
│   ├── __main__.py      # FastAPI app entry point
│   ├── broker.py        # or tkq.py - broker declaration
│   ├── tasks.py         # Task definitions
│   ├── settings.py      # Pydantic settings
│   ├── dependencies.py  # FastAPI dependencies
│   ├── lifespan.py      # Lifespan handlers
│   └── routes.py        # API routes
└── tests/
    └── test_tasks.py
```

## Complete FastAPI Integration Example

### 1. Settings (settings.py)

```python
from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    env: str = Field(default="prod", description="Environment: prod, dev, pytest")
    postgres_url: str = Field(
        default="postgresql://user:pass@localhost:5432/db",
        description="PostgreSQL connection URL"
    )
    nats_urls: str = Field(
        default="nats://localhost:4222/",
        description="NATS server URLs, comma-separated"
    )
    redis_url: str = Field(
        default="redis://localhost",
        description="Redis URL for result backend"
    )

    class Config:
        env_file = ".env"
        env_prefix = "MY_APP_"

settings = Settings()
```

### 2. Broker Declaration (tkq.py or broker.py)

```python
import taskiq_fastapi
from taskiq import InMemoryBroker
from taskiq_nats import PullBasedJetStreamBroker
from taskiq_redis import RedisAsyncResultBackend

from my_app.settings import settings

# Use PullBasedJetStreamBroker for NATS
broker = PullBasedJetStreamBroker(
    settings.nats_urls.split(","),
    queue="my_app_queue",
).with_result_backend(
    RedisAsyncResultBackend(settings.redis_url),
)

# Use InMemoryBroker for testing
if settings.env.lower() == "pytest":
    broker = InMemoryBroker()

# Initialize taskiq-fastapi integration
taskiq_fastapi.init(broker, "my_app.__main__:get_app")
```

### 3. Lifespan Handlers (lifespan.py)

```python
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from psycopg_pool import AsyncConnectionPool

from my_app.settings import settings
from my_app.tkq import broker

async def setup_db(app: FastAPI) -> None:
    """Initialize database connection pool."""
    app.state.pg_pool = AsyncConnectionPool(settings.postgres_url, open=False)
    await app.state.pg_pool.open()
    
    async with app.state.pg_pool.connection() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS my_objs (
                id SERIAL PRIMARY KEY,
                name TEXT
            )
        """)

async def shutdown_db(app: FastAPI) -> None:
    """Close database connections."""
    await app.state.pg_pool.close()

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application lifespan - startup and shutdown."""
    await setup_db(app)
    
    # Only start broker if not running as worker
    if not broker.is_worker_process:
        await broker.startup()
    
    yield
    
    # Only shutdown broker if not running as worker
    if not broker.is_worker_process:
        await broker.shutdown()
    
    await shutdown_db(app)
```

### 4. FastAPI App Entry Point (__main__.py)

```python
from fastapi import FastAPI
from my_app.lifespan import lifespan

app = FastAPI()
```

Run with: `uvicorn my_app.__main__:app`

### 5. Dependencies (dependencies.py)

```python
from typing import Any, AsyncGenerator

from fastapi import Request
from psycopg import AsyncCursor, Rollback
from psycopg_pool import AsyncConnectionPool
from taskiq import TaskiqDepends

async def get_cursor(
    request: Request = TaskiqDepends(),
) -> AsyncGenerator[AsyncCursor[Any], None]:
    """Get a database cursor from the connection pool."""
    pool: AsyncConnectionPool = request.app.state.pg_pool
    
    async with pool.connection() as conn:
        async with conn.cursor(binary=True) as cur:
            yield cur

async def get_trans_cursor(
    request: Request = TaskiqDepends(),
) -> AsyncGenerator[AsyncCursor[Any], None]:
    """Get a database cursor with transaction support."""
    pool: AsyncConnectionPool = request.app.state.pg_pool
    
    async with pool.connection() as conn:
        async with conn.cursor(binary=True) as cur:
            async with conn.transaction() as trans:
                try:
                    yield cur
                except Exception:
                    # Rollback on exception
                    raise Rollback(trans)
```

### 6. Tasks (tasks.py)

```python
from datetime import datetime
from logging import getLogger
from typing import Any

from psycopg import AsyncCursor
from taskiq import TaskiqDepends

from my_app.dependencies import get_trans_cursor
from my_app.dtos import InputObjectDTO
from my_app.tkq import broker

logger = getLogger(__name__)

@broker.task
async def delayed_save(
    target: InputObjectDTO,
    cursor: AsyncCursor[Any] = TaskiqDepends(get_trans_cursor),
) -> bool:
    """Save an object to the database with transaction."""
    logger.info("Saving object with name '%s'", target.name)
    await cursor.execute(
        "INSERT INTO my_objs(name) VALUES (%s)",
        params=(target.name,),
    )
    return True

@broker.task(schedule=[{"cron": "* * * * *"}])
async def scheduled_task(
    cursor: AsyncCursor[Any] = TaskiqDepends(get_trans_cursor),
):
    """Run every minute to count objects."""
    cur = await cursor.execute("SELECT COUNT(*) FROM my_objs")
    count = await cur.fetchone()
    if count is not None:
        logger.info("Number of objects: %s", count[0])

@broker.task
async def dynamic_schedule_task(msg: str | None = None) -> None:
    """Dynamically scheduled task."""
    logger.info("Executed at %s with message %s", datetime.now(), msg)
```

### 7. API Routes (routes.py)

```python
import datetime
from typing import Any, List

from fastapi import APIRouter, Depends
from psycopg import AsyncCursor

from my_app.dependencies import get_cursor
from my_app.dto import InputObjectDTO, OutputObjectDTO
from my_app import tasks
from my_app.tkq_sched import redis_source

router = APIRouter()

@router.get("/objects", response_model=List[OutputObjectDTO])
async def get_objects(
    cursor: AsyncCursor[Any] = Depends(get_cursor),
    offset: int = 0,
    limit: int = 10,
) -> List[OutputObjectDTO]:
    """Fetch objects from database."""
    await cursor.execute(
        "SELECT id, name FROM my_objs OFFSET %s LIMIT %s;",
        params=(offset, limit),
    )
    return [
        OutputObjectDTO(id=id, name=name) 
        for id, name in await cursor.fetchall()
    ]

@router.put("/objects")
async def put_object(input_obj: InputObjectDTO) -> dict:
    """Create object via delayed task."""
    task = await tasks.delayed_save.kiq(input_obj)
    result = await task.wait_result()
    if result.return_value:
        return {"state": "saved"}
    return {"state": "conflict"}

@router.post("/schedule")
async def schedule_task(dto: InputScheduleDTO) -> None:
    """Dynamically schedule a task."""
    target_time = datetime.datetime.now(datetime.UTC) + datetime.timedelta(
        minutes=dto.delay,
    )
    await tasks.dynamic_schedule_task.schedule_by_time(
        source=redis_source,
        time=target_time,
        msg=dto.message,
    )
```

### 8. Scheduler Setup (tkq_sched.py)

```python
from taskiq import TaskiqScheduler
from taskiq.schedule_sources import LabelScheduleSource
from taskiq_redis import RedisScheduleSource

from my_app.settings import settings
from my_app.tkq import broker

redis_source = RedisScheduleSource(settings.redis_url)

scheduler = TaskiqScheduler(
    broker,
    sources=[
        RedisScheduleSource(settings.redis_url),  # Dynamic scheduling
        LabelScheduleSource(broker),  # Label-based scheduling
    ],
)
```

### 9. Testing Setup

conftest.py:
```python
import pytest
import taskiq_fastapi
from my_app import create_app
from my_app.tkq import broker

@pytest.fixture
def anyio_backend():
    return 'asyncio'

@pytest.fixture
def fastapi_app():
    return create_app()

@pytest.fixture(autouse=True)
async def init_taskiq_deps(fastapi_app):
    """Populate dependency context for InMemoryBroker testing."""
    taskiq_fastapi.populate_dependency_context(broker, fastapi_app)
    yield
    broker.custom_dependency_context = {}
```

test_tasks.py:
```python
import pytest
from my_app.tasks import delayed_save
from my_app.dto import InputObjectDTO

@pytest.mark.anyio
async def test_delayed_save():
    result = await delayed_save(InputObjectDTO(name="test"))
    assert result is True
```

## Key Points

1. **Always check `broker.is_worker_process`** before calling startup/shutdown to avoid conflicts when running as worker
2. **Use `TaskiqDepends()`** for FastAPI dependencies in task parameters
3. **Mark Request/HTTPConnection parameters** with `TaskiqDepends()` for proper DI
4. **In tests**, use `populate_dependency_context` to enable DI resolution with InMemoryBroker
5. **Use `await_inplace=True`** on InMemoryBroker to auto-await tasks in tests
6. **Generator dependencies** (yield-based) support transaction rollback via `Rollback` exception
