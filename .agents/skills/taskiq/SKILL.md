---
name: taskiq
description: |
  Taskiq is an async distributed task queue library for Python, similar to Celery but built for asyncio. Use this skill whenever the user wants to:
  - Set up a task queue system in their Python application
  - Run background/async tasks in a distributed manner
  - Integrate task scheduling with FastAPI, AioHTTP, or other async frameworks
  - Add workers, schedulers, and message brokers (Redis, RabbitMQ, NATS) to their project
  - Implement dependency injection and state management for async tasks
  - Test async tasks properly
  
  This skill triggers for requests like "how do I set up taskiq", "add background tasks to my FastAPI app", "create a task scheduler", "set up a worker process", or any question about distributed task queues in Python.
---

# Taskiq Skill

Taskiq is an async-first distributed task queue library for Python with dependency injection, scheduling, middlewares, and framework integrations.

## Quick Start

### Installation

```bash
pip install taskiq
```

For production use, install with recommended brokers:
```bash
pip install taskiq-aio-pika taskiq-redis taskiq-nats
```

### Basic Broker Setup

Create a `broker.py` file with your broker declaration:

```python
from taskiq import InMemoryBroker

broker = InMemoryBroker()
```

For distributed setup with RabbitMQ + Redis:
```python
from taskiq_aio_pika import AioPikaBroker
from taskiq_redis import RedisAsyncResultBackend

broker = AioPikaBroker(
    "amqp://guest:guest@localhost:5672",
).with_result_backend(RedisAsyncResultBackend("redis://localhost"))
```

### Defining Tasks

```python
from taskiq import InMemoryBroker

broker = InMemoryBroker()

@broker.task
async def add_one(value: int) -> int:
    return value + 1

@broker.task(timeout=30)
async def heavy_task():
    await asyncio.sleep(10)
```

### Running Tasks (Client)

```python
import asyncio
from broker import broker, add_one

async def main():
    await broker.startup()
    
    # Send task to queue
    task = await add_one.kiq(1)
    
    # Wait for result
    result = await task.wait_result(timeout=5)
    
    if not result.is_err:
        print(f"Result: {result.return_value}")
    
    await broker.shutdown()

asyncio.run(main())
```

### Running Workers

```bash
# Start worker
taskiq worker module.path:broker

# With file system discovery (auto-import tasks.py files)
taskiq worker module.path:broker -fsd

# With hot reload
pip install "taskiq[reload]"
taskiq worker module.path:broker --reload
```

### Running Schedulers

```bash
taskiq scheduler module.path:scheduler
```

## Core Concepts

### Brokers

Brokers send messages to queues and receive them for processing. Built-in options:
- `InMemoryBroker` - Development only, no network
- `AioPikaBroker` - RabbitMQ via aio-pika
- `NatsBroker` - NATS via taskiq-nats
- `ListQueueBroker` - Redis list-based queue

Production recommended: `taskiq-aio-pika` or `taskiq-nats` for broker, `taskiq-redis` for result backend.

### Kicker

Kicker forms the message for the broker. Use `.kiq()` to send tasks:

```python
# Basic send
await my_task.kiq(arg1, arg2)

# With kicker for customization
await my_task.kicker().with_labels(label="value").kiq(arg1, arg2)

# Different broker
await my_task.kicker().with_broker(other_broker).kiq(arg1)
```

### Labels

Labels add metadata to tasks for routing, scheduling, etc.:

```python
@broker.task(my_label=1, task_name="custom.name")
async def my_task():
    pass

# Or via kicker
await my_task.kicker().with_labels(cron="*/5 * * * *").kiq()
```

### Result Backend

By default, brokers don't store results. Add a result backend:

```python
from taskiq_redis import RedisAsyncResultBackend

broker = AioPikaBroker("amqp://localhost").with_result_backend(
    RedisAsyncResultBackend("redis://localhost")
)
```

## FastAPI Integration

Install: `pip install taskiq-fastapi`

### Setup

```python
# tkq.py
import taskiq_fastapi
from taskiq import ZeroMQBroker

broker = ZeroMQBroker()
taskiq_fastapi.init(broker, "my_app.__main__:app")
```

### Lifespan Management

```python
# lifespan.py
from contextlib import asynccontextmanager
from fastapi import FastAPI
from my_app.tkq import broker

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    if not broker.is_worker_process:
        await broker.startup()
    yield
    # Shutdown
    if not broker.is_worker_process:
        await broker.shutdown()

app = FastAPI(lifespan=lifespan)
```

### Dependency Injection with FastAPI

Mark FastAPI dependencies with `TaskiqDepends`:

```python
from fastapi import Request
from taskiq import TaskiqDepends

async def get_redis_pool(request: Request = TaskiqDepends()):
    return request.app.state.redis_pool
```

### Testing FastAPI + Taskiq

```python
import taskiq_fastapi
from my_app.tkq import broker

@pytest.fixture(autouse=True)
def init_taskiq_deps(fastapi_app):
    taskiq_fastapi.populate_dependency_context(broker, fastapi_app)
    yield
    broker.custom_dependency_context = {}
```

## State and Dependencies

### Worker Startup/Shutdown Events

```python
from taskiq import TaskiqEvents, TaskiqState

@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def startup(state: TaskiqState):
    state.redis = await create_redis_pool()

@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def shutdown(state: TaskiqState):
    await state.redis.disconnect()
```

### Dependency Injection

```python
from typing import Annotated
from taskiq import TaskiqDepends, Context

def get_db_connection(context: Context = TaskiqDepends()) -> DBConnection:
    return context.state.db

@broker.task
async def my_task(db: Annotated[DBConnection, TaskiqDepends(get_db_connection)]):
    await db.query("SELECT * FROM users")
```

Generator dependencies for startup/teardown:

```python
async def get_session() -> AsyncGenerator[Session, None]:
    session = await create_session()
    yield session
    await session.close()

@broker.task
async def my_task(session: Session = TaskiqDepends(get_session)):
    await session.execute(...)
```

## Scheduling Tasks

### Label-based Scheduling

```python
from taskiq import TaskiqScheduler
from taskiq.schedule_sources import LabelScheduleSource

scheduler = TaskiqScheduler(
    broker=broker,
    sources=[LabelScheduleSource(broker)]
)

@broker.task(schedule=[{"cron": "*/5 * * * *", "args": [1]}])
async def heavy_task(value: int):
    pass
```

### Dynamic Scheduling with Redis

```python
from taskiq_redis import ListRedisScheduleSource
import datetime

redis_source = ListRedisScheduleSource("redis://localhost")

scheduler = TaskiqScheduler(broker, sources=[redis_source])

# Schedule by time
await my_task.schedule_by_time(
    source=redis_source,
    time=datetime.datetime.now(datetime.UTC) + timedelta(minutes=5),
    arg1="value"
)

# Schedule by interval
await my_task.schedule_by_interval(
    source=redis_source,
    interval=timedelta(seconds=30),
    arg1="value"
)

# Schedule by cron
await my_task.schedule_by_cron(
    source=redis_source,
    cron="0 * * * *",
    arg1="value"
)

# Unschedule
schedule = await my_task.schedule_by_time(...)
await schedule.unschedule()
```

## Middlewares

```python
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.message import TaskiqMessage

class MyMiddleware(TaskiqMiddleware):
    async def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        message.labels["my_label"] = "value"
        return message
    
    def post_send(self, message: TaskiqMessage) -> None:
        print(f"Message {message.task_id} was sent")
    
    async def post_execute(self, message: TaskiqMessage) -> None:
        print(f"Task {message.task_name} completed")
```

Add to broker:
```python
broker.add_middleware(MyMiddleware())
```

## Context and Task Control

```python
from typing import Annotated
from taskiq import Context, TaskiqDepends

@broker.task
async def my_task(context: Annotated[Context, TaskiqDepends()]):
    # Access the current message
    task_id = context.message.task_id
    
    # Requeue or reject the task
    if some_condition:
        await context.requeue()  # Put back in queue
    else:
        await context.reject()  # Drop the message
```

## Testing

### InMemoryBroker for Tests

```python
import os
from taskiq import InMemoryBroker

env = os.environ.get("ENVIRONMENT")
broker = InMemoryBroker() if env == "pytest" else get_prod_broker()

# Auto-await tasks
broker = InMemoryBroker(await_inplace=True)
```

### Testing Tasks

```python
@pytest.mark.anyio
async def test_task():
    result = await my_task(1, 2)
    assert result == 3
```

### Awaiting Unawaited Tasks

```python
# Option 1: await_inplace
broker = InMemoryBroker(await_inplace=True)

# Option 2: wait_all
await some_function_that_kiqs()
await broker.wait_all()
```

## CLI Options

### Worker
```bash
taskiq worker broker:broker -fsd \
    --workers 4 \
    --max-async-tasks 100 \
    --ack-type when_executed \
    --reload
```

### Scheduler
```bash
taskiq scheduler broker:scheduler \
    --skip-first-run \
    --update-interval 60
```

## Environment Variables

```bash
# Docker commands for dependencies
docker run --rm -d -p 5672:5672 -p 15672:15672 \
    --env "RABBITMQ_DEFAULT_USER=guest" \
    --env "RABBITMQ_DEFAULT_PASS=guest" \
    rabbitmq:3.8.27-management-alpine

docker run --rm -d -p 6379:6379 redis
```

## Key Resources

- Documentation: https://taskiq-python.github.io/guide/
- GitHub: https://github.com/taskiq-python/taskiq
- FastAPI Example: https://github.com/taskiq-python/examples/tree/master/fastapi-app
