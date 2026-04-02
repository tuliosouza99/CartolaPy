# Taskiq Available Components

## Brokers

### InMemoryBroker
- **Package**: `taskiq` (built-in)
- **Use case**: Development only, local testing
- **Limitations**: No network, no distributed execution

```python
from taskiq import InMemoryBroker
broker = InMemoryBroker(await_inplace=True)  # Auto-await in tests
```

### AioPikaBroker (RabbitMQ)
- **Package**: `taskiq-aio-pika`
- **Use case**: Production message broker with RabbitMQ
- **Features**: Reliable message delivery, acknowledgements

```python
from taskiq_aio_pika import AioPikaBroker
broker = AioPikaBroker("amqp://guest:guest@localhost:5672")
```

### NatsBroker
- **Package**: `taskiq-nats`
- **Use case**: Lightweight, high-performance message broker
- **Variants**: 
  - `NatsBroker` - Pub/sub style
  - `PullBasedJetStreamBroker` - Queue-based with pull consumers

```python
from taskiq_nats import NatsBroker, PullBasedJetStreamBroker

# Pub/sub
broker = NatsBroker(["nats://localhost:4222"])

# Queue-based (recommended for workers)
broker = PullBasedJetStreamBroker(
    ["nats://localhost:4222"],
    queue="my_queue",
)
```

### ListQueueBroker (Redis)
- **Package**: `taskiq-redis`
- **Use case**: Simple Redis-based queue
- **Note**: Basic implementation, may lack features of dedicated brokers

```python
from taskiq_redis import ListQueueBroker
broker = ListQueueBroker("redis://localhost:6379/0")
```

## Result Backends

### RedisAsyncResultBackend
- **Package**: `taskiq-redis`
- **Use case**: Store task execution results
- **Recommended for production**

```python
from taskiq_redis import RedisAsyncResultBackend

result_backend = RedisAsyncResultBackend("redis://localhost")
broker = broker.with_result_backend(result_backend)
```

### InMemoryResultBackend
- **Package**: `taskiq` (built-in with InMemoryBroker)
- **Use case**: Development and testing

## Schedule Sources

### LabelScheduleSource
- **Package**: `taskiq` (built-in)
- **Use case**: Tasks scheduled via `@broker.task(schedule=[...])` decorator labels

```python
from taskiq.schedule_sources import LabelScheduleSource
scheduler = TaskiqScheduler(broker, [LabelScheduleSource(broker)])
```

### RedisScheduleSource
- **Package**: `taskiq-redis`
- **Use case**: Dynamic scheduling stored in Redis

```python
from taskiq_redis import RedisScheduleSource
redis_source = RedisScheduleSource("redis://localhost")
scheduler = TaskiqScheduler(broker, [redis_source])
```

### ListRedisScheduleSource
- **Package**: `taskiq-redis`
- **Use case**: Dynamic scheduling with list-based storage (FIFO)

```python
from taskiq_redis import ListRedisScheduleSource
redis_source = ListRedisScheduleSource("redis://localhost")
```

## Serializers

### Default: JSONSerializer
- Built-in, uses standard JSON

### ORJSONSerializer
- **Package**: `taskiq[orjson]` or `pip install orjson`
- **Use case**: Faster serialization, native datetime/uuid support

```python
from taskiq.serializers import ORJSONSerializer
broker = broker.with_serializer(ORJSONSerializer())
```

### MSGPackSerializer
- **Package**: `taskiq[msgpack]` or `pip install msgpack`
- **Use case**: Compact binary format

### CBORSerializer
- **Package**: `taskiq[cbor]` or `pip install cbor2`
- **Use case**: Compact binary with more type support than msgpack

## Framework Integrations

### taskiq-fastapi
- **Package**: `taskiq-fastapi`
- **Use case**: Seamless FastAPI integration with dependency injection

```python
import taskiq_fastapi
taskiq_fastapi.init(broker, "my_app.__main__:app")
```

### taskiq-aiohttp
- **Package**: `taskiq-aiohttp`
- **Use case**: AioHTTP framework integration

### taskiq-faststream
- **Package**: `taskiq-faststream`
- **Use case**: FastStream framework integration (NATS-based)

### taskiq-aiogram
- **Package**: `taskiq-aiogram`
- **Use case**: Aiogram Telegram bot framework integration

## Installation Recommendations

### Minimal (Development)
```bash
pip install taskiq
```

### Production (RabbitMQ + Redis)
```bash
pip install taskiq taskiq-aio-pika taskiq-redis
```

### Production (NATS + Redis)
```bash
pip install taskiq taskiq-nats taskiq-redis
```

### Full Stack (FastAPI + RabbitMQ + Redis)
```bash
pip install taskiq taskiq-aio-pika taskiq-redis taskiq-fastapi
```

### With Extras
```bash
pip install "taskiq[orjson,msgpack,reload]"
```

## Docker Setup

### RabbitMQ
```bash
docker run --rm -d \
  -p 5672:5672 \
  -p 15672:15672 \
  --env "RABBITMQ_DEFAULT_USER=guest" \
  --env "RABBITMQ_DEFAULT_PASS=guest" \
  rabbitmq:3.8.27-management-alpine
```

### Redis
```bash
docker run --rm -d -p 6379:6379 redis
```

### NATS
```bash
docker run --rm -d -p 4222:4222 nats:latest
```
