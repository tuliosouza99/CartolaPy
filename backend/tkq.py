import os

import taskiq_fastapi
from taskiq import AsyncBroker, InMemoryBroker, ZeroMQBroker

env = os.environ.get("ENVIRONMENT")

if env == "pytest":
    broker: AsyncBroker = InMemoryBroker(await_inplace=True)
else:
    broker = ZeroMQBroker()

taskiq_fastapi.init(broker, "main:get_app")
