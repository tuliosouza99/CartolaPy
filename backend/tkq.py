import taskiq_fastapi
from taskiq import ZeroMQBroker

broker = ZeroMQBroker()
taskiq_fastapi.init(broker, "main:get_app")
