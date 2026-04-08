import pytest
from src.tkq import broker
from taskiq import InMemoryBroker


class TestUpdateDataScheduledTask:
    def test_task_is_scheduled_with_5_min_cron(self):
        from src.tasks import update_data_task

        schedules = update_data_task.labels.get("schedule", [])
        assert len(schedules) > 0, "Task should have at least one schedule"

        cron_schedules = [s for s in schedules if "cron" in s]
        assert len(cron_schedules) > 0, "Task should have a cron schedule"

        cron_expr = cron_schedules[0]["cron"]
        assert "*/5" in cron_expr, f"Task should run every 5 minutes, got: {cron_expr}"

    def test_task_is_registered_in_broker(self):
        from src.tasks import update_data_task

        all_tasks = broker.get_all_tasks()
        assert update_data_task.task_name in all_tasks, (
            f"Task should be registered in broker. Available: {list(all_tasks.keys())}"
        )

    @pytest.mark.anyio
    async def test_task_updates_rodada_id_state(self, fastapi_app):
        from src.tasks import update_data_task

        result = await update_data_task.kiq()
        await result.wait_result()

        assert fastapi_app.state.rodada_id_state["current"] == 15
        assert fastapi_app.state.rodada_id_state["previous"] == 15

    @pytest.mark.anyio
    async def test_task_returns_result_with_rodada_id(self, fastapi_app):
        from src.tasks import update_data_task

        result = await update_data_task.kiq()
        task_result = await result.wait_result()

        assert task_result.return_value is not None
        assert (
            "rodada_id" in task_result.return_value
            or "new_rodada_id" in task_result.return_value
        )

    @pytest.mark.anyio
    async def test_task_returns_rodada_id_when_unchanged(self, fastapi_app):
        from src.tasks import update_data_task

        result = await update_data_task.kiq()
        task_result = await result.wait_result()

        assert task_result.return_value["rodada_changed"] is False
        assert task_result.return_value["rodada_id"] == 15


class TestTaskDependencies:
    def test_broker_is_zeromq_or_inmemory(self):
        broker_class_name = broker.__class__.__name__
        assert "ZeroMQ" in broker_class_name or "InMemory" in broker_class_name, (
            f"Expected ZeroMQ or InMemory broker, got: {broker_class_name}"
        )

    def test_inmemory_broker_used_in_pytest(self):
        import os

        if os.environ.get("ENVIRONMENT") == "pytest":
            assert isinstance(broker, InMemoryBroker), (
                "Broker should be InMemoryBroker when ENVIRONMENT=pytest"
            )

    def test_inmemory_broker_has_await_inplace(self):
        if isinstance(broker, InMemoryBroker):
            assert broker.await_inplace is True, (
                "InMemoryBroker should have await_inplace=True for synchronous test execution"
            )
