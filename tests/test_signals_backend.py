import logging
from unittest.mock import MagicMock, Mock, patch

import pytest

from celery_monitor.signals_backend.base import safe_signal_handler
from celery_monitor.signals_backend.noop import NoopSignalsResultBackend

try:
    import json

    import fakeredis as _fakeredis
    import lz4.frame as _lz4

    from celery_monitor.enums import TaskField
    from celery_monitor.redis_keys import (
        REDIS_KEY_RECENT_TASKS,
        REDIS_KEY_STATUS_COUNTS,
        REDIS_KEY_TASK_DETAILS,
        REDIS_KEY_TASK_PAYLOAD,
        REDIS_KEY_TASKS_NAMES,
        REDIS_KEY_WORKERS_NAMES,
    )
    from celery_monitor.signals_backend.redis import RedisSignalsResultBackend

    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False


class TestSafeSignalHandler:
    def test_passes_through_return_value(self):
        @safe_signal_handler
        def my_handler(*args, **kwargs):
            return "result"

        assert my_handler() == "result"

    def test_catches_exception_and_logs(self, caplog):
        @safe_signal_handler
        def failing_handler(*args, **kwargs):
            raise ValueError("boom")

        with caplog.at_level(logging.WARNING):
            result = failing_handler()

        assert result is None
        assert "failing_handler" in caplog.text
        assert "boom" in caplog.text

    def test_passes_args_and_kwargs(self):
        @safe_signal_handler
        def echo_handler(a, b, key=None):
            return (a, b, key)

        assert echo_handler(1, 2, key="x") == (1, 2, "x")


class TestNoopSignalsResultBackend:
    @pytest.fixture
    def backend(self):
        from celery import Celery

        app = Mock(spec=Celery)
        return NoopSignalsResultBackend(app)

    def test_task_prerun_handler_does_nothing(self, backend):
        result = backend.task_prerun_handler(
            task_id="t1", task=Mock(), args=[], kwargs={}
        )
        assert result is None

    def test_task_postrun_handler_does_nothing(self, backend):
        result = backend.task_postrun_handler(
            task_id="t1", task=Mock(), state="SUCCESS"
        )
        assert result is None

    def test_task_published_handler_does_nothing(self, backend):
        result = backend.task_published_handler(
            sender="tasks.add", headers={"id": "t1"}, body=None, routing_key="default"
        )
        assert result is None

    def test_task_failure_handler_does_nothing(self, backend):
        result = backend.task_failure_handler(task_id="t1", exception=ValueError("e"))
        assert result is None

    def test_task_retry_handler_does_nothing(self, backend):
        result = backend.task_retry_handler(task_id="t1", reason="retry")
        assert result is None

    def test_task_revoked_handler_does_nothing(self, backend):
        result = backend.task_revoked_handler(request=Mock(), terminated=False)
        assert result is None


@pytest.mark.skipif(not HAS_REDIS, reason="redis not installed")
class TestRedisSignalsResultBackend:
    @pytest.fixture
    def fake_redis_server(self):
        return _fakeredis.FakeServer()

    @pytest.fixture
    def fake_redis(self, fake_redis_server):
        return _fakeredis.FakeRedis(server=fake_redis_server, decode_responses=True)

    @pytest.fixture
    def fake_redis_bytes(self, fake_redis_server):
        return _fakeredis.FakeRedis(server=fake_redis_server, decode_responses=False)

    @pytest.fixture
    def backend(self, fake_redis, fake_redis_bytes):
        from celery import Celery

        app = Mock(spec=Celery)
        app.conf.result_backend = "redis://localhost:6379/0"
        app.conf.broker_url = "redis://localhost:6379/0"
        with (
            patch.object(
                RedisSignalsResultBackend, "_get_redis_client", return_value=fake_redis
            ),
            patch.object(
                RedisSignalsResultBackend,
                "_get_bytes_redis_client",
                return_value=fake_redis_bytes,
            ),
        ):
            return RedisSignalsResultBackend(app)

    def _make_task(self, name="tasks.add", hostname="worker1@host"):
        task = Mock()
        task.name = name
        task.request.hostname = hostname
        return task

    def test_task_published_handler_stores_initial_task_data(self, backend, fake_redis):
        backend.task_published_handler(
            sender="tasks.add",
            headers={"id": "t1", "task": "tasks.add"},
            body=None,
            routing_key="high_priority",
        )

        data = fake_redis.hgetall(REDIS_KEY_TASK_DETAILS.format(task_id="t1"))
        assert (
            "task_id" not in data
        )  # task_id is the key suffix, not stored in the hash
        assert data[TaskField.TASK_NAME] == "tasks.add"
        assert data[TaskField.QUEUE_NAME] == "high_priority"
        assert data[TaskField.STATUS] == "QUEUED"
        assert TaskField.DATE_CREATED in data

    def test_task_published_handler_adds_to_recent_tasks(self, backend, fake_redis):
        backend.task_published_handler(
            sender="tasks.add", headers={"id": "t1"}, body=None, routing_key="default"
        )

        assert fake_redis.zscore(REDIS_KEY_RECENT_TASKS, "t1") is not None

    def test_task_published_handler_tracks_task_name(self, backend, fake_redis):
        backend.task_published_handler(
            sender="tasks.process",
            headers={"id": "t1"},
            body=None,
            routing_key="default",
        )

        assert fake_redis.sismember(REDIS_KEY_TASKS_NAMES, "tasks.process")

    def test_task_published_handler_increments_queued_count(self, backend, fake_redis):
        backend.task_published_handler(
            sender="tasks.add", headers={"id": "t1"}, body=None, routing_key="default"
        )

        assert fake_redis.hget(REDIS_KEY_STATUS_COUNTS, "QUEUED") == "1"

    def test_task_published_handler_skips_missing_task_id(self, backend, fake_redis):
        backend.task_published_handler(
            sender="tasks.add", headers={}, body=None, routing_key="default"
        )

        assert fake_redis.hgetall(REDIS_KEY_STATUS_COUNTS) == {}

    def test_prerun_after_published_transitions_queued_to_started(
        self, backend, fake_redis
    ):
        backend.task_published_handler(
            sender="tasks.add",
            headers={"id": "t1", "task": "tasks.add"},
            body=None,
            routing_key="default",
        )
        task = self._make_task()
        backend.task_prerun_handler(task_id="t1", task=task, args=[], kwargs={})

        assert fake_redis.hget(REDIS_KEY_STATUS_COUNTS, "QUEUED") == "0"
        assert fake_redis.hget(REDIS_KEY_STATUS_COUNTS, "STARTED") == "1"
        data = fake_redis.hgetall(REDIS_KEY_TASK_DETAILS.format(task_id="t1"))
        assert data[TaskField.STATUS] == "STARTED"

    def test_task_prerun_handler_stores_task_data(self, backend, fake_redis):
        task = self._make_task()
        backend.task_prerun_handler(task_id="t1", task=task, args=[1, 2], kwargs={})

        data = fake_redis.hgetall(REDIS_KEY_TASK_DETAILS.format(task_id="t1"))
        assert data[TaskField.TASK_NAME] == "tasks.add"
        assert data[TaskField.STATUS] == "STARTED"
        assert data[TaskField.WORKER] == "worker1@host"
        assert TaskField.DATE_STARTED in data

    def test_task_prerun_handler_adds_to_recent_tasks(self, backend, fake_redis):
        task = self._make_task()
        backend.task_prerun_handler(task_id="t1", task=task, args=[], kwargs={})

        assert fake_redis.zscore(REDIS_KEY_RECENT_TASKS, "t1") is not None

    def test_task_prerun_handler_tracks_task_and_worker_names(
        self, backend, fake_redis
    ):
        task = self._make_task(name="tasks.process", hostname="worker2@host")
        backend.task_prerun_handler(task_id="t1", task=task, args=[], kwargs={})

        assert fake_redis.sismember(REDIS_KEY_TASKS_NAMES, "tasks.process")
        assert fake_redis.sismember(REDIS_KEY_WORKERS_NAMES, "worker2@host")

    def test_task_prerun_handler_increments_started_count(self, backend, fake_redis):
        task = self._make_task()
        backend.task_prerun_handler(task_id="t1", task=task, args=[], kwargs={})

        assert fake_redis.hget(REDIS_KEY_STATUS_COUNTS, "STARTED") == "1"

    def test_task_postrun_handler_stores_args(
        self, backend, fake_redis, fake_redis_bytes
    ):
        task = self._make_task()
        backend.task_postrun_handler(
            task_id="t1",
            task=task,
            args=[1, 2],
            kwargs={"key": "val"},
            state="SUCCESS",
            retval=42,
        )

        raw = fake_redis_bytes.get(REDIS_KEY_TASK_PAYLOAD.format(task_id="t1"))
        assert raw is not None
        result_data = json.loads(_lz4.decompress(raw))
        assert result_data[TaskField.TASK_ARGS] == "[1, 2]"
        assert result_data[TaskField.TASK_KWARGS] == '{"key": "val"}'
        assert result_data[TaskField.RESULT] == "42"

    def test_task_prerun_handler_uses_hostname_fallback(self, backend, fake_redis):
        task = Mock()
        task.name = "tasks.add"
        task.request = None

        backend.task_prerun_handler(
            task_id="t1", task=task, args=[], kwargs={}, hostname="fallback@host"
        )

        data = fake_redis.hgetall(REDIS_KEY_TASK_DETAILS.format(task_id="t1"))
        assert data[TaskField.WORKER] == "fallback@host"

    def test_task_postrun_handler_updates_status(
        self, backend, fake_redis, fake_redis_bytes
    ):
        fake_redis.hset(
            REDIS_KEY_TASK_DETAILS.format(task_id="t1"),
            mapping={TaskField.STATUS: "STARTED"},
        )
        fake_redis.hincrby(REDIS_KEY_STATUS_COUNTS, "STARTED", 1)

        task = self._make_task()
        backend.task_postrun_handler(
            task_id="t1", task=task, state="SUCCESS", retval=42
        )

        data = fake_redis.hgetall(REDIS_KEY_TASK_DETAILS.format(task_id="t1"))
        assert data[TaskField.STATUS] == "SUCCESS"
        assert TaskField.DATE_DONE in data
        raw = fake_redis_bytes.get(REDIS_KEY_TASK_PAYLOAD.format(task_id="t1"))
        assert json.loads(_lz4.decompress(raw))[TaskField.RESULT] == "42"

    def test_task_postrun_handler_adjusts_status_counts(self, backend, fake_redis):
        fake_redis.hset(
            REDIS_KEY_TASK_DETAILS.format(task_id="t1"),
            mapping={TaskField.STATUS: "STARTED"},
        )
        fake_redis.hincrby(REDIS_KEY_STATUS_COUNTS, "STARTED", 1)

        task = self._make_task()
        backend.task_postrun_handler(
            task_id="t1", task=task, state="SUCCESS", retval=None
        )

        assert fake_redis.hget(REDIS_KEY_STATUS_COUNTS, "STARTED") == "0"
        assert fake_redis.hget(REDIS_KEY_STATUS_COUNTS, "SUCCESS") == "1"

    def test_task_postrun_handler_handles_non_serializable_retval(
        self, backend, fake_redis, fake_redis_bytes
    ):
        fake_redis.hset(
            REDIS_KEY_TASK_DETAILS.format(task_id="t1"),
            mapping={TaskField.STATUS: "STARTED"},
        )

        class Unserializable:
            def __repr__(self):
                return "Unserializable()"

        task = self._make_task()
        backend.task_postrun_handler(
            task_id="t1", task=task, state="SUCCESS", retval=Unserializable()
        )

        assert (
            fake_redis_bytes.get(REDIS_KEY_TASK_PAYLOAD.format(task_id="t1"))
            is not None
        )

    def test_task_failure_handler_stores_exception_info(
        self, backend, fake_redis, fake_redis_bytes
    ):
        exc = ValueError("Something went wrong")
        backend.task_failure_handler(task_id="t1", exception=exc)

        data = fake_redis.hgetall(REDIS_KEY_TASK_DETAILS.format(task_id="t1"))
        assert TaskField.EXCEPTION not in data  # exception lives in error key now
        raw = fake_redis_bytes.get(REDIS_KEY_TASK_PAYLOAD.format(task_id="t1"))
        error_data = json.loads(_lz4.decompress(raw))
        assert error_data[TaskField.EXCEPTION] == "Something went wrong"
        assert error_data[TaskField.EXCEPTION_TYPE] == "ValueError"

    def test_task_retry_handler_sets_retry_status(self, backend, fake_redis):
        backend.task_retry_handler(task_id="t1", reason="Retry limit")

        data = fake_redis.hgetall(REDIS_KEY_TASK_DETAILS.format(task_id="t1"))
        assert data.get(TaskField.STATUS) == "RETRY"
        assert data.get(TaskField.RETRY_REASON) == "Retry limit"
        assert data.get(TaskField.RETRY_COUNT) == "1"

    def test_task_retry_handler_without_reason(self, backend, fake_redis):
        backend.task_retry_handler(task_id="t1", reason=None)

        data = fake_redis.hgetall(REDIS_KEY_TASK_DETAILS.format(task_id="t1"))
        assert data.get(TaskField.STATUS) == "RETRY"
        assert TaskField.RETRY_REASON not in data

    def test_task_revoked_handler_updates_status(self, backend, fake_redis):
        fake_redis.hset(
            REDIS_KEY_TASK_DETAILS.format(task_id="t1"),
            mapping={TaskField.STATUS: "STARTED"},
        )
        fake_redis.hincrby(REDIS_KEY_STATUS_COUNTS, "STARTED", 1)

        request = Mock()
        request.id = "t1"
        backend.task_revoked_handler(request=request, terminated=True)

        data = fake_redis.hgetall(REDIS_KEY_TASK_DETAILS.format(task_id="t1"))
        assert data[TaskField.STATUS] == "REVOKED"
        assert TaskField.DATE_DONE in data
        assert data[TaskField.TERMINATED] == "True"

    def test_task_revoked_handler_adjusts_status_counts(self, backend, fake_redis):
        fake_redis.hset(
            REDIS_KEY_TASK_DETAILS.format(task_id="t1"),
            mapping={TaskField.STATUS: "STARTED"},
        )
        fake_redis.hincrby(REDIS_KEY_STATUS_COUNTS, "STARTED", 1)

        request = Mock()
        request.id = "t1"
        backend.task_revoked_handler(request=request, terminated=False)

        assert fake_redis.hget(REDIS_KEY_STATUS_COUNTS, "STARTED") == "0"
        assert fake_redis.hget(REDIS_KEY_STATUS_COUNTS, "REVOKED") == "1"

    def test_task_revoked_handler_no_request_does_nothing(self, backend, fake_redis):
        backend.task_revoked_handler(request=None, terminated=False)
        # No Redis keys should have been written
        assert fake_redis.hgetall(REDIS_KEY_STATUS_COUNTS) == {}

    def test_get_redis_client_handles_exception(self):
        import sys

        from celery import Celery

        app = Mock(spec=Celery)
        app.conf.result_backend = None
        app.conf.broker_url = "redis://localhost:6379/0"

        mock_redis_module = MagicMock()
        mock_redis_module.from_url.side_effect = Exception("Connection refused")
        with patch.dict(sys.modules, {"redis": mock_redis_module}):
            backend = RedisSignalsResultBackend.__new__(RedisSignalsResultBackend)
            backend.app = app
            backend.task_data_ttl = 3600
            result = backend._get_redis_client()

        assert result is None
