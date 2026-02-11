"""Tests for queue monitor functionality."""

import json

import pytest
from celery import Celery
from kombu import Exchange, Queue

from celery_monitor.queue_monitor import get_queue_monitor
from celery_monitor.queue_monitor.base import QueueMonitor
from celery_monitor.queue_monitor.redis import RedisMonitor


@pytest.fixture
def celery_app():
    """Create a Celery app instance for testing."""
    app = Celery("test_app")
    app.conf.broker_url = "redis://localhost:6379/0"
    app.conf.task_default_queue = "celery"
    return app


@pytest.fixture
def celery_app_with_queues(celery_app):
    """Create a Celery app with multiple configured queues."""
    celery_app.conf.task_queues = [
        Queue("high_priority", Exchange("high_priority"), routing_key="high"),
        Queue("default", Exchange("default"), routing_key="default"),
        Queue("low_priority", Exchange("low_priority"), routing_key="low"),
    ]
    return celery_app


@pytest.fixture
def fake_redis(monkeypatch):
    """Create a fakeredis instance and patch redis.from_url."""
    import fakeredis
    import redis

    fake_client = fakeredis.FakeStrictRedis()

    def mock_from_url(url, **kwargs):
        return fake_client

    monkeypatch.setattr(redis, "from_url", mock_from_url)
    return fake_client


class TestQueueMonitorBase:
    """Tests for base QueueMonitor class."""

    def test_init_sets_broker_url(self, celery_app):
        """Test that QueueMonitor initializes with broker URL from Celery."""
        monitor = QueueMonitor()
        assert monitor.broker_url == celery_app.conf.broker_url

    def test_get_queue_task_types_returns_empty_list(self):
        """Test that base QueueMonitor returns empty list for task types."""
        monitor = QueueMonitor()
        assert monitor.get_queue_task_types() == []

    def test_get_queue_stats_returns_empty_list(self):
        """Test that base QueueMonitor returns empty list for queue stats."""
        monitor = QueueMonitor()
        assert monitor.get_queue_stats() == []

    def test_get_queue_names_with_default_queue(self, celery_app):
        """Test getting queue names with default queue configuration."""
        monitor = QueueMonitor()
        queue_names = monitor.get_queue_names()
        assert queue_names == ["celery"]

    def test_get_queue_names_with_task_queues_list(self, celery_app_with_queues):
        """Test getting queue names from task_queues list."""
        monitor = QueueMonitor()
        queue_names = monitor.get_queue_names()
        assert set(queue_names) == {"high_priority", "default", "low_priority"}

    def test_get_queue_names_with_task_queues_dict(self, celery_app):
        """Test getting queue names from task_queues dict."""
        celery_app.conf.task_queues = {
            "queue1": {"exchange": "ex1"},
            "queue2": {"exchange": "ex2"},
        }
        monitor = QueueMonitor()
        queue_names = monitor.get_queue_names()
        assert set(queue_names) == {"queue1", "queue2"}


class TestRedisMonitor:
    """Tests for RedisMonitor class."""

    def test_init_creates_redis_connection(self, celery_app, fake_redis):
        """Test that RedisMonitor initializes with Redis connection."""
        monitor = RedisMonitor()
        assert monitor.redis is not None
        assert monitor.broker_url == celery_app.conf.broker_url

    def test_get_queue_stats_empty_queues(self, celery_app, fake_redis):
        """Test getting queue stats when queues are empty."""
        monitor = RedisMonitor()
        stats = monitor.get_queue_stats()

        assert len(stats) == 2
        assert stats[0].queue_name == "total"
        assert stats[0].count == 0
        assert stats[1].queue_name == "celery"
        assert stats[1].count == 0

    def test_get_queue_stats_with_tasks(self, celery_app, fake_redis):
        """Test getting queue stats when queues have tasks."""
        fake_redis.rpush("celery", json.dumps({"task": "tasks.add"}))
        fake_redis.rpush("celery", json.dumps({"task": "tasks.multiply"}))
        fake_redis.rpush("celery", json.dumps({"task": "tasks.divide"}))

        monitor = RedisMonitor()
        stats = monitor.get_queue_stats()

        assert len(stats) == 2
        assert stats[0].queue_name == "total"
        assert stats[0].count == 3
        assert stats[1].queue_name == "celery"
        assert stats[1].count == 3

    def test_get_queue_stats_multiple_queues(self, celery_app_with_queues, fake_redis):
        """Test getting queue stats for multiple queues."""
        fake_redis.rpush("high_priority", json.dumps({"task": "tasks.urgent"}))
        fake_redis.rpush("high_priority", json.dumps({"task": "tasks.critical"}))
        fake_redis.rpush("default", json.dumps({"task": "tasks.normal"}))
        fake_redis.rpush("low_priority", json.dumps({"task": "tasks.batch"}))

        monitor = RedisMonitor()
        stats = monitor.get_queue_stats()

        assert len(stats) == 4
        assert stats[0].queue_name == "total"
        assert stats[0].count == 4

        queue_stats_dict = {s.queue_name: s.count for s in stats[1:]}
        assert queue_stats_dict["high_priority"] == 2
        assert queue_stats_dict["default"] == 1
        assert queue_stats_dict["low_priority"] == 1

    def test_get_queue_stats_sorted_by_name(self, celery_app, fake_redis):
        """Test that queue stats are sorted alphabetically by queue name."""
        celery_app.conf.task_queues = [
            Queue("zebra", Exchange("zebra"), routing_key="zebra"),
            Queue("alpha", Exchange("alpha"), routing_key="alpha"),
            Queue("middle", Exchange("middle"), routing_key="middle"),
        ]

        fake_redis.rpush("zebra", json.dumps({"task": "tasks.z"}))
        fake_redis.rpush("alpha", json.dumps({"task": "tasks.a"}))
        fake_redis.rpush("middle", json.dumps({"task": "tasks.m"}))

        monitor = RedisMonitor()
        stats = monitor.get_queue_stats()

        assert stats[0].queue_name == "total"
        assert stats[1].queue_name == "alpha"
        assert stats[2].queue_name == "middle"
        assert stats[3].queue_name == "zebra"

    def test_get_queue_stats_handles_redis_error(
        self, celery_app, fake_redis, monkeypatch
    ):
        """Test that get_queue_stats returns empty list on Redis error."""
        monitor = RedisMonitor()

        def mock_llen_failing(key):
            raise Exception("Redis operation failed")

        monkeypatch.setattr(monitor.redis, "llen", mock_llen_failing)

        stats = monitor.get_queue_stats()
        assert stats == []

    def test_get_queue_task_types_empty_queues(self, celery_app, fake_redis):
        """Test getting task types when queues are empty."""
        monitor = RedisMonitor()
        task_types = monitor.get_queue_task_types()
        assert task_types == []

    def test_get_queue_task_types_with_tasks(self, celery_app, fake_redis):
        """Test getting task types from queue with tasks."""
        fake_redis.rpush("celery", json.dumps({"task": "tasks.add"}))
        fake_redis.rpush("celery", json.dumps({"task": "tasks.add"}))
        fake_redis.rpush("celery", json.dumps({"task": "tasks.multiply"}))

        monitor = RedisMonitor()
        task_types = monitor.get_queue_task_types()

        assert len(task_types) == 2
        task_dict = {t.task_name: t.count for t in task_types}
        assert task_dict["tasks.add"] == 2
        assert task_dict["tasks.multiply"] == 1
        assert all(t.queue_name == "celery" for t in task_types)

    def test_get_queue_task_types_multiple_queues(
        self, celery_app_with_queues, fake_redis
    ):
        """Test getting task types from multiple queues."""
        fake_redis.rpush("high_priority", json.dumps({"task": "tasks.urgent"}))
        fake_redis.rpush("high_priority", json.dumps({"task": "tasks.urgent"}))
        fake_redis.rpush("default", json.dumps({"task": "tasks.normal"}))
        fake_redis.rpush("low_priority", json.dumps({"task": "tasks.batch"}))

        monitor = RedisMonitor()
        task_types = monitor.get_queue_task_types()

        assert len(task_types) == 3
        high_priority_tasks = [t for t in task_types if t.queue_name == "high_priority"]
        assert len(high_priority_tasks) == 1
        assert high_priority_tasks[0].task_name == "tasks.urgent"
        assert high_priority_tasks[0].count == 2

    def test_extract_task_name_from_headers(self, celery_app, fake_redis):
        """Test extracting task name from message headers."""
        monitor = RedisMonitor()
        msg = json.dumps({"headers": {"task": "tasks.from_headers"}}).encode()
        task_name = monitor._extract_task_name(msg)
        assert task_name == "tasks.from_headers"

    def test_extract_task_name_from_body(self, celery_app, fake_redis):
        """Test extracting task name from message body when headers missing."""
        monitor = RedisMonitor()
        msg = json.dumps({"task": "tasks.from_body"}).encode()
        task_name = monitor._extract_task_name(msg)
        assert task_name == "tasks.from_body"

    def test_extract_task_name_invalid_json(self, celery_app, fake_redis):
        """Test that invalid JSON returns None."""
        monitor = RedisMonitor()
        msg = b"not valid json"
        task_name = monitor._extract_task_name(msg)
        assert task_name is None

    def test_extract_task_name_missing_task_field(self, celery_app, fake_redis):
        """Test that message without task field returns unknown."""
        monitor = RedisMonitor()
        msg = json.dumps({"some_field": "value"}).encode()
        task_name = monitor._extract_task_name(msg)
        assert task_name == "unknown"

    def test_count_tasks_in_queue(self, celery_app, fake_redis):
        """Test counting tasks by type in a queue."""
        fake_redis.rpush("celery", json.dumps({"task": "tasks.add"}))
        fake_redis.rpush("celery", json.dumps({"task": "tasks.add"}))
        fake_redis.rpush("celery", json.dumps({"task": "tasks.multiply"}))
        fake_redis.rpush("celery", b"invalid json")

        monitor = RedisMonitor()
        task_counts = monitor._count_tasks_in_queue("celery")

        assert task_counts["tasks.add"] == 2
        assert task_counts["tasks.multiply"] == 1
        assert len(task_counts) == 2


class TestGetQueueMonitor:
    """Tests for get_queue_monitor factory function."""

    def test_returns_redis_monitor_when_redis_available(
        self, celery_app, fake_redis, monkeypatch
    ):
        """Test that get_queue_monitor returns RedisMonitor when Redis is available."""
        from celery_monitor import utils

        monkeypatch.setattr(utils, "has_redis", lambda: True)

        monitor = get_queue_monitor()
        assert isinstance(monitor, RedisMonitor)

    def test_returns_base_monitor_when_redis_unavailable(
        self, celery_app, monkeypatch, fake_redis
    ):
        """Test that get_queue_monitor returns base QueueMonitor when Redis unavailable."""

        def mock_has_redis():
            return False

        import celery_monitor.queue_monitor

        monkeypatch.setattr(celery_monitor.queue_monitor, "has_redis", mock_has_redis)

        monitor = get_queue_monitor()
        assert isinstance(monitor, QueueMonitor)
        assert not isinstance(monitor, RedisMonitor)
