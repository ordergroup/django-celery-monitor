from unittest.mock import patch

import pytest

from celery_monitor.models import DashboardStatusCount

try:
    import fakeredis as _fakeredis

    from celery_monitor.enums import TaskField
    from celery_monitor.redis_keys import (
        REDIS_KEY_RECENT_TASKS,
        REDIS_KEY_STATS_TASK,
        REDIS_KEY_STATS_TASK_INDEX,
        REDIS_KEY_STATUS_COUNTS,
        REDIS_KEY_TASK_DETAILS,
        REDIS_KEY_TASKS_NAMES,
        REDIS_KEY_WORKERS_NAMES,
    )
    from celery_monitor.results_monitor.redis_results import RedisResultsMonitor

    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False


@pytest.mark.skipif(not HAS_REDIS, reason="redis not installed")
class TestRedisResultsMonitor:
    @pytest.fixture
    def fake_redis(self):
        return _fakeredis.FakeRedis(decode_responses=True)

    @pytest.fixture
    def monitor(self, fake_redis):
        with patch(
            "celery_monitor.results_monitor.redis_results.get_results_client",
            return_value=fake_redis,
        ):
            return RedisResultsMonitor()

    def _add_task(
        self, r, task_id, task_name, status, worker, started_ts=None, done_ts=None
    ):
        data = {
            TaskField.TASK_NAME: task_name,
            TaskField.STATUS: status,
            TaskField.WORKER: worker,
        }
        if started_ts is not None:
            data[TaskField.DATE_STARTED] = str(started_ts)
        if done_ts is not None:
            data[TaskField.DATE_DONE] = str(done_ts)
        r.hset(REDIS_KEY_TASK_DETAILS.format(task_id=task_id), mapping=data)
        r.zadd(REDIS_KEY_RECENT_TASKS, {task_id: done_ts or started_ts or 0.0})
        r.sadd(REDIS_KEY_TASKS_NAMES, task_name)
        r.sadd(REDIS_KEY_WORKERS_NAMES, worker)
        r.hincrby(REDIS_KEY_STATUS_COUNTS, status, 1)

    def test_get_overall_status_counts(self, monitor, fake_redis):
        fake_redis.hset(
            REDIS_KEY_STATUS_COUNTS, mapping={"SUCCESS": "5", "FAILURE": "2"}
        )

        result = monitor.get_overall_status_counts()

        assert result[0].status == "total"
        assert result[0].count == 7
        counts = {r.status: r.count for r in result[1:]}
        assert counts["SUCCESS"] == 5
        assert counts["FAILURE"] == 2

    def test_get_overall_status_counts_empty(self, monitor):
        result = monitor.get_overall_status_counts()
        assert result == [DashboardStatusCount("total", 0)]

    def test_get_last_hour_status_counts(self, monitor, fake_redis):
        import time

        now = time.time()
        self._add_task(
            fake_redis,
            "t1",
            "tasks.add",
            "SUCCESS",
            "worker1@host",
            now - 600,
            now - 300,
        )
        self._add_task(
            fake_redis,
            "t2",
            "tasks.add",
            "FAILURE",
            "worker1@host",
            now - 400,
            now - 200,
        )
        self._add_task(
            fake_redis,
            "t3",
            "tasks.add",
            "SUCCESS",
            "worker1@host",
            now - 7200,
            now - 7000,
        )

        result = monitor.get_last_hour_status_counts()

        assert result[0].status == "total"
        assert result[0].count == 2
        counts = {r.status: r.count for r in result[1:]}
        assert counts["SUCCESS"] == 1
        assert counts["FAILURE"] == 1

    def test_get_recent_tasks(self, monitor, fake_redis):
        import time

        now = time.time()
        self._add_task(
            fake_redis, "t1", "tasks.add", "SUCCESS", "worker1@host", now - 10, now
        )
        self._add_task(
            fake_redis,
            "t2",
            "tasks.process",
            "FAILURE",
            "worker2@host",
            now - 20,
            now - 5,
        )

        result = monitor.get_recent_tasks()

        assert len(result) == 2
        assert result[0].task_id == "t1"  # highest score (most recent) first

    def test_get_recent_tasks_filter_by_status(self, monitor, fake_redis):
        import time

        now = time.time()
        self._add_task(
            fake_redis, "t1", "tasks.add", "SUCCESS", "worker1@host", now - 10, now
        )
        self._add_task(
            fake_redis,
            "t2",
            "tasks.process",
            "FAILURE",
            "worker2@host",
            now - 20,
            now - 5,
        )

        result = monitor.get_recent_tasks(status="SUCCESS")

        assert len(result) == 1
        assert result[0].task_id == "t1"

    def test_get_recent_tasks_filter_by_worker(self, monitor, fake_redis):
        import time

        now = time.time()
        self._add_task(
            fake_redis, "t1", "tasks.add", "SUCCESS", "worker1@host", now - 10, now
        )
        self._add_task(
            fake_redis,
            "t2",
            "tasks.process",
            "SUCCESS",
            "worker2@host",
            now - 10,
            now - 1,
        )

        result = monitor.get_recent_tasks(worker="worker2@host")

        assert len(result) == 1
        assert result[0].task_id == "t2"

    def test_get_task_detail(self, monitor, fake_redis):
        import time

        now = time.time()
        self._add_task(
            fake_redis, "t1", "tasks.add", "SUCCESS", "worker1@host", now - 10, now
        )

        result = monitor.get_task_detail("t1")

        assert result is not None
        assert result.task_id == "t1"
        assert result.task_name == "tasks.add"
        assert result.status == "SUCCESS"
        assert result.worker == "worker1@host"

    def test_get_task_detail_not_found(self, monitor):
        with patch.object(
            monitor.workers_monitor, "get_task_detail", return_value=None
        ):
            result = monitor.get_task_detail("nonexistent")
        assert result is None

    def test_get_tasks_paginated(self, monitor, fake_redis):
        import time

        now = time.time()
        for i in range(5):
            self._add_task(
                fake_redis,
                f"t{i}",
                "tasks.add",
                "SUCCESS",
                "worker1@host",
                now - (i + 1) * 10,
                now - i * 10,
            )

        result = monitor.get_tasks(page=0, page_size=3)

        assert result.total == 5
        assert len(result.tasks) == 3

    def test_get_task_execution_stats(self, monitor, fake_redis):
        import time

        from celery_monitor.tasks import _collect_buckets, _save_stats

        now = time.time()
        tasks = []
        for i in range(3):
            self._add_task(
                fake_redis, f"s{i}", "tasks.add", "SUCCESS", "w1@h", now - 10, now
            )
            tasks.append(
                {
                    "task_name": "tasks.add",
                    "status": "SUCCESS",
                    "date_started": str(now - 10),
                    "date_done": str(now),
                    "date_created": str(now - 12),
                }
            )
        self._add_task(fake_redis, "f1", "tasks.add", "FAILURE", "w1@h", now - 10, now)
        tasks.append(
            {
                "task_name": "tasks.add",
                "status": "FAILURE",
                "date_started": str(now - 10),
                "date_done": str(now),
                "date_created": str(now - 12),
            }
        )

        _save_stats(
            fake_redis,
            _collect_buckets(tasks, "task_name"),
            REDIS_KEY_STATS_TASK,
            REDIS_KEY_STATS_TASK_INDEX,
        )

        result = monitor.get_task_execution_stats()

        assert len(result) == 1
        stats = result[0]
        assert stats.task_name == "tasks.add"
        assert stats.total_count == 4
        assert stats.success_count == 3
        assert stats.failure_count == 1
        assert stats.avg_runtime is not None
        assert stats.min_runtime is not None
        assert stats.max_runtime is not None

    def test_get_tasks_names(self, monitor, fake_redis):
        fake_redis.sadd(REDIS_KEY_TASKS_NAMES, "tasks.add", "tasks.process")

        result = monitor.get_tasks_names()

        assert sorted(result) == ["tasks.add", "tasks.process"]

    def test_get_workers_names(self, monitor, fake_redis):
        fake_redis.sadd(REDIS_KEY_WORKERS_NAMES, "worker1@host", "worker2@host")

        result = monitor.get_workers_names()

        assert sorted(result) == ["worker1@host", "worker2@host"]
