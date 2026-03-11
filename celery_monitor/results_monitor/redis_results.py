import contextlib
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from datetime import timezone as dt_timezone

import redis
from celery import current_app
from django.utils import timezone

from celery_monitor.models import (
    DashboardStatusCount,
    TaskDetail,
    TaskExecutionStats,
    TaskOverview,
    TasksPage,
    WorkerStats,
)
from celery_monitor.redis_keys import (
    REDIS_KEY_RECENT_TASKS,
    REDIS_KEY_STATUS_COUNTS,
    REDIS_KEY_TASK_DETAILS,
    REDIS_KEY_TASKS_NAMES,
    REDIS_KEY_WORKERS_NAMES,
)
from celery_monitor.results_monitor.base import CeleryResultsMonitor
from celery_monitor.results_monitor.workers_results import WorkersCeleryResultsMonitor

logger = logging.getLogger(__name__)

REDIS_SCHEME = "redis://"
REDIS_SECURE_SCHEME = "rediss://"


class RedisResultsMonitor(CeleryResultsMonitor):
    """
    Mixin that uses a custom Redis schema for efficient task monitoring.

    This mixin queries task results from Redis using a custom schema that stores
    additional metadata beyond what Celery's standard result backend provides.

    Redis schema:
    - celery:monitor:tasks:{task_id} -> Hash with task details
    - celery:monitor:tasks:recent -> Sorted set with task IDs by timestamp
    - celery:monitor:status_counts -> Hash with status counts
    - celery:monitor:task_names -> Set of all task names
    - celery:monitor:workers -> Set of all worker names

    To populate this data, you must register the signal handlers in your Celery app:
        from celery_monitor.signals import setup_celery_monitor_signals
        setup_celery_monitor_signals(app)
    """

    def __init__(self):
        super().__init__()
        self.client = self._init_client()
        self.workers_monitor = WorkersCeleryResultsMonitor()

    def _init_client(self) -> redis.Redis:
        redis_url = current_app.conf.result_backend or current_app.conf.broker_url
        if not redis_url:
            raise ValueError(
                "Cannot initialize Redis client. Celery broker_url or result_backend must be set and use Redis"
            )

        return redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=3,
        )

    def get_worker_stats(self, include_offline: bool = False) -> list[WorkerStats]:
        return self.workers_monitor.get_worker_stats(include_offline)

    def get_overall_status_counts(self) -> list[DashboardStatusCount]:
        counts: dict = self.client.hgetall(REDIS_KEY_STATUS_COUNTS)
        stats = [
            DashboardStatusCount(status=status, count=int(count))
            for status, count in sorted(counts.items())
        ]
        total = sum(s.count for s in stats)
        return [DashboardStatusCount("total", total), *stats]

    def get_last_hour_status_counts(self) -> list[DashboardStatusCount]:
        now = timezone.now()
        hour_ago = now - timedelta(hours=1)
        task_ids = self.client.zrangebyscore(
            REDIS_KEY_RECENT_TASKS, hour_ago.timestamp(), now.timestamp()
        )

        status_counts = defaultdict(int)

        if task_ids:
            pipeline = self.client.pipeline()
            for task_id in task_ids:
                pipeline.hget(REDIS_KEY_TASK_DETAILS.format(task_id=task_id), "status")

            for status in pipeline.execute():
                if status:
                    status_counts[status] += 1

        stats = [
            DashboardStatusCount(status=status, count=count)
            for status, count in sorted(status_counts.items())
        ]
        return [DashboardStatusCount("total", sum(status_counts.values())), *stats]

    def get_task_execution_stats(
        self,
        sort_by: str = "total_count",
        sort_order: str = "desc",
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[TaskExecutionStats]:
        start_time = date_from.timestamp() if date_from else float("-inf")
        end_time = date_to.timestamp() if date_to else float("inf")

        task_ids = self.client.zrangebyscore(
            REDIS_KEY_RECENT_TASKS, start_time, end_time
        )
        if not task_ids:
            return []

        pipeline = self.client.pipeline()
        for task_id in task_ids:
            pipeline.hgetall(REDIS_KEY_TASK_DETAILS.format(task_id=task_id))

        stats_by_name = defaultdict(
            lambda: {
                "total": 0,
                "success": 0,
                "failure": 0,
                "runtimes": [],
            }
        )

        for task_data in pipeline.execute():
            task_name = task_data.get("task_name")
            if not task_name:
                continue

            stats_by_name[task_name]["total"] += 1
            status = task_data.get("status")
            if status == "SUCCESS":
                stats_by_name[task_name]["success"] += 1
            elif status == "FAILURE":
                stats_by_name[task_name]["failure"] += 1
            stats_by_name[task_name]["runtimes"].append(get_execution_time(task_data))

        result = []
        for task_name, data in stats_by_name.items():
            runtimes = [r for r in data["runtimes"] if r]
            result.append(
                TaskExecutionStats(
                    task_name=task_name,
                    total_count=data["total"],
                    success_count=data["success"],
                    failure_count=data["failure"],
                    avg_runtime=sum(runtimes) / len(runtimes) if runtimes else None,
                    min_runtime=min(runtimes) if runtimes else None,
                    max_runtime=max(runtimes) if runtimes else None,
                )
            )

        return sorted(
            result,
            key=lambda x: getattr(x, sort_by, None) or -1,
            reverse=(sort_order == "desc"),
        )

    def get_recent_tasks(
        self,
        status: str | None = None,
        task_name: str | None = None,
        worker: str | None = None,
        limit: int = 50,
    ) -> list[TaskOverview]:
        """
        Get recent tasks from Redis, if filtering is applied this function will return
        always less results than limit, because filtering is happening after fetching the data from redis.
        """
        task_ids = self.client.zrevrange(REDIS_KEY_RECENT_TASKS, 0, limit * 2 - 1)
        if not task_ids:
            return []

        pipeline = self.client.pipeline()
        for task_id in task_ids:
            pipeline.hgetall(REDIS_KEY_TASK_DETAILS.format(task_id=task_id))

        recent_tasks = []
        for task_data in pipeline.execute():
            task_status = task_data.get("status")
            task_name_value = task_data.get("task_name")
            task_worker = task_data.get("worker")

            if status and task_status != status:
                continue
            if task_name and task_name_value != task_name:
                continue
            if worker and task_worker != worker:
                continue

            recent_tasks.append(
                TaskOverview(
                    task_id=task_data.get("task_id"),
                    task_name=task_name_value,
                    status=task_status,
                    worker=task_worker,
                    date_started=get_timestamp(task_data, "date_started"),
                    date_done=get_timestamp(task_data, "date_done"),
                    execution_time=get_execution_time(task_data),
                )
            )

        return recent_tasks

    def get_task_detail(self, task_id: str) -> TaskDetail | None:
        """Get detailed information about a specific task from Redis."""
        task_data: dict = self.client.hgetall(
            REDIS_KEY_TASK_DETAILS.format(task_id=task_id)
        )
        if not task_data:
            return self.workers_monitor.get_task_detail(task_id)

        date_started = get_timestamp(task_data, "date_started")
        date_done = get_timestamp(task_data, "date_done")
        date_created = get_timestamp(task_data, "date_created")

        return TaskDetail(
            task_id=task_id,
            task_name=task_data.get("task_name"),
            status=task_data.get("status"),
            worker=task_data.get("worker", "unknown"),
            date_started=date_started.isoformat() if date_started else None,
            date_created=date_created.isoformat() if date_created else None,
            date_done=date_done.isoformat() if date_done else None,
            task_args=task_data.get("task_args"),
            task_kwargs=task_data.get("task_kwargs"),
            result=task_data.get("result"),
            traceback=task_data.get("traceback"),
            meta=None,
            periodic_task_name=None,
            exception=task_data.get("exception"),
            exception_type=task_data.get("exception_type"),
        )

    def get_tasks(
        self,
        status: str | None = None,
        task_name: str | None = None,
        worker: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        page: int = 0,
        page_size: int = 50,
    ) -> TasksPage:
        start = page * page_size

        if date_from or date_to:
            start_time = date_from.timestamp() if date_from else float("-inf")
            end_time = date_to.timestamp() if date_to else float("inf")
            all_task_ids = list(
                reversed(
                    self.client.zrangebyscore(
                        REDIS_KEY_RECENT_TASKS,
                        start_time,
                        end_time,
                    )
                )
            )
            total = len(all_task_ids)
            task_ids = all_task_ids[start : start + page_size]
        else:
            total = self.client.zcard(REDIS_KEY_RECENT_TASKS)
            task_ids = self.client.zrevrange(
                REDIS_KEY_RECENT_TASKS,
                start=start,
                end=start + page_size - 1,
            )

        return TasksPage(tasks=self._get_tasks_overviews(task_ids), total=total)

    def _get_tasks_overviews(self, task_ids: list[str]) -> list[TaskOverview]:
        pipeline = self.client.pipeline()
        for task_id in task_ids:
            pipeline.hgetall(REDIS_KEY_TASK_DETAILS.format(task_id=task_id))

        return [
            TaskOverview(
                task_id=task_data.get("task_id"),
                task_name=task_data.get("task_name"),
                status=task_data.get("status"),
                worker=task_data.get("worker"),
                date_started=get_timestamp(task_data, "date_started"),
                date_done=get_timestamp(task_data, "date_done"),
                execution_time=get_execution_time(task_data),
            )
            for task_data in pipeline.execute()
        ]

    def get_tasks_names(self) -> list[str]:
        return sorted(self.client.smembers(REDIS_KEY_TASKS_NAMES))

    def get_workers_names(self) -> list[str]:
        return sorted(self.client.smembers(REDIS_KEY_WORKERS_NAMES))


def get_execution_time(task_data: dict) -> float | None:
    started = task_data.get("date_started")
    done = task_data.get("date_done")
    if not started or not done:
        return None

    with contextlib.suppress(ValueError, TypeError):
        elapsed = float(done) - float(started)
        return elapsed if elapsed >= 0 else None

    return None


def get_timestamp(task_data: dict, prop_name: str) -> datetime | None:
    value = task_data.get(prop_name)
    if not value:
        return None

    with contextlib.suppress(ValueError, TypeError):
        return datetime.fromtimestamp(float(value), tz=dt_timezone.utc)
