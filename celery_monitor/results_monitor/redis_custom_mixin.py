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
    RecentTask,
    RecentTasksData,
    TaskDetail,
    TaskExecutionStats,
)

logger = logging.getLogger(__name__)

REDIS_SCHEME = "redis://"
REDIS_SECURE_SCHEME = "rediss://"


class RedisCustomResultsMixin:
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
        self._redis_client = None

    def _get_redis_client(self) -> redis.Redis | None:
        if self._redis_client is not None:
            return self._redis_client

        try:
            redis_url = current_app.conf.broker_url or current_app.conf.result_backend
            if not redis_url:
                logger.warning(
                    "Cannot initialize Redis client. Celery broker_url or result_backend must be set and use Redis"
                )
                return None

            self._redis_client = redis.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=3,
            )
            return self._redis_client
        except Exception as e:
            logger.warning("Could not connect to Redis: %s", e)
            return None

    def get_overall_status_counts(self) -> list[DashboardStatusCount]:
        client = self._get_redis_client()
        if not client:
            return []

        counts: dict = client.hgetall("celery:monitor:status_counts")
        stats = [
            DashboardStatusCount(status=status, count=int(count))
            for status, count in sorted(counts.items())
        ]
        total = sum(s.count for s in stats)
        return [DashboardStatusCount("total", total), *stats]

    def get_last_hour_status_counts(self) -> list[DashboardStatusCount]:
        client = self._get_redis_client()
        if not client:
            return []

        # Get tasks from the last hour using sorted set
        now = timezone.now()
        hour_ago = now - timedelta(hours=1)
        task_ids = client.zrangebyscore(
            "celery:monitor:tasks:recent", hour_ago.timestamp(), now.timestamp()
        )

        status_counts = defaultdict(int)
        total = 0

        if task_ids:
            pipeline = client.pipeline()
            for task_id in task_ids:
                pipeline.hget(f"celery:monitor:tasks:{task_id}", "status")

            statuses = pipeline.execute()

            for status in statuses:
                if not status:
                    continue

                status_counts[status] += 1
                total += 1

        stats = [
            DashboardStatusCount(status=status, count=count)
            for status, count in sorted(status_counts.items())
        ]

        return [DashboardStatusCount("total", total), *stats]

    def get_task_execution_stats(
        self,
        hours: int | None = 1,
        sort_by: str = "total_count",
        sort_order: str = "desc",
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[TaskExecutionStats]:
        """Get task execution statistics."""
        client = self._get_redis_client()
        if not client:
            return []

        start_time, end_time = get_datetime_range(hours, date_from, date_to)

        task_ids = client.zrangebyscore(
            "celery:monitor:tasks:recent", start_time, end_time
        )
        if not task_ids:
            return []

        pipeline = client.pipeline()
        for task_id in task_ids:
            pipeline.hgetall(f"celery:monitor:tasks:{task_id}")

        tasks = pipeline.execute()

        stats_by_name = {}

        for task_data in tasks:
            task_name = task_data.get("task_name")
            if not task_name:
                continue

            if task_name not in stats_by_name:
                stats_by_name[task_name] = {
                    "total": 0,
                    "success": 0,
                    "failure": 0,
                    "runtimes": [],
                }

            stats_by_name[task_name]["total"] += 1

            status = task_data.get("status")
            if status == "SUCCESS":
                stats_by_name[task_name]["success"] += 1
            elif status == "FAILURE":
                stats_by_name[task_name]["failure"] += 1

            # Calculate runtime if available
            execution_time = get_execution_time(task_data)
            stats_by_name[task_name]["runtimes"].append(execution_time)

        # Convert to TaskExecutionStats
        result = []
        for task_name, data in stats_by_name.items():
            runtimes = [r for r in data["runtimes"] if r]
            avg_runtime = sum(runtimes) / len(runtimes) if runtimes else None
            min_runtime = min(runtimes) if runtimes else None
            max_runtime = max(runtimes) if runtimes else None

            result.append(
                TaskExecutionStats(
                    task_name=task_name,
                    total_count=data["total"],
                    success_count=data["success"],
                    failure_count=data["failure"],
                    avg_runtime=avg_runtime,
                    min_runtime=min_runtime,
                    max_runtime=max_runtime,
                )
            )

        reverse = sort_order == "desc"
        result = sorted(
            result,
            key=lambda x: (
                getattr(x, sort_by, None)
                if getattr(x, sort_by, None) is not None
                else -1
            ),
            reverse=reverse,
        )
        return result

    def get_recent_tasks(
        self,
        status: str | None = None,
        task_name: str | None = None,
        worker: str | None = None,
        limit: int = 50,
    ) -> RecentTasksData:
        """
        Get recent tasks from Redis, if filtering is applied this function will return
        always less results than limit, because filtering is happening after fetching the data from redis.
        """
        client = self._get_redis_client()
        if not client:
            return RecentTasksData(recent_tasks=[], task_names=[], workers=[])

        # Get recent task IDs (sorted by timestamp, newest first)
        task_ids = client.zrevrange("celery:monitor:tasks:recent", 0, limit * 2 - 1)
        if not task_ids:
            return RecentTasksData(recent_tasks=[], task_names=[], workers=[])

        pipeline = client.pipeline()
        for task_id in task_ids:
            pipeline.hgetall(f"celery:monitor:tasks:{task_id}")

        tasks = pipeline.execute()

        recent_tasks = []
        task_names_set = set()
        workers_set = set()

        for task_data in tasks:
            # Apply filters
            task_status = task_data.get("status")
            task_name_value = task_data.get("task_name")
            task_worker = task_data.get("worker")

            if task_name_value:
                task_names_set.add(task_name_value)
            if task_worker:
                workers_set.add(task_worker)

            if status and task_status != status:
                continue
            if task_name and task_name_value != task_name:
                continue
            if worker and task_worker != worker:
                continue

            execution_time = get_execution_time(task_data)
            date_started = get_timestamp(task_data, "date_started")
            date_done = get_timestamp(task_data, "date_done")

            recent_tasks.append(
                RecentTask(
                    task_id=task_data.get("task_id"),
                    task_name=task_name_value,
                    status=task_status,
                    worker=task_worker,
                    date_started=date_started.isoformat() if date_started else None,
                    date_done=date_done.isoformat() if date_done else None,
                    execution_time=execution_time,
                )
            )

        all_task_names = client.smembers("celery:monitor:task_names")
        all_workers = client.smembers("celery:monitor:workers")

        return RecentTasksData(
            recent_tasks=recent_tasks,
            task_names=sorted(all_task_names or task_names_set),
            workers=sorted(all_workers or workers_set),
        )

    def get_task_detail(self, task_id: str):
        """Get detailed information about a specific task from Redis."""
        client = self._get_redis_client()
        if not client:
            return None

        task_data: dict = client.hgetall(f"celery:monitor:tasks:{task_id}")
        if not task_data:
            return None

        # Convert timestamps to datetime objects for display
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

        return TaskDetail(task_data)


def get_datetime_range(
    hours: int | None = 1,
    date_from: str | None = None,
    date_to: str | None = None,
) -> tuple[float, float]:
    if date_from and date_to:
        try:
            date_from_dt = datetime.fromisoformat(date_from)
            date_to_dt = datetime.fromisoformat(date_to)
            if timezone.is_naive(date_from_dt):
                date_from_dt = timezone.make_aware(date_from_dt)
            if timezone.is_naive(date_to_dt):
                date_to_dt = timezone.make_aware(date_to_dt)
            start_time = date_from_dt.timestamp()
            end_time = date_to_dt.timestamp()
            return start_time, end_time
        except (ValueError, TypeError):
            if hours is not None:
                end_time = timezone.now().timestamp()
                start_time = (timezone.now() - timedelta(hours=hours)).timestamp()
                return start_time, end_time
            else:
                raise
    elif hours is not None:
        end_time = timezone.now().timestamp()
        start_time = (timezone.now() - timedelta(hours=hours)).timestamp()
        return start_time, end_time
    else:
        raise Exception("either hours of date_from and date_to must be defined")


def get_execution_time(task_data: dict) -> float | None:
    started = task_data.get("date_started")
    done = task_data.get("date_done")
    if not started or not done:
        return None

    with contextlib.suppress(ValueError, TypeError):
        return float(done) - float(started)

    return None


def get_timestamp(task_data: dict, prop_name: str) -> datetime | None:
    value = task_data.get(prop_name)
    if not value:
        return None

    with contextlib.suppress(ValueError, TypeError):
        return datetime.fromtimestamp(float(value), tz=dt_timezone.utc)
