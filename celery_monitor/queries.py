from dataclasses import dataclass
from datetime import timedelta

from django.db.models import Count
from django.utils import timezone

from celery_monitor.models import CeleryStatusCount
from celery_monitor.utils import has_django_celery_result, has_redis, is_postgres


@dataclass
class DashboardStatusCount:
    status: str
    count: int | None


def get_overall_status_counts() -> list[DashboardStatusCount]:
    has_celery_results = has_django_celery_result()
    is_pg = is_postgres()
    if is_pg and has_celery_results:
        status_counts = CeleryStatusCount.objects.all().order_by("status")
        stats = [DashboardStatusCount(row.status, row.count) for row in status_counts]
    elif not is_pg and has_celery_results:
        from django_celery_results.models import TaskResult

        status_counts = (
            TaskResult.objects.values("status")
            .annotate(count=Count("id"))
            .order_by("status")
        )
        stats = [
            DashboardStatusCount(row["status"], row["count"]) for row in status_counts
        ]
    else:
        stats = []

    return [DashboardStatusCount("total", sum(i.count for i in stats)), *stats]


def get_last_hour_status_counts() -> list[DashboardStatusCount]:
    has_celery_results = has_django_celery_result()

    if has_celery_results:
        from django_celery_results.models import TaskResult

        status_counts = (
            TaskResult.objects.filter(
                date_created__gte=(timezone.now() - timedelta(hours=1))
            )
            .values("status")
            .annotate(count=Count("id"))
            .order_by("status")
        )
        stats = [
            DashboardStatusCount(row["status"], row["count"]) for row in status_counts
        ]
    else:
        stats = []

    return [DashboardStatusCount("total", sum(i.count for i in stats)), *stats]


@dataclass
class QueueStats:
    queue_name: str
    count: int


def get_redis_queue_stats() -> list[QueueStats]:
    """Get statistics for all Redis queues."""
    if not has_redis():
        return []

    try:
        import redis
        from celery import current_app

        broker_url = current_app.conf.broker_url
        r = redis.from_url(broker_url)

        task_queues = current_app.conf.task_queues

        if isinstance(task_queues, dict):
            queue_names = list(task_queues.keys())
        elif task_queues:
            queue_names = [q.name for q in task_queues]
        else:
            queue_names = [current_app.conf.task_default_queue or "celery"]

        queue_stats = []
        total_count = 0

        for queue_name in queue_names:
            count = r.llen(queue_name)
            queue_stats.append(QueueStats(queue_name=queue_name, count=count))
            total_count += count

        return [QueueStats(queue_name="total", count=total_count), *queue_stats]

    except Exception:
        return []


@dataclass
class QueueTaskTypeStats:
    queue_name: str
    task_name: str
    count: int


def get_redis_queue_task_types() -> list[QueueTaskTypeStats]:
    """Get task type breakdown for all Redis queues."""
    if not has_redis():
        return []

    try:
        import json

        import redis
        from celery import current_app

        broker_url = current_app.conf.broker_url
        r = redis.from_url(broker_url)

        task_queues = current_app.conf.task_queues

        if isinstance(task_queues, dict):
            queue_names = list(task_queues.keys())
        elif task_queues:
            queue_names = [q.name for q in task_queues]
        else:
            queue_names = [current_app.conf.task_default_queue or "celery"]

        stats = []

        for queue_name in queue_names:
            task_types = {}

            # Inspect messages in the queue to get task type breakdown
            # We use LRANGE to peek at messages without removing them
            messages = r.lrange(queue_name, 0, -1)
            for msg in messages:
                try:
                    # Celery stores messages as JSON with 'task' field containing task name
                    decoded = json.loads(msg)
                    if "headers" in decoded and "task" in decoded["headers"]:
                        task_name = decoded["headers"]["task"]
                    elif "task" in decoded:
                        task_name = decoded["task"]
                    else:
                        task_name = "unknown"

                    task_types[task_name] = task_types.get(task_name, 0) + 1
                except (json.JSONDecodeError, KeyError):
                    # If we can't parse the message, skip it
                    pass

            for task_name, count in task_types.items():
                stats.append(
                    QueueTaskTypeStats(
                        queue_name=queue_name, task_name=task_name, count=count
                    )
                )

        return stats

    except Exception:
        return []


@dataclass
class TaskExecutionStats:
    task_name: str
    total_count: int
    success_count: int
    failure_count: int
    avg_runtime: float | None  # Average runtime in seconds


def get_task_execution_stats() -> list[TaskExecutionStats]:
    """Get execution time and success/failure stats per task type."""
    if not has_django_celery_result():
        return []

    try:
        from django.db.models import Avg, Count, F, Q
        from django_celery_results.models import TaskResult

        # Get stats grouped by task name
        stats = (
            TaskResult.objects.values("task_name")
            .annotate(
                total_count=Count("id"),
                success_count=Count("id", filter=Q(status="SUCCESS")),
                failure_count=Count("id", filter=Q(status="FAILURE")),
                avg_runtime=Avg(
                    F("date_done") - F("date_started"),
                    filter=Q(
                        status="SUCCESS",
                        date_started__isnull=False,
                        date_done__isnull=False,
                    ),
                ),
            )
            .order_by("-total_count")
        )

        result = []
        for stat in stats:
            avg_seconds = None
            if stat["avg_runtime"]:
                avg_seconds = stat["avg_runtime"].total_seconds()

            result.append(
                TaskExecutionStats(
                    task_name=stat["task_name"],
                    total_count=stat["total_count"],
                    success_count=stat["success_count"],
                    failure_count=stat["failure_count"],
                    avg_runtime=avg_seconds,
                )
            )

        return result

    except Exception:
        return []




