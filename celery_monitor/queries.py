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

        # Sort queue stats by name
        queue_stats.sort(key=lambda q: q.queue_name)

        return [QueueStats(queue_name="total", count=total_count), *queue_stats]

    except Exception:
        return []


@dataclass
class WorkerStats:
    name: str
    status: str  # online, offline
    active_tasks: int
    pool_size: int | None = None
    max_concurrency: int | None = None


def get_worker_stats() -> list[WorkerStats]:
    """Get stats for all Celery workers."""
    try:
        from celery import current_app

        inspect = current_app.control.inspect(timeout=0.1)

        active_workers = inspect.active()

        workers = []

        if active_workers:
            for worker_name, active_tasks_list in active_workers.items():
                active_count = len(active_tasks_list) if active_tasks_list else 0

                workers.append(
                    WorkerStats(
                        name=worker_name,
                        status="online",
                        active_tasks=active_count,
                        pool_size=None,
                        max_concurrency=None,
                    )
                )

        if has_django_celery_result():
            from django_celery_results.models import TaskResult

            recent_workers = (
                TaskResult.objects.exclude(worker__isnull=True)
                .values_list("worker", flat=True)
                .distinct()[:50]
            )

            online_worker_names = {w.name for w in workers}
            for worker_name in recent_workers:
                if worker_name not in online_worker_names:
                    workers.append(
                        WorkerStats(
                            name=worker_name,
                            status="offline",
                            active_tasks=0,
                        )
                    )

        workers.sort(key=lambda w: (w.status != "online", w.name))
        return workers

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
                    decoded = json.loads(msg)
                    if "headers" in decoded and "task" in decoded["headers"]:
                        task_name = decoded["headers"]["task"]
                    elif "task" in decoded:
                        task_name = decoded["task"]
                    else:
                        task_name = "unknown"

                    task_types[task_name] = task_types.get(task_name, 0) + 1
                except (json.JSONDecodeError, KeyError):
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
    avg_runtime: float | None


def get_task_execution_stats(
    hours: int | None = 1, sort_by: str = "total_count", sort_order: str = "desc"
) -> list[TaskExecutionStats]:
    if not has_django_celery_result():
        return []

    try:
        from django.db.models import Avg, Count, F, Q
        from django_celery_results.models import TaskResult

        queryset = TaskResult.objects.all()

        if hours is not None:
            time_threshold = timezone.now() - timedelta(hours=hours)
            queryset = queryset.filter(date_done__gte=time_threshold)

        stats = queryset.values("task_name").annotate(
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

        reverse = sort_order == "desc"
        if sort_by == "task_name":
            result.sort(key=lambda x: x.task_name, reverse=reverse)
        elif sort_by == "total_count":
            result.sort(key=lambda x: x.total_count, reverse=reverse)
        elif sort_by == "success_count":
            result.sort(key=lambda x: x.success_count, reverse=reverse)
        elif sort_by == "failure_count":
            result.sort(key=lambda x: x.failure_count, reverse=reverse)
        elif sort_by == "avg_runtime":
            result.sort(
                key=lambda x: x.avg_runtime if x.avg_runtime is not None else -1,
                reverse=reverse,
            )

        return result

    except Exception:
        return []
