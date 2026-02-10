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

    return [DashboardStatusCount("total", sum(i.count for i in stats))] + stats


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

    return [DashboardStatusCount("total", sum(i.count for i in stats))] + stats


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

        queue_names = [q.name for q in current_app.conf.task_queues or []]

        if not queue_names:
            queue_names = ["celery"]

        queue_stats = []
        total_count = 0

        for queue_name in queue_names:
            redis_key = (
                f"celery:{queue_name}"
                if not queue_name.startswith("celery:")
                else queue_name
            )
            count = r.llen(redis_key)
            queue_stats.append(QueueStats(queue_name=queue_name, count=count))
            total_count += count

        return [QueueStats(queue_name="total", count=total_count)] + queue_stats

    except Exception:
        return []
