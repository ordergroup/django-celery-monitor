from datetime import timedelta
from celery_monitor.models import CeleryStatusCount
from django.utils import timezone
from django.db.models import Count
from dataclasses import dataclass
from django_celery_results.models import TaskResult
from celery_monitor.utils import is_postgres, has_django_celery_result


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
