from datetime import timedelta

from django.db.models import Avg, Count, F, Q
from django.utils import timezone
from django_celery_results.models import TaskResult

from celery_monitor.models import (
    CeleryStatusCount,
    DashboardStatusCount,
    TaskExecutionStats,
    WorkerStats,
)


class CeleryResultsMixin:
    def get_overall_status_counts(self) -> list[DashboardStatusCount]:
        if self.is_postgres and self.has_django_celery_result:
            status_counts = CeleryStatusCount.objects.all().order_by("status")
            stats = [
                DashboardStatusCount(row.status, row.count) for row in status_counts
            ]
        elif not self.is_postgres and self.has_django_celery_result:
            from django_celery_results.models import TaskResult

            status_counts = (
                TaskResult.objects.values("status")
                .annotate(count=Count("id"))
                .order_by("status")
            )
            stats = [
                DashboardStatusCount(row["status"], row["count"])
                for row in status_counts
            ]
        else:
            stats = []

        return [DashboardStatusCount("total", sum(i.count for i in stats)), *stats]

    def get_last_hour_status_counts(self) -> list[DashboardStatusCount]:
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
        return [DashboardStatusCount("total", sum(i.count for i in stats)), *stats]

    def get_worker_stats(self) -> list[WorkerStats]:
        workers = super().get_worker_stats()
        worker_names = {worker.name for worker in workers}
        try:
            recent_workers = set(
                TaskResult.objects.exclude(worker__isnull=True)
                .values_list("worker", flat=True)
                .distinct()
            )

            for worker_name in recent_workers:
                if worker_name not in worker_names:
                    workers.append(
                        WorkerStats(
                            name=worker_name,
                            status="offline",
                            active_tasks=0,
                        )
                    )
                    worker_names.add(worker_name)

            return sorted(workers, key=lambda w: (w.status != "online", w.name))

        except Exception:
            return workers

    def get_task_execution_stats(
        self,
        hours: int | None = 1,
        sort_by: str = "total_count",
        sort_order: str = "desc",
    ) -> list[TaskExecutionStats]:
        try:
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
