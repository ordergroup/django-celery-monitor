import logging
from datetime import datetime, timedelta

from django.core.cache import cache
from django.db.models import Avg, Count, F, Max, Min, Q
from django.db.models.functions import TruncDay, TruncHour
from django.utils import timezone
from django_celery_results.models import TaskResult

from celery_monitor.models import (
    CeleryStatusCount,
    DashboardStatusCount,
    TaskDetail,
    TaskExecutionStats,
    TaskOverview,
    TasksPage,
    TaskTypeTimeSeries,
    ThroughputBucket,
    WorkerStats,
)
from celery_monitor.results_monitor.base import CeleryResultsMonitor
from celery_monitor.results_monitor.workers_results import WorkersCeleryResultsMonitor
from celery_monitor.utils import is_postgres

logger = logging.getLogger("celery_monitor")


class DjangoCeleryResultsMonitor(CeleryResultsMonitor):
    def __init__(self):
        self.is_postgres = is_postgres()
        self.workers_monitor = WorkersCeleryResultsMonitor()

    def get_overall_status_counts(self) -> list[DashboardStatusCount]:
        if self.is_postgres:
            status_counts = CeleryStatusCount.objects.all().order_by("status")
            stats = [
                DashboardStatusCount(row.status, row.count) for row in status_counts
            ]
        else:
            status_counts = (
                TaskResult.objects.values("status")
                .annotate(count=Count("id"))
                .order_by("status")
            )
            stats = [
                DashboardStatusCount(row["status"], row["count"])
                for row in status_counts
            ]

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

    def get_worker_stats(self, include_offline: bool = False) -> list[WorkerStats]:
        workers = self.workers_monitor.get_worker_stats(include_offline)
        worker_names = {worker.name for worker in workers}
        try:
            day_ago = timezone.now() - timedelta(days=1)
            recent_workers = set(
                TaskResult.objects.filter(date_created__gte=day_ago)
                .exclude(worker__isnull=True)
                .values_list("worker", flat=True)
                .distinct()
            )

            for worker_name in recent_workers:
                if include_offline and worker_name not in worker_names:
                    workers.append(
                        WorkerStats(
                            name=worker_name,
                            status="offline",
                            active_tasks=0,
                        )
                    )
                    worker_names.add(worker_name)

            return sorted(workers, key=lambda w: (w.status != "online", w.name))

        except Exception as e:
            logger.exception("Error getting worker stats from celery results: %s", e)
            return workers

    def get_task_execution_stats(
        self,
        sort_by: str = "total_count",
        sort_order: str = "desc",
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[TaskExecutionStats]:
        try:
            queryset = TaskResult.objects.all()

            if date_from:
                if timezone.is_naive(date_from):
                    date_from = timezone.make_aware(date_from)
                queryset = queryset.filter(date_done__gte=date_from)
            if date_to:
                if timezone.is_naive(date_to):
                    date_to = timezone.make_aware(date_to)
                queryset = queryset.filter(date_done__lte=date_to)

            nullable_fields = {
                "avg_runtime",
                "min_runtime",
                "max_runtime",
                "avg_wait",
                "min_wait",
                "max_wait",
            }
            if sort_by in nullable_fields:
                if sort_order == "desc":
                    order_expr = F(sort_by).desc(nulls_last=True)
                else:
                    order_expr = F(sort_by).asc(nulls_first=True)
            else:
                order_expr = f"-{sort_by}" if sort_order == "desc" else sort_by

            stats = (
                queryset.values("task_name")
                .annotate(
                    total_count=Count("id"),
                    success_count=Count("id", filter=Q(status="SUCCESS")),
                    failure_count=Count("id", filter=Q(status="FAILURE")),
                    queued_count=Count(
                        "id", filter=Q(status__in=("QUEUED", "PENDING"))
                    ),
                    started_count=Count("id", filter=Q(status="STARTED")),
                    avg_runtime=Avg(
                        F("date_done") - F("date_started"),
                        filter=Q(
                            status="SUCCESS",
                            date_started__isnull=False,
                            date_done__isnull=False,
                        ),
                    ),
                    min_runtime=Min(
                        F("date_done") - F("date_started"),
                        filter=Q(
                            status="SUCCESS",
                            date_started__isnull=False,
                            date_done__isnull=False,
                        ),
                    ),
                    max_runtime=Max(
                        F("date_done") - F("date_started"),
                        filter=Q(
                            status="SUCCESS",
                            date_started__isnull=False,
                            date_done__isnull=False,
                        ),
                    ),
                    avg_wait=Avg(
                        F("date_started") - F("date_created"),
                        filter=Q(
                            date_started__isnull=False,
                            date_created__isnull=False,
                        ),
                    ),
                    min_wait=Min(
                        F("date_started") - F("date_created"),
                        filter=Q(
                            date_started__isnull=False,
                            date_created__isnull=False,
                        ),
                    ),
                    max_wait=Max(
                        F("date_started") - F("date_created"),
                        filter=Q(
                            date_started__isnull=False,
                            date_created__isnull=False,
                        ),
                    ),
                )
                .order_by(order_expr)
            )

            result = []
            for stat in stats:
                avg_seconds = None
                if stat["avg_runtime"]:
                    avg_seconds = stat["avg_runtime"].total_seconds()

                min_seconds = None
                if stat["min_runtime"]:
                    min_seconds = stat["min_runtime"].total_seconds()

                max_seconds = None
                if stat["max_runtime"]:
                    max_seconds = stat["max_runtime"].total_seconds()

                avg_wait_seconds = None
                if stat["avg_wait"]:
                    avg_wait_seconds = stat["avg_wait"].total_seconds()

                min_wait_seconds = None
                if stat["min_wait"]:
                    min_wait_seconds = stat["min_wait"].total_seconds()

                max_wait_seconds = None
                if stat["max_wait"]:
                    max_wait_seconds = stat["max_wait"].total_seconds()

                result.append(
                    TaskExecutionStats(
                        task_name=stat["task_name"],
                        total_count=stat["total_count"],
                        success_count=stat["success_count"],
                        failure_count=stat["failure_count"],
                        queued_count=stat["queued_count"],
                        started_count=stat["started_count"],
                        avg_runtime=avg_seconds,
                        min_runtime=min_seconds,
                        max_runtime=max_seconds,
                        avg_wait=avg_wait_seconds,
                        min_wait=min_wait_seconds,
                        max_wait=max_wait_seconds,
                    )
                )

            return result

        except Exception as e:
            logger.exception("Error getting task execution stats: %s", e)
            return []

    def get_recent_tasks(
        self,
        status: str | None = None,
        task_name: str | None = None,
        queue_name: str | None = None,
        worker: str | None = None,
        limit: int = 50,
    ) -> list[TaskOverview]:
        try:
            qs = TaskResult.objects.all()

            if status:
                qs = qs.filter(status=status)
            if task_name:
                qs = qs.filter(task_name=task_name)
            if worker:
                qs = qs.filter(worker=worker)

            recent_tasks_qs = qs.order_by("-date_done")[:limit]

            recent_tasks = []
            for task in recent_tasks_qs:
                execution_time = None
                if (
                    task.date_started
                    and task.date_done
                    and task.date_done >= task.date_started
                ):
                    execution_time = (
                        task.date_done - task.date_started
                    ).total_seconds()

                recent_tasks.append(
                    TaskOverview(
                        task_id=task.task_id,
                        task_name=task.task_name,
                        status=task.status,
                        worker=task.worker,
                        date_started=task.date_started,
                        date_done=task.date_done,
                        execution_time=execution_time,
                        queue_name="unknown",  # TODO: we probably need a signals similar to redis to get the queue_name
                    )
                )

            if queue_name:
                # TODO: find a way to filter by queue_name
                pass

            return recent_tasks

        except Exception as e:
            logger.exception("Error getting recent tasks: %s", e)
            return []

    def get_task_detail(self, task_id: str) -> TaskDetail | None:
        """Get detailed information about a specific task from django-celery-results."""

        try:
            task_result = TaskResult.objects.get(task_id=task_id)
            return TaskDetail(
                task_id=task_result.task_id,
                task_name=task_result.task_name,
                status=task_result.status,
                worker=task_result.worker,
                date_started=task_result.date_started,
                date_created=task_result.date_created,
                date_done=task_result.date_done,
                task_args=task_result.task_args,
                task_kwargs=task_result.task_kwargs,
                result=task_result.result,
                traceback=task_result.traceback,
                meta=task_result.meta,
                periodic_task_name=None,
                exception=None,
                exception_type=None,
                queue_name="unknown",  # TODO: we probably need a signals similar to redis to get the queue_name
            )

        except TaskResult.DoesNotExist:
            return self.workers_monitor.get_task_detail(task_id)
        except Exception as e:
            logger.exception("Error getting task detail: %s", e)
            return None

    def get_tasks(
        self,
        status: str | None = None,
        task_name: str | None = None,
        queue_name: str | None = None,
        worker: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        page: int = 0,
        page_size: int = 50,
    ) -> TasksPage:
        try:
            qs = TaskResult.objects.all()

            if status:
                qs = qs.filter(status=status)
            if task_name:
                qs = qs.filter(task_name=task_name)
            if worker:
                qs = qs.filter(worker=worker)
            if date_from:
                qs = qs.filter(date_done__gte=date_from)
            if date_to:
                qs = qs.filter(date_done__lte=date_to)

            total = qs.count()
            offset = page * page_size
            page_qs = qs.order_by("-date_done")[offset : offset + page_size]

            tasks = []
            for task in page_qs:
                execution_time = None
                if (
                    task.date_started
                    and task.date_done
                    and task.date_done >= task.date_started
                ):
                    execution_time = (
                        task.date_done - task.date_started
                    ).total_seconds()
                tasks.append(
                    TaskOverview(
                        task_id=task.task_id,
                        task_name=task.task_name,
                        status=task.status,
                        worker=task.worker,
                        date_started=task.date_started,
                        date_done=task.date_done,
                        execution_time=execution_time,
                        queue_name=None,
                    )
                )

            return TasksPage(tasks=tasks, total=total)

        except Exception as e:
            logger.exception("Error getting tasks: %s", e)
            return TasksPage(tasks=[], total=0)

    def get_task_type_time_series(
        self,
        task_name: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[TaskTypeTimeSeries]:
        try:
            queryset = TaskResult.objects.filter(task_name=task_name)

            if date_from:
                if timezone.is_naive(date_from):
                    date_from = timezone.make_aware(date_from)
                queryset = queryset.filter(date_done__gte=date_from)
            if date_to:
                if timezone.is_naive(date_to):
                    date_to = timezone.make_aware(date_to)
                queryset = queryset.filter(date_done__lte=date_to)

            if date_from and date_to:
                range_hours = (date_to - date_from).total_seconds() / 3600
            else:
                range_hours = 24 * 30

            trunc_fn = TruncHour if range_hours <= 48 else TruncDay

            stats = (
                queryset.annotate(bucket=trunc_fn("date_done"))
                .values("bucket")
                .annotate(
                    count=Count("id"),
                    success_count=Count("id", filter=Q(status="SUCCESS")),
                    failure_count=Count("id", filter=Q(status="FAILURE")),
                    avg_runtime=Avg(
                        F("date_done") - F("date_started"),
                        filter=Q(date_started__isnull=False, date_done__isnull=False),
                    ),
                    min_runtime=Min(
                        F("date_done") - F("date_started"),
                        filter=Q(date_started__isnull=False, date_done__isnull=False),
                    ),
                    max_runtime=Max(
                        F("date_done") - F("date_started"),
                        filter=Q(date_started__isnull=False, date_done__isnull=False),
                    ),
                    avg_wait=Avg(
                        F("date_started") - F("date_created"),
                        filter=Q(
                            date_started__isnull=False, date_created__isnull=False
                        ),
                    ),
                    min_wait=Min(
                        F("date_started") - F("date_created"),
                        filter=Q(
                            date_started__isnull=False, date_created__isnull=False
                        ),
                    ),
                    max_wait=Max(
                        F("date_started") - F("date_created"),
                        filter=Q(
                            date_started__isnull=False, date_created__isnull=False
                        ),
                    ),
                )
                .order_by("bucket")
            )

            return [
                TaskTypeTimeSeries(
                    bucket=stat["bucket"],
                    count=stat["count"],
                    success_count=stat["success_count"],
                    failure_count=stat["failure_count"],
                    avg_runtime=stat["avg_runtime"].total_seconds()
                    if stat["avg_runtime"]
                    else None,
                    min_runtime=stat["min_runtime"].total_seconds()
                    if stat["min_runtime"]
                    else None,
                    max_runtime=stat["max_runtime"].total_seconds()
                    if stat["max_runtime"]
                    else None,
                    avg_wait=stat["avg_wait"].total_seconds()
                    if stat["avg_wait"]
                    else None,
                    min_wait=stat["min_wait"].total_seconds()
                    if stat["min_wait"]
                    else None,
                    max_wait=stat["max_wait"].total_seconds()
                    if stat["max_wait"]
                    else None,
                )
                for stat in stats
            ]

        except Exception as e:
            logger.exception("Error getting task type time series: %s", e)
            return []

    def get_throughput_time_series(
        self,
        task_name: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[ThroughputBucket]:
        return []

    def get_queue_time_series(
        self,
        queue_name: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[TaskTypeTimeSeries]:
        # queue_name is not stored in django-celery-results TaskResult
        return []

    def get_queue_throughput_time_series(
        self,
        queue_name: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[ThroughputBucket]:
        return []

    def get_tasks_names(self) -> list[str]:
        return cache.get_or_set(
            "celery_monitor:task_names",
            lambda: list(
                TaskResult.objects.exclude(task_name__isnull=True)
                .values_list("task_name", flat=True)
                .distinct()
                .order_by("task_name")
            ),
            timeout=60,
        )

    def get_workers_names(self) -> list[str]:
        return cache.get_or_set(
            "celery_monitor:worker_names",
            lambda: list(
                TaskResult.objects.exclude(worker__isnull=True)
                .values_list("worker", flat=True)
                .distinct()
                .order_by("worker")
            ),
            timeout=60,
        )
