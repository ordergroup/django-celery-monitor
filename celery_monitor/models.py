from dataclasses import dataclass

from django.db import models


class CeleryStatusCount(models.Model):
    status = models.CharField(max_length=50, primary_key=True)
    count = models.IntegerField()

    class Meta:
        managed = False
        db_table = "celery_status_counts"


@dataclass()
class QueueStats:
    queue_name: str
    count: int


@dataclass
class WorkerStats:
    name: str
    status: str
    active_tasks: int
    pool_size: int | None = None
    max_concurrency: int | None = None


@dataclass
class DashboardStatusCount:
    status: str
    count: int | None


@dataclass
class TaskExecutionStats:
    task_name: str
    total_count: int
    success_count: int
    failure_count: int
    avg_runtime: float | None


@dataclass
class QueueTaskTypeStats:
    queue_name: str
    task_name: str
    count: int


@dataclass
class RecentTask:
    task_id: str
    task_name: str | None
    status: str
    worker: str | None
    date_started: str | None
    date_done: str | None
    execution_time: float | None  # in seconds


@dataclass
class RecentTasksData:
    recent_tasks: list[RecentTask]
    task_names: list[str]
    workers: list[str]


