from dataclasses import dataclass
from datetime import datetime
from typing import Any

from django.db import models


class CeleryStatusCount(models.Model):
    status = models.CharField(max_length=50, primary_key=True)
    count = models.IntegerField()

    class Meta:
        managed = False
        db_table = "celery_status_counts"

    def __str__(self) -> str:
        return f"{self.status}: {self.count}"


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
    queues: list[str] | None = None
    reserved_tasks: int = 0


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
    min_runtime: float | None
    max_runtime: float | None


@dataclass
class QueueTaskTypeStats:
    queue_name: str
    task_name: str
    count: int


@dataclass
class TaskOverview:
    task_id: str
    task_name: str | None
    status: str
    worker: str | None
    date_started: datetime | None
    date_done: datetime | None
    execution_time: float | None


@dataclass
class RecentTasksData:
    recent_tasks: list[TaskOverview]
    task_names: list[str]
    workers: list[str]


@dataclass
class TasksPage:
    tasks: list[TaskOverview]
    total: int
    task_names: list[str]
    workers: list[str]


@dataclass
class TaskDetail:
    task_id: str
    task_name: str | None
    status: str
    worker: str | None
    date_created: str | None
    date_started: str | None
    date_done: str | None
    task_args: list | None
    task_kwargs: dict | None
    result: dict | None
    traceback: str | None
    periodic_task_name: str | None
    meta: Any | None
    exception_type: str | None
    exception: str | None
