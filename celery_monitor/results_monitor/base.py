import logging
from abc import ABC, abstractmethod
from datetime import datetime

from celery_monitor.models import (
    DashboardStatusCount,
    TaskDetail,
    TaskExecutionStats,
    TaskOverview,
    TasksPage,
    TaskTypeTimeSeries,
    WorkerStats,
)

logger = logging.getLogger("celery_monitor")


class CeleryResultsMonitor(ABC):
    @abstractmethod
    def get_overall_status_counts(self) -> list[DashboardStatusCount]: ...

    @abstractmethod
    def get_last_hour_status_counts(self) -> list[DashboardStatusCount]: ...

    @abstractmethod
    def get_worker_stats(self, include_offline: bool = False) -> list[WorkerStats]: ...

    @abstractmethod
    def get_task_execution_stats(
        self,
        sort_by: str = "total_count",
        sort_order: str = "desc",
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[TaskExecutionStats]: ...

    @abstractmethod
    def get_recent_tasks(
        self,
        status: str | None = None,
        task_name: str | None = None,
        queue_name: str | None = None,
        worker: str | None = None,
        limit: int = 50,
    ) -> list[TaskOverview]: ...

    @abstractmethod
    def get_task_detail(self, task_id: str) -> TaskDetail | None: ...

    @abstractmethod
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
    ) -> TasksPage: ...

    @abstractmethod
    def get_task_type_time_series(
        self,
        task_name: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[TaskTypeTimeSeries]: ...

    @abstractmethod
    def get_throughput_time_series(
        self,
        task_name: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> tuple[list[datetime], list[datetime]]:
        """Returns (queued_timestamps, started_timestamps) — sorted lists of individual event times for a specific task type."""
        ...

    @abstractmethod
    def get_queue_time_series(
        self,
        queue_name: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[TaskTypeTimeSeries]: ...

    @abstractmethod
    def get_queue_throughput_time_series(
        self,
        queue_name: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> tuple[list[datetime], list[datetime]]:
        """Returns (queued_timestamps, started_timestamps) for all tasks in the given queue."""
        ...

    @abstractmethod
    def get_tasks_names(self) -> list[str]: ...

    @abstractmethod
    def get_workers_names(self) -> list[str]: ...
