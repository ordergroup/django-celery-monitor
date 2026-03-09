import logging
from abc import ABC, abstractmethod
from datetime import datetime

from celery_monitor.models import (
    DashboardStatusCount,
    RecentTasksData,
    TaskDetail,
    TaskExecutionStats,
    TasksPage,
    WorkerStats,
)

logger = logging.getLogger(__name__)


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
        worker: str | None = None,
        limit: int = 50,
    ) -> RecentTasksData: ...

    @abstractmethod
    def get_task_detail(self, task_id: str) -> TaskDetail | None: ...

    @abstractmethod
    def get_tasks(
        self,
        status: str | None = None,
        task_name: str | None = None,
        worker: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        page: int = 0,
        page_size: int = 50,
    ) -> TasksPage: ...
