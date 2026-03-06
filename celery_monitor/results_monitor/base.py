import logging
from abc import ABC, abstractmethod

from celery_monitor.models import (
    DashboardStatusCount,
    RecentTasksData,
    TaskDetail,
    TaskExecutionStats,
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
        hours: int | None = 1,
        sort_by: str = "total_count",
        sort_order: str = "desc",
        date_from: str | None = None,
        date_to: str | None = None,
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
