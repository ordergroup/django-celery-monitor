import logging
from datetime import datetime

from celery import current_app

from celery_monitor.models import (
    DashboardStatusCount,
    RecentTasksData,
    TaskDetail,
    TaskExecutionStats,
    TasksPage,
    WorkerStats,
)
from celery_monitor.results_monitor.base import CeleryResultsMonitor

logger = logging.getLogger(__name__)


class WorkersCeleryResultsMonitor(CeleryResultsMonitor):
    """
    Limited implementation of CeleryResultsMonitor to return only information
    about online workers
    """

    def get_overall_status_counts(self) -> list[DashboardStatusCount]:
        return []

    def get_last_hour_status_counts(self) -> list[DashboardStatusCount]:
        return []

    def get_worker_stats(
        self, include_offline: bool | None = None
    ) -> list[WorkerStats]:
        try:
            inspect = current_app.control.inspect(timeout=0.5)

            # Get online workers using ping
            ping_response = inspect.ping()
            online_worker_names = set(ping_response.keys()) if ping_response else set()

            # Get active tasks - this shows currently executing tasks
            active_workers = inspect.active()

            # Get reserved (prefetched but not yet started) tasks
            reserved_workers = inspect.reserved()

            # Get active queues for each worker
            active_queues = inspect.active_queues()

            workers = []
            seen_workers = set()

            for worker_name in online_worker_names:
                if worker_name in seen_workers:
                    continue

                seen_workers.add(worker_name)

                # Get active tasks for this worker
                active_tasks_list = []
                if active_workers and worker_name in active_workers:
                    active_tasks_list = active_workers[worker_name]

                active_count = len(active_tasks_list) if active_tasks_list else 0

                reserved_count = 0
                if reserved_workers and worker_name in reserved_workers:
                    reserved_count = len(reserved_workers[worker_name])

                # Get queues for this worker
                queues = []
                if active_queues and worker_name in active_queues:
                    queues = [q["name"] for q in active_queues[worker_name]]

                workers.append(
                    WorkerStats(
                        name=worker_name,
                        status="online",
                        active_tasks=active_count,
                        pool_size=None,
                        max_concurrency=None,
                        queues=queues if queues else None,
                        reserved_tasks=reserved_count,
                    )
                )

            return sorted(workers, key=lambda w: (w.status != "online", w.name))

        except Exception as e:
            logger.exception("Error getting worker stats: %s", e)
            return []

    def get_task_execution_stats(
        self,
        sort_by: str = "total_count",
        sort_order: str = "desc",
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[TaskExecutionStats]:
        return []

    def get_recent_tasks(
        self,
        status: str | None = None,
        task_name: str | None = None,
        worker: str | None = None,
        limit: int = 50,
    ) -> RecentTasksData:
        return RecentTasksData(recent_tasks=[], task_names=[], workers=[])

    def get_task_detail(self, task_id: str) -> TaskDetail | None:
        try:
            inspect = current_app.control.inspect(timeout=1.0)
            reserved = inspect.reserved() or {}
            for worker_name, tasks in reserved.items():
                for t in tasks:
                    if t.get("id") == task_id:
                        return TaskDetail(
                            task_id=task_id,
                            task_name=t.get("name"),
                            status="RESERVED",
                            worker=t.get("hostname") or worker_name,
                            date_created=None,
                            date_started=None,
                            date_done=None,
                            task_args=t.get("args"),
                            task_kwargs=t.get("kwargs"),
                            result=None,
                            traceback=None,
                            periodic_task_name=None,
                            meta=None,
                            exception_type=None,
                            exception=None,
                        )
        except Exception as e:
            logger.exception("Error looking up reserved task detail: %s", e)
        return None

    def get_tasks(
        self,
        status: str | None = None,
        task_name: str | None = None,
        worker: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        page: int = 0,
        page_size: int = 50,
    ) -> TasksPage:
        return TasksPage(tasks=[], total=0, task_names=[], workers=[])
