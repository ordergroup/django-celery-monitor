import logging

from celery import current_app

from celery_monitor.models import (
    DashboardStatusCount,
    RecentTasksData,
    TaskDetail,
    TaskExecutionStats,
    WorkerStats,
)
from celery_monitor.utils import has_django_celery_result, is_postgres

logger = logging.getLogger(__name__)


class CeleryResultsMonitor:
    def __init__(self):
        self.is_postgres = is_postgres()
        self.has_django_celery_result = has_django_celery_result()

    def get_overall_status_counts(self) -> list[DashboardStatusCount]:
        return []

    def get_last_hour_status_counts(self) -> list[DashboardStatusCount]:
        return []

    def get_worker_stats(self) -> list[WorkerStats]:
        try:
            inspect = current_app.control.inspect(timeout=3.0)

            # Get online workers using ping
            ping_response = inspect.ping()
            online_worker_names = set(ping_response.keys()) if ping_response else set()

            # Get active tasks - this shows currently executing tasks
            active_workers = inspect.active()

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
                    )
                )

            return sorted(workers, key=lambda w: (w.status != "online", w.name))

        except Exception as e:
            logger.exception("Error getting worker stats: %s", e)
            return []

    def get_task_execution_stats(
        self,
        hours: int | None = 1,
        sort_by: str = "total_count",
        sort_order: str = "desc",
        date_from: str | None = None,
        date_to: str | None = None,
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
        return None
