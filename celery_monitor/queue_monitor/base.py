from abc import ABC

from celery import current_app

from celery_monitor.models import QueueStats, QueueTaskTypeStats


class QueueMonitor(ABC):
    def __init__(self):
        self.broker_url = current_app.conf.broker_url

    def get_queue_task_types(self) -> list[QueueTaskTypeStats]:
        return []

    def get_queue_stats(self) -> list[QueueStats]:
        return []

    def get_queue_names(self) -> list[str]:
        task_queues = current_app.conf.task_queues

        if isinstance(task_queues, dict):
            queue_names = list(task_queues.keys())
        elif task_queues:
            queue_names = [q.name for q in task_queues]
        else:
            queue_names = [current_app.conf.task_default_queue or "celery"]

        return queue_names
