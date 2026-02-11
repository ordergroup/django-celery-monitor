import json
from collections import defaultdict

import redis

from celery_monitor.models import QueueStats, QueueTaskTypeStats
from celery_monitor.queue_monitor.base import QueueMonitor


class RedisMonitor(QueueMonitor):
    def __init__(self):
        super().__init__()
        self.redis = redis.from_url(self.broker_url)

    def get_queue_task_types(self) -> list[QueueTaskTypeStats]:
        stats = []
        for queue_name in self.get_queue_names():
            task_types = self._count_tasks_in_queue(queue_name)
            stats.extend(
                QueueTaskTypeStats(
                    queue_name=queue_name, task_name=task_name, count=count
                )
                for task_name, count in task_types.items()
            )
        return stats

    def get_queue_stats(self) -> list[QueueStats]:
        try:
            queue_stats = []
            total_count = 0

            for queue_name in self.get_queue_names():
                count = self.redis.llen(queue_name)
                queue_stats.append(QueueStats(queue_name=queue_name, count=count))
                total_count += count

            queue_stats = sorted(queue_stats, key=lambda q: q.queue_name)
            return [QueueStats(queue_name="total", count=total_count), *queue_stats]

        except Exception:
            return []

    def _count_tasks_in_queue(self, queue_name: str) -> dict[str, int]:
        task_types = defaultdict(int)
        messages = self.redis.lrange(queue_name, 0, -1)

        for msg in messages:
            task_name = self._extract_task_name(msg)
            if not task_name:
                continue

            task_types[task_name] += 1

        return task_types

    def _extract_task_name(self, msg: bytes) -> str | None:
        try:
            decoded = json.loads(msg)
            headers = decoded.get("headers", {})
            return headers.get("task") or decoded.get("task", "unknown")
        except (json.JSONDecodeError, KeyError):
            return None
