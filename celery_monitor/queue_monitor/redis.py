import json
import time
from collections import defaultdict

import redis
from celery import current_app
from redis import Redis

from celery_monitor.models import QueueStats, QueueTaskTypeStats
from celery_monitor.queue_monitor.base import QueueMonitor
from celery_monitor.redis_keys import REDIS_KEY_QUEUE_LEN_STREAM


class RedisMonitor(QueueMonitor):
    def __init__(self):
        super().__init__()
        self.redis: Redis = redis.from_url(self.broker_url)

    def _get_results_backend_connection(self) -> Redis:
        redis_url = current_app.conf.result_backend or current_app.conf.broker_url
        return redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=3,
        )

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

    def clear_queue(self, queue_name: str) -> None:
        self.redis.delete(queue_name)

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

    def queue_length_history(self) -> dict:
        queue_names = self.get_queue_names()

        now_ms = int(time.time() * 1000)
        start_ms = now_ms - 24 * 3600 * 1000

        try:
            r = self._get_results_backend_connection()
            pipeline = r.pipeline()
            for queue_name in queue_names:
                stream_key = REDIS_KEY_QUEUE_LEN_STREAM.format(queue_name=queue_name)
                pipeline.xrange(stream_key, min=start_ms, max=now_ms)
            all_entries = pipeline.execute()

            queues = {
                queue_name: [
                    {"x": int(entry_id.split("-")[0]), "y": int(fields["queue_len"])}
                    for entry_id, fields in entries
                ]
                for queue_name, entries in zip(queue_names, all_entries, strict=False)
            }

        except Exception:
            queues = {q: [] for q in queue_names}

        return queues
