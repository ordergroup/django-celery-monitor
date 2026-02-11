from celery_monitor.queue_monitor.base import QueueMonitor
from celery_monitor.utils import has_redis


def get_queue_monitor() -> QueueMonitor:
    if has_redis():
        from celery_monitor.queue_monitor.redis import RedisMonitor

        return RedisMonitor()

    return QueueMonitor()
