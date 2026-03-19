import time

from django.conf import settings

from celery_monitor.enums import BackendType
from celery_monitor.results_monitor.base import CeleryResultsMonitor
from celery_monitor.results_monitor.workers_results import WorkersCeleryResultsMonitor
from celery_monitor.utils import has_django_celery_result, has_redis

_MONITOR_CLASS_CACHE_TTL = 30  # seconds
_cached_monitor_class: type[CeleryResultsMonitor] | None = None
_cache_expires_at: float = 0.0


def get_results_monitor() -> CeleryResultsMonitor:
    """
    Factory function that returns the appropriate results monitor based on available backends.

    The backend selection is controlled by the CELERY_MONITOR_RESULTS_BACKEND setting,
    which can be set to "celery_results", "redis", or left unset for automatic detection.

    Priority order:
    1. Django Celery Results - when CELERY_MONITOR_RESULTS_BACKEND is "celery_results"
       or "unknown" and django-celery-results is installed
    2. Redis - when CELERY_MONITOR_RESULTS_BACKEND is "redis" or "unknown" and Redis is configured
    3. Base monitor - fallback with limited functionality (only live worker stats)

    Returns:
        CeleryResultsMonitor: The appropriate monitor implementation based on available backends

    Settings:
        CELERY_MONITOR_RESULTS_BACKEND: str - Backend type ("celery_results", "redis", or unset)
    """
    global _cached_monitor_class, _cache_expires_at

    now = time.monotonic()
    if _cached_monitor_class is None or now >= _cache_expires_at:
        _cached_monitor_class = _resolve_monitor_class()
        _cache_expires_at = now + _MONITOR_CLASS_CACHE_TTL

    return _cached_monitor_class()


def _resolve_monitor_class() -> type[CeleryResultsMonitor]:
    results_backend = BackendType.from_str(
        getattr(settings, "CELERY_MONITOR_RESULTS_BACKEND", "unknown")
    )

    if (
        results_backend in (BackendType.CELERY_RESULTS, BackendType.UNKNOWN)
        and has_django_celery_result()
    ):
        from celery_monitor.results_monitor.django_celery_results import (
            DjangoCeleryResultsMonitor,
        )

        return DjangoCeleryResultsMonitor

    if results_backend in (BackendType.REDIS, BackendType.UNKNOWN) and has_redis():
        from celery_monitor.redis.client import get_results_client
        from celery_monitor.redis.keys import REDIS_KEY_LAST_CALCULATION_TIMESTAMP
        from celery_monitor.results_monitor.redis_computed_results import (
            RedisComputedResultsMonitor,
        )
        from celery_monitor.results_monitor.redis_results import (
            RedisResultsMonitor,
        )

        redis = get_results_client()
        has_calculated_results = bool(redis.get(REDIS_KEY_LAST_CALCULATION_TIMESTAMP))

        return (
            RedisComputedResultsMonitor
            if has_calculated_results
            else RedisResultsMonitor
        )

    return WorkersCeleryResultsMonitor
