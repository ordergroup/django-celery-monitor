from django.conf import settings

from celery_monitor.enums import BackendType
from celery_monitor.results_monitor.base import CeleryResultsMonitor
from celery_monitor.utils import has_django_celery_result, has_redis


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
    results_backend = BackendType.from_str(
        getattr(settings, "CELERY_MONITOR_RESULTS_BACKEND", "unknown")
    )

    if (
        results_backend in (BackendType.CELERY_RESULTS, BackendType.UNKNOWN)
        and has_django_celery_result()
    ):
        from celery_monitor.results_monitor.celery_results_mixin import (
            CeleryResultsMixin,
        )

        class EnhancedResultsMonitor(CeleryResultsMixin, CeleryResultsMonitor):
            pass

        return EnhancedResultsMonitor()

    elif results_backend in (BackendType.REDIS, BackendType.UNKNOWN) and has_redis():
        from celery_monitor.results_monitor.redis_custom_mixin import (
            RedisCustomResultsMixin,
        )

        class RedisResultsMonitor(RedisCustomResultsMixin, CeleryResultsMonitor):
            pass

        return RedisResultsMonitor()

    # Default: use base monitor (limited functionality)
    return CeleryResultsMonitor()
