from django.conf import settings

from celery_monitor.results_monitor.base import CeleryResultsMonitor
from celery_monitor.utils import has_django_celery_result, has_redis


def get_results_monitor() -> CeleryResultsMonitor:
    """
    Factory function that returns the appropriate results monitor based on available backends.

    Priority order:
    1. Redis - when DJANGO_CELERY_MONITOR_FORCE_REDIS is set to True
    2. Django Celery Results (if installed) - uses database backend
    3. Redis (if configured)
    4. Base monitor - limited functionality (only live worker stats)

    For Redis monitoring to work, you must set up the signal handlers in your Celery app:
        from celery_monitor.signals import setup_celery_monitor_signals
        setup_celery_monitor_signals(app)
    """
    force_redis = getattr(settings, "DJANGO_CELERY_MONITOR_FORCE_REDIS", False)

    # Use Redis if forced or as fallback
    if (force_redis or not has_django_celery_result()) and has_redis():
        from celery_monitor.results_monitor.redis_custom_mixin import (
            RedisCustomResultsMixin,
        )

        class RedisResultsMonitor(RedisCustomResultsMixin, CeleryResultsMonitor):
            pass

        return RedisResultsMonitor()

    # Prefer django-celery-results if available
    if has_django_celery_result():
        from celery_monitor.results_monitor.celery_results_mixin import (
            CeleryResultsMixin,
        )

        class EnhancedResultsMonitor(CeleryResultsMixin, CeleryResultsMonitor):
            pass

        return EnhancedResultsMonitor()

    # Default: use base monitor (limited functionality)
    return CeleryResultsMonitor()
