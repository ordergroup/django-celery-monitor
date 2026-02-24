from celery import Celery
from django.conf import settings

from celery_monitor.enums import BackendType
from celery_monitor.signals_backend.base import SignalsResultBackend
from celery_monitor.signals_backend.noop import NoopSignalsResultBackend
from celery_monitor.signals_backend.redis import RedisSignalsResultBackend
from celery_monitor.utils import has_django_celery_result, has_redis


def get_signals_backend(app: Celery) -> SignalsResultBackend:
    """
    Factory function that returns the appropriate signals backend based on available backends.

    The backend selection is controlled by the CELERY_MONITOR_RESULTS_BACKEND setting,
    which can be set to "celery_results", "redis", or left unset for automatic detection.

    Priority order:
    1. Noop backend - when CELERY_MONITOR_RESULTS_BACKEND is "celery_results"
       or "unknown" and django-celery-results is installed (no signal handling needed)
    2. Redis backend - when CELERY_MONITOR_RESULTS_BACKEND is "redis" or "unknown" and Redis is configured
    3. Noop backend - fallback (no signal handling)

    Args:
        app: The Celery application instance

    Returns:
        SignalsResultBackend: The appropriate signals backend implementation

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
        return NoopSignalsResultBackend(app)

    elif results_backend in (BackendType.REDIS, BackendType.UNKNOWN) and has_redis():
        return RedisSignalsResultBackend(app)

    return NoopSignalsResultBackend(app)
