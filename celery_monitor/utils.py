from celery import current_app
from django.db import connection


def is_postgres() -> bool:
    return connection.vendor == "postgresql"


def has_django_celery_result() -> bool:
    try:
        import django_celery_results  # noqa
    except ImportError:
        return False

    return True


def has_redis() -> bool:
    """
    Check if Redis is available and configured for Celery.

    Returns True if:
    - Celery broker_url is configured with Redis
    """
    redis_url = current_app.conf.broker_url

    if not redis_url:
        return False

    return redis_url.startswith("redis://") or redis_url.startswith("rediss://")


def is_redis_backend() -> bool:
    """
    Return True when the active results monitor is the Redis backend.

    Mirrors the selection logic in get_results_monitor(): Redis is used only
    when django-celery-results is NOT taking priority.
    """
    from django.conf import settings

    from celery_monitor.enums import BackendType

    results_backend = BackendType.from_str(
        getattr(settings, "CELERY_MONITOR_RESULTS_BACKEND", "unknown")
    )

    if (
        results_backend in (BackendType.CELERY_RESULTS, BackendType.UNKNOWN)
        and has_django_celery_result()
    ):
        return False

    return results_backend in (BackendType.REDIS, BackendType.UNKNOWN) and has_redis()
