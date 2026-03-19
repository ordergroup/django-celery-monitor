from typing import Any

from celery import current_app
from celery.app.control import Inspect
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


def float_or(v: str | None, default: Any | None = None) -> float | Any | None:
    return float(v) if v else default


def create_active_targeted_inspect() -> tuple[Inspect, set[str]] | tuple[None, None]:
    inspect = current_app.control.inspect(timeout=1.0)

    # Get online workers using ping
    ping_response = inspect.ping()
    online_worker_names = set(ping_response.keys()) if ping_response else set()

    if not online_worker_names:
        return None, None

    # Target only discovered workers for subsequent calls to avoid
    # re-broadcasting to all workers and missing responses due to timing
    return current_app.control.inspect(
        destination=list(online_worker_names), timeout=1.0
    ), online_worker_names
