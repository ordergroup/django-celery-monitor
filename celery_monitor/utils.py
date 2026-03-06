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
