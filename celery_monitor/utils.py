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
    - Redis package is installed
    - Celery broker_url or result_backend is configured with Redis
    - Connection to Redis can be established
    """
    broker_url = current_app.conf.broker_url
    result_backend = current_app.conf.result_backend

    redis_url = result_backend or broker_url
    if not redis_url:
        return False

    return redis_url.startswith("redis://") or redis_url.startswith("rediss://")
