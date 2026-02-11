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
    """Check if Redis is available and configured for Celery."""
    try:
        import redis
        from celery import current_app

        broker_url = current_app.conf.broker_url
        if not broker_url or not broker_url.startswith("redis://"):
            return False

        r = redis.from_url(broker_url, socket_connect_timeout=1)
        r.ping()
        return True
    except Exception:
        return False
