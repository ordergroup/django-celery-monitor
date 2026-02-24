from celery import Celery
from django.conf import settings

from celery_monitor.signals_backend.base import SignalsResultBackend
from celery_monitor.signals_backend.noop import NoopSignalsResultBackend
from celery_monitor.signals_backend.redis import RedisSignalsResultBackend
from celery_monitor.utils import has_django_celery_result, has_redis


def get_signals_backend(app: Celery) -> SignalsResultBackend:
    force_redis = getattr(settings, "DJANGO_CELERY_MONITOR_FORCE_REDIS", False)
    if (has_django_celery_result() and has_redis() and force_redis) or has_redis():
        return RedisSignalsResultBackend(app)
    else:
        NoopSignalsResultBackend(app)
