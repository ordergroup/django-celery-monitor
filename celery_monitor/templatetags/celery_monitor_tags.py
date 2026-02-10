from django import template

from celery_monitor.utils import has_django_celery_result, has_redis

register = template.Library()


@register.simple_tag
def has_celery_results():
    """Check if django-celery-results is installed."""
    return has_django_celery_result()


@register.simple_tag
def has_redis_broker():
    """Check if Redis is available and configured as Celery broker."""
    return has_redis()
