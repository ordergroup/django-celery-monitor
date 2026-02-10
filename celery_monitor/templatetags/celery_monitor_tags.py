from django import template

from celery_monitor.utils import has_django_celery_result

register = template.Library()


@register.simple_tag
def has_celery_results():
    """Check if django-celery-results is installed."""
    return has_django_celery_result()
