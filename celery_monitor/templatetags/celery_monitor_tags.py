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


@register.filter
def format_duration(seconds):
    """Format duration in seconds to human readable format."""
    if seconds is None:
        return "-"
    
    if seconds < 1:
        return f"{seconds:.3f}s"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"
