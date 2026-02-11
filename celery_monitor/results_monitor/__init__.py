from celery_monitor.results_monitor.base import CeleryResultsMonitor
from celery_monitor.utils import has_django_celery_result


def get_results_monitor() -> CeleryResultsMonitor:
    if has_django_celery_result():
        from celery_monitor.results_monitor.celery_results_mixin import (
            CeleryResultsMixin,
        )

        class EnhancedResultsMonitor(CeleryResultsMixin, CeleryResultsMonitor):
            pass

        return EnhancedResultsMonitor()

    return CeleryResultsMonitor()
