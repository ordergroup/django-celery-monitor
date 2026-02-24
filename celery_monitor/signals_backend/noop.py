import logging

from celery_monitor.signals_backend.base import SignalsResultBackend

logger = logging.getLogger(__name__)


class NoopSignalsResultBackend(SignalsResultBackend):
    def task_prerun_handler(
        self, sender=None, task_id=None, task=None, args=None, kwargs=None, **kw
    ):
        pass

    def task_postrun_handler(
        self, sender=None, task_id=None, task=None, state=None, retval=None, **kwargs
    ):
        pass

    def task_failure_handler(self, sender=None, task_id=None, exception=None, **kwargs):
        pass

    def task_retry_handler(self, sender=None, task_id=None, reason=None, **kwargs):
        pass

    def task_revoked_handler(
        self, sender=None, request=None, terminated=None, **kwargs
    ):
        pass
