import logging
from abc import ABC, abstractmethod
from functools import wraps

from celery import Celery

logger = logging.getLogger(__name__)


def safe_signal_handler(func):
    """
    Decorator to catch exceptions in signal handlers.

    Ensures that monitoring failures don't affect task execution.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning("Error in %s: %s", func.__name__, e)

    return wrapper


class SignalsResultBackend(ABC):
    def __init__(self, app: Celery):
        self.app = app

    @abstractmethod
    @safe_signal_handler
    def task_prerun_handler(
        self, sender=None, task_id=None, task=None, args=None, kwargs=None, **kw
    ): ...

    @abstractmethod
    @safe_signal_handler
    def task_postrun_handler(
        self, sender=None, task_id=None, task=None, state=None, retval=None, **kwargs
    ): ...

    @abstractmethod
    @safe_signal_handler
    def task_failure_handler(
        self, sender=None, task_id=None, exception=None, **kwargs
    ): ...

    @abstractmethod
    @safe_signal_handler
    def task_retry_handler(self, sender=None, task_id=None, reason=None, **kwargs): ...

    @abstractmethod
    @safe_signal_handler
    def task_revoked_handler(
        self, sender=None, request=None, terminated=None, **kwargs
    ): ...
