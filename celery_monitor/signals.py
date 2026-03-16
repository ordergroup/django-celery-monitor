import logging

from celery import signals

from celery_monitor.signals_backend import get_signals_backend

logger = logging.getLogger(__name__)


@signals.after_task_publish.connect
def task_published_handler(
    sender=None, headers=None, body=None, routing_key=None, **kwargs
):
    from celery import current_app

    try:
        get_signals_backend(current_app).task_published_handler(
            sender, headers, body, routing_key, **kwargs
        )
    except Exception:
        logger.exception("celery_monitor: error in task_published_handler")


@signals.task_prerun.connect
def task_prerun_handler(
    sender=None, task_id=None, task=None, args=None, kwargs=None, **kw
):
    from celery import current_app

    try:
        get_signals_backend(current_app).task_prerun_handler(
            sender, task_id, task, args, kwargs, **kw
        )
    except Exception:
        logger.exception(
            "celery_monitor: error in task_prerun_handler (task_id=%s)", task_id
        )


@signals.task_postrun.connect
def task_postrun_handler(
    sender=None, task_id=None, task=None, state=None, retval=None, **kwargs
):
    from celery import current_app

    try:
        get_signals_backend(current_app).task_postrun_handler(
            sender, task_id, task, state, retval, **kwargs
        )
    except Exception:
        logger.exception(
            "celery_monitor: error in task_postrun_handler (task_id=%s)", task_id
        )


@signals.task_failure.connect
def task_failure_handler(sender=None, task_id=None, exception=None, **kwargs):
    from celery import current_app

    try:
        get_signals_backend(current_app).task_failure_handler(
            sender, task_id, exception, **kwargs
        )
    except Exception:
        logger.exception(
            "celery_monitor: error in task_failure_handler (task_id=%s)", task_id
        )


@signals.task_retry.connect
def task_retry_handler(sender=None, task_id=None, reason=None, **kwargs):
    from celery import current_app

    try:
        get_signals_backend(current_app).task_retry_handler(
            sender, task_id, reason, **kwargs
        )
    except Exception:
        logger.exception(
            "celery_monitor: error in task_retry_handler (task_id=%s)", task_id
        )


@signals.task_revoked.connect
def task_revoked_handler(sender=None, request=None, terminated=None, **kwargs):
    from celery import current_app

    try:
        get_signals_backend(current_app).task_revoked_handler(
            sender, request, terminated, **kwargs
        )
    except Exception:
        logger.exception("celery_monitor: error in task_revoked_handler")
