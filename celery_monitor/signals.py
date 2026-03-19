import logging

from celery import signals

from celery_monitor.signals_backend import get_signals_backend

logger = logging.getLogger("celery_monitor")


@signals.after_task_publish.connect
def task_published_handler(
    sender=None, headers=None, body=None, routing_key=None, **kwargs
):
    from celery import current_app

    get_signals_backend(current_app).task_published_handler(
        sender, headers, body, routing_key, **kwargs
    )


@signals.task_prerun.connect
def task_prerun_handler(
    sender=None, task_id=None, task=None, args=None, kwargs=None, **kw
):
    from celery import current_app

    get_signals_backend(current_app).task_prerun_handler(
        sender=sender, task_id=task_id, task=task, args=args, kwargs=kwargs, **kw
    )


@signals.task_postrun.connect
def task_postrun_handler(
    sender=None, task_id=None, task=None, state=None, retval=None, **kwargs
):
    from celery import current_app

    get_signals_backend(current_app).task_postrun_handler(
        sender=sender, task_id=task_id, task=task, state=state, retval=retval, **kwargs
    )


@signals.task_failure.connect
def task_failure_handler(sender=None, task_id=None, exception=None, **kwargs):
    from celery import current_app

    get_signals_backend(current_app).task_failure_handler(
        sender=sender, task_id=task_id, exception=exception, **kwargs
    )


@signals.task_retry.connect
def task_retry_handler(sender=None, task_id=None, reason=None, **kwargs):
    from celery import current_app

    get_signals_backend(current_app).task_retry_handler(
        sender=sender, task_id=task_id, reason=reason, **kwargs
    )


@signals.task_revoked.connect
def task_revoked_handler(sender=None, request=None, terminated=None, **kwargs):
    from celery import current_app

    get_signals_backend(current_app).task_revoked_handler(
        sender=sender, request=request, terminated=terminated, **kwargs
    )
