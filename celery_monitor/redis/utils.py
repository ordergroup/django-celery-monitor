import contextlib
from datetime import datetime, timezone

from celery_monitor.redis.enums import TaskField


def get_execution_time(task_data: dict) -> float | None:
    started = task_data.get(TaskField.DATE_STARTED)
    done = task_data.get(TaskField.DATE_DONE)
    if not started or not done:
        return None

    with contextlib.suppress(ValueError, TypeError):
        elapsed = float(done) - float(started)
        return elapsed if elapsed >= 0 else None

    return None


def get_wait_time(task_data: dict) -> float | None:
    created = task_data.get(TaskField.DATE_CREATED)
    started = task_data.get(TaskField.DATE_STARTED)
    if not created or not started:
        return None

    with contextlib.suppress(ValueError, TypeError):
        elapsed = float(started) - float(created)
        return elapsed if elapsed >= 0 else None

    return None


def get_timestamp(task_data: dict, prop_name: str) -> datetime | None:
    value = task_data.get(prop_name)
    if not value:
        return None

    with contextlib.suppress(ValueError, TypeError):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
