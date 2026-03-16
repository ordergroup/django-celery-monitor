import contextlib
import json
import logging
from datetime import timedelta

from celery import Celery
from django.conf import settings
from django.utils import timezone
from redis.client import Pipeline

from celery_monitor.redis_keys import (
    REDIS_KEY_QUEUE_LEN_SAMPLE_LOCK,
    REDIS_KEY_QUEUE_LEN_STREAM,
    REDIS_KEY_RECENT_TASKS,
    REDIS_KEY_STATUS_COUNTS,
    REDIS_KEY_TASK_DETAILS,
    REDIS_KEY_TASKS_NAMES,
    REDIS_KEY_WORKERS_NAMES,
)
from celery_monitor.signals_backend.base import SignalsResultBackend

logger = logging.getLogger(__name__)


class RedisSignalsResultBackend(SignalsResultBackend):
    def __init__(self, app: Celery):
        self.app = app
        self.task_data_ttl = getattr(
            settings, "DJANGO_CELERY_MONITOR_TASK_DATA_TTL", 7 * 24 * 60 * 60
        )
        self.queue_history_maxlen = getattr(
            settings, "DJANGO_CELERY_MONITOR_QUEUE_HISTORY_MAXLEN", 10000
        )
        self.redis_client = self._get_redis_client()
        self.broker_redis_client = self._get_broker_redis_client()

    def task_published_handler(
        self, sender=None, headers=None, body=None, routing_key=None, **kwargs
    ):
        now = timezone.now()
        task_id = (headers or {}).get("id")
        if not task_id:
            return

        task_name = sender or (headers or {}).get("task")
        task_data = {
            "task_id": task_id,
            "task_name": task_name,
            "queue_name": routing_key,
            "status": "QUEUED",
            "date_created": str(now.timestamp()),
        }

        pipeline = self.redis_client.pipeline()
        pipeline.hset(REDIS_KEY_TASK_DETAILS.format(task_id=task_id), mapping=task_data)
        pipeline.zadd(REDIS_KEY_RECENT_TASKS, {task_id: now.timestamp()})
        if task_name:
            pipeline.sadd(REDIS_KEY_TASKS_NAMES, task_name)
        pipeline.hincrby(REDIS_KEY_STATUS_COUNTS, "QUEUED", 1)
        pipeline.expire(
            REDIS_KEY_TASK_DETAILS.format(task_id=task_id), self.task_data_ttl
        )
        cutoff = now - timedelta(seconds=self.task_data_ttl)
        pipeline.zremrangebyscore(REDIS_KEY_RECENT_TASKS, 0, cutoff.timestamp())

        self._update_queue_len(pipeline, routing_key)
        pipeline.execute()

    def task_prerun_handler(
        self, sender=None, task_id=None, task=None, args=None, kwargs=None, **kw
    ):
        now = timezone.now()

        worker = "unknown"
        queue_name = None
        if hasattr(task, "request") and task.request:
            worker = task.request.hostname or "unknown"
            queue_name = (task.request.delivery_info or {}).get("routing_key")
        if worker == "unknown":
            worker = kw.get("hostname", "unknown")

        task_args_str = json.dumps(args) if args else None
        task_kwargs_str = json.dumps(kwargs) if kwargs else None

        update_data = {
            "status": "STARTED",
            "worker": worker,
            "date_started": str(now.timestamp()),
        }
        if task_args_str:
            update_data["task_args"] = task_args_str
        if task_kwargs_str:
            update_data["task_kwargs"] = task_kwargs_str

        # Read existing status before pipeline to decide count transition
        existing_status = self.redis_client.hget(
            REDIS_KEY_TASK_DETAILS.format(task_id=task_id), "status"
        )

        pipeline = self.redis_client.pipeline()
        pipeline.hset(
            REDIS_KEY_TASK_DETAILS.format(task_id=task_id), mapping=update_data
        )
        # Fallback: set date_created and task_name only if not already set by publish handler
        pipeline.hsetnx(
            REDIS_KEY_TASK_DETAILS.format(task_id=task_id),
            "date_created",
            str(now.timestamp()),
        )
        pipeline.hsetnx(
            REDIS_KEY_TASK_DETAILS.format(task_id=task_id), "task_name", task.name
        )
        pipeline.zadd(REDIS_KEY_RECENT_TASKS, {task_id: now.timestamp()})
        pipeline.sadd(REDIS_KEY_TASKS_NAMES, task.name)
        pipeline.sadd(REDIS_KEY_WORKERS_NAMES, worker)
        if existing_status:
            pipeline.hincrby(REDIS_KEY_STATUS_COUNTS, existing_status, -1)
        pipeline.hincrby(REDIS_KEY_STATUS_COUNTS, "STARTED", 1)
        pipeline.expire(
            REDIS_KEY_TASK_DETAILS.format(task_id=task_id), self.task_data_ttl
        )
        cutoff = now - timedelta(seconds=self.task_data_ttl)
        pipeline.zremrangebyscore(REDIS_KEY_RECENT_TASKS, 0, cutoff.timestamp())
        self._update_queue_len(pipeline, queue_name)
        pipeline.execute()

    def task_postrun_handler(
        self, sender=None, task_id=None, task=None, state=None, retval=None, **kwargs
    ):
        now = timezone.now()

        update_data = {
            "status": state,
            "date_done": str(now.timestamp()),
        }

        if retval is not None:
            try:
                result_str = json.dumps(retval)
                update_data["result"] = result_str
            except (TypeError, ValueError):
                # If not serializable, store string representation
                update_data["result"] = str(retval)

        task_data = self.redis_client.hgetall(
            REDIS_KEY_TASK_DETAILS.format(task_id=task_id)
        )
        previous_status = task_data.get("status") if task_data else None

        pipeline = self.redis_client.pipeline()

        pipeline.hset(
            REDIS_KEY_TASK_DETAILS.format(task_id=task_id), mapping=update_data
        )

        if previous_status and previous_status != state:
            pipeline.hincrby(REDIS_KEY_STATUS_COUNTS, previous_status, -1)

        pipeline.hincrby(REDIS_KEY_STATUS_COUNTS, state, 1)
        pipeline.expire(
            REDIS_KEY_TASK_DETAILS.format(task_id=task_id), self.task_data_ttl
        )
        pipeline.execute()

    def task_failure_handler(self, sender=None, task_id=None, exception=None, **kwargs):
        exception_info = {
            "exception": str(exception),
            "exception_type": type(exception).__name__,
        }

        pipeline = self.redis_client.pipeline()
        pipeline.hset(
            REDIS_KEY_TASK_DETAILS.format(task_id=task_id), mapping=exception_info
        )
        pipeline.execute()

    def task_retry_handler(self, sender=None, task_id=None, reason=None, **kwargs):
        pipeline = self.redis_client.pipeline()
        pipeline.hset(REDIS_KEY_TASK_DETAILS.format(task_id=task_id), "status", "RETRY")

        if reason:
            pipeline.hset(
                REDIS_KEY_TASK_DETAILS.format(task_id=task_id),
                "retry_reason",
                str(reason),
            )

        pipeline.hincrby(
            REDIS_KEY_TASK_DETAILS.format(task_id=task_id), "retry_count", 1
        )
        pipeline.execute()

    def task_revoked_handler(
        self, sender=None, request=None, terminated=None, **kwargs
    ):
        task_id = request.id if request else None
        if not task_id:
            return

        now = timezone.now()

        update_data = {
            "status": "REVOKED",
            "date_done": str(now.timestamp()),
            "terminated": str(terminated),
        }

        task_data = self.redis_client.hgetall(
            REDIS_KEY_TASK_DETAILS.format(task_id=task_id)
        )
        previous_status = task_data.get("status") if task_data else None

        pipeline = self.redis_client.pipeline()
        pipeline.hset(
            REDIS_KEY_TASK_DETAILS.format(task_id=task_id), mapping=update_data
        )

        if previous_status and previous_status != "REVOKED":
            pipeline.hincrby(REDIS_KEY_STATUS_COUNTS, previous_status, -1)

        pipeline.hincrby(REDIS_KEY_STATUS_COUNTS, "REVOKED", 1)

        pipeline.execute()

    def _get_redis_client(self):
        try:
            import redis

            redis_url = self.app.conf.result_backend or self.app.conf.broker_url
            return redis.from_url(
                redis_url, decode_responses=True, socket_connect_timeout=3
            )
        except Exception as e:
            logger.warning("Could not connect to Redis for monitoring: %s", e)
            return None

    def _get_broker_redis_client(self):
        try:
            import redis

            return redis.from_url(
                self.app.conf.broker_url,
                decode_responses=True,
                socket_connect_timeout=3,
            )
        except Exception as e:
            logger.warning(
                "Could not connect to Redis broker for queue monitoring: %s", e
            )
            return None

    def _update_queue_len(
        self,
        pipeline: Pipeline,
        queue_name: str | None = None,
    ):
        if not queue_name:
            return
        lock_key = REDIS_KEY_QUEUE_LEN_SAMPLE_LOCK.format(queue_name=queue_name)
        acquired = self.redis_client.set(lock_key, 1, nx=True, ex=60)
        if not acquired:
            return

        queue_len = None
        if self.broker_redis_client:
            with contextlib.suppress(Exception):
                queue_len = self.broker_redis_client.llen(queue_name)

        stream_key = REDIS_KEY_QUEUE_LEN_STREAM.format(queue_name=queue_name)
        pipeline.xadd(
            stream_key,
            {"queue_len": queue_len},
            maxlen=self.queue_history_maxlen,
            approximate=True,
        )
