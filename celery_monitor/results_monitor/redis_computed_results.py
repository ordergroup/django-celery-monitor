import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from datetime import timezone as dt_timezone

import lz4.frame
from django.utils import timezone

from celery_monitor.models import (
    DashboardStatusCount,
    TaskDetail,
    TaskExecutionStats,
    TaskOverview,
    TasksPage,
    TaskTypeTimeSeries,
    ThroughputBucket,
    WorkerStats,
)
from celery_monitor.redis.client import get_results_client
from celery_monitor.redis.constants import SCAN_THRESHOLD
from celery_monitor.redis.enums import TaskField
from celery_monitor.redis.keys import (
    REDIS_KEY_RECENT_TASKS,
    REDIS_KEY_STATS_QUEUE,
    REDIS_KEY_STATS_QUEUE_INDEX,
    REDIS_KEY_STATS_TASK,
    REDIS_KEY_STATS_TASK_INDEX,
    REDIS_KEY_STATS_TASK_ROLLUP,
    REDIS_KEY_STATUS_COUNTS,
    REDIS_KEY_TASK_DETAILS,
    REDIS_KEY_TASK_PAYLOAD,
    REDIS_KEY_TASKS_NAMES,
    REDIS_KEY_THROUGHPUT_QUEUE,
    REDIS_KEY_THROUGHPUT_QUEUE_INDEX,
    REDIS_KEY_THROUGHPUT_TASK,
    REDIS_KEY_THROUGHPUT_TASK_INDEX,
    REDIS_KEY_WORKERS_NAMES,
)
from celery_monitor.redis.utils import get_execution_time, get_timestamp
from celery_monitor.results_monitor.base import CeleryResultsMonitor
from celery_monitor.results_monitor.workers_results import WorkersCeleryResultsMonitor
from celery_monitor.utils import float_or

logger = logging.getLogger("celery_monitor")


class RedisComputedResultsMonitor(CeleryResultsMonitor):
    def __init__(self):
        super().__init__()
        self.client = get_results_client()
        self.bytes_client = get_results_client(decode_responses=False)
        self.workers_monitor = WorkersCeleryResultsMonitor()

    def get_worker_stats(self, include_offline: bool = False) -> list[WorkerStats]:
        return self.workers_monitor.get_worker_stats(include_offline)

    def get_overall_status_counts(self) -> list[DashboardStatusCount]:
        counts: dict = self.client.hgetall(REDIS_KEY_STATUS_COUNTS)
        stats = [
            DashboardStatusCount(status=status, count=int(count))
            for status, count in sorted(counts.items())
        ]
        total = sum(s.count for s in stats)
        return [DashboardStatusCount("total", total), *stats]

    def get_last_hour_status_counts(self) -> list[DashboardStatusCount]:
        now = timezone.now()
        hour_ago = now - timedelta(hours=1)
        task_ids = self.client.zrangebyscore(
            REDIS_KEY_RECENT_TASKS, hour_ago.timestamp(), now.timestamp()
        )

        status_counts = defaultdict(int)

        if task_ids:
            pipeline = self.client.pipeline()
            for task_id in task_ids:
                pipeline.hget(
                    REDIS_KEY_TASK_DETAILS.format(task_id=task_id), TaskField.STATUS
                )

            for status in pipeline.execute():
                if status:
                    status_counts[status] += 1

        stats = [
            DashboardStatusCount(status=status, count=count)
            for status, count in sorted(status_counts.items())
        ]
        return [DashboardStatusCount("total", sum(status_counts.values())), *stats]

    def get_task_execution_stats(
        self,
        sort_by: str = "total_count",
        sort_order: str = "desc",
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[TaskExecutionStats]:
        if date_from is None and date_to is None:
            return self._get_task_execution_stats_from_rollup(sort_by, sort_order)

        start_ts = date_from.timestamp() if date_from else float("-inf")
        end_ts = date_to.timestamp() if date_to else float("inf")

        members = self.client.zrangebyscore(
            REDIS_KEY_STATS_TASK_INDEX, start_ts, end_ts
        )
        if not members:
            return []

        # Parse "{name}:{bucket_ts}" — split on last ":" since task names may contain "."
        parsed = [(m[: m.rfind(":")], int(m[m.rfind(":") + 1 :])) for m in members]

        pipeline = self.client.pipeline()
        for name, bucket_ts in parsed:
            pipeline.hgetall(
                REDIS_KEY_STATS_TASK.format(name=name, bucket_ts=bucket_ts)
            )

        agg: dict = defaultdict(
            lambda: {
                "total": 0,
                "success": 0,
                "failure": 0,
                "sum_runtime": 0.0,
                "runtime_count": 0,
                "min_runtime": None,
                "max_runtime": None,
                "sum_wait": 0.0,
                "wait_count": 0,
                "min_wait": None,
                "max_wait": None,
            }
        )

        for (name, _), data in zip(parsed, pipeline.execute(), strict=False):
            if not data:
                continue
            a = agg[name]
            a["total"] += int(data["count"])
            a["success"] += int(data["success_count"])
            a["failure"] += int(data["failure_count"])

            runtime_count = int(data.get("runtime_count") or 0)
            if runtime_count:
                a["sum_runtime"] += float(data["sum_runtime"])
                a["runtime_count"] += runtime_count
                min_rt, max_rt = float(data["min_runtime"]), float(data["max_runtime"])
                a["min_runtime"] = (
                    min(a["min_runtime"], min_rt)
                    if a["min_runtime"] is not None
                    else min_rt
                )
                a["max_runtime"] = (
                    max(a["max_runtime"], max_rt)
                    if a["max_runtime"] is not None
                    else max_rt
                )

            wait_count = int(data.get("wait_count") or 0)
            if wait_count:
                a["sum_wait"] += float(data["sum_wait"])
                a["wait_count"] += wait_count
                min_w, max_w = float(data["min_wait"]), float(data["max_wait"])
                a["min_wait"] = (
                    min(a["min_wait"], min_w) if a["min_wait"] is not None else min_w
                )
                a["max_wait"] = (
                    max(a["max_wait"], max_w) if a["max_wait"] is not None else max_w
                )

        result = [
            TaskExecutionStats(
                task_name=name,
                total_count=a["total"],
                success_count=a["success"],
                failure_count=a["failure"],
                avg_runtime=a["sum_runtime"] / a["runtime_count"]
                if a["runtime_count"]
                else None,
                min_runtime=a["min_runtime"],
                max_runtime=a["max_runtime"],
                avg_wait=a["sum_wait"] / a["wait_count"] if a["wait_count"] else None,
                min_wait=a["min_wait"],
                max_wait=a["max_wait"],
            )
            for name, a in agg.items()
        ]

        return sorted(
            result,
            key=lambda x: getattr(x, sort_by, None) or -1,
            reverse=(sort_order == "desc"),
        )

    def _get_task_execution_stats_from_rollup(
        self, sort_by: str, sort_order: str
    ) -> list[TaskExecutionStats]:
        task_names = self.client.smembers(REDIS_KEY_TASKS_NAMES)
        if not task_names:
            return []

        pipeline = self.client.pipeline()
        for name in task_names:
            pipeline.hgetall(REDIS_KEY_STATS_TASK_ROLLUP.format(name=name))

        result = []
        for name, data in zip(task_names, pipeline.execute(), strict=False):
            if not data:
                continue
            total = int(data.get("total_count") or 0)
            if not total:
                continue
            runtime_count = int(data.get("runtime_count") or 0)
            wait_count = int(data.get("wait_count") or 0)
            result.append(
                TaskExecutionStats(
                    task_name=name,
                    total_count=total,
                    success_count=int(data.get("success_count") or 0),
                    failure_count=int(data.get("failure_count") or 0),
                    avg_runtime=float(data["sum_runtime"]) / runtime_count
                    if runtime_count
                    else None,
                    min_runtime=float_or(data.get("min_runtime")),
                    max_runtime=float_or(data.get("max_runtime")),
                    avg_wait=float(data["sum_wait"]) / wait_count
                    if wait_count
                    else None,
                    min_wait=float_or(data.get("min_wait")),
                    max_wait=float_or(data.get("max_wait")),
                )
            )

        return sorted(
            result,
            key=lambda x: getattr(x, sort_by, None) or -1,
            reverse=(sort_order == "desc"),
        )

    def get_recent_tasks(
        self,
        status: str | None = None,
        task_name: str | None = None,
        queue_name: str | None = None,
        worker: str | None = None,
        limit: int = 50,
    ) -> list[TaskOverview]:
        """
        Get recent tasks from Redis, if filtering is applied this function will return
        always less results than limit, because filtering is happening after fetching the data from redis.
        """
        task_ids = self.client.zrevrange(REDIS_KEY_RECENT_TASKS, 0, limit * 2 - 1)
        if not task_ids:
            return []

        pipeline = self.client.pipeline()
        for task_id in task_ids:
            pipeline.hgetall(REDIS_KEY_TASK_DETAILS.format(task_id=task_id))

        recent_tasks = []
        for task_id, task_data in zip(task_ids, pipeline.execute(), strict=False):
            task_status = task_data.get(TaskField.STATUS)
            task_name_value = task_data.get(TaskField.TASK_NAME)
            queue_name_value = task_data.get(TaskField.QUEUE_NAME)
            task_worker = task_data.get(TaskField.WORKER)

            if status and task_status != status:
                continue
            if task_name and task_name_value != task_name:
                continue
            if queue_name and queue_name_value != queue_name:
                continue
            if worker and task_worker != worker:
                continue

            recent_tasks.append(
                TaskOverview(
                    task_id=task_id,
                    task_name=task_name_value,
                    status=task_status,
                    worker=task_worker,
                    date_started=get_timestamp(task_data, TaskField.DATE_STARTED),
                    date_done=get_timestamp(task_data, TaskField.DATE_DONE),
                    execution_time=get_execution_time(task_data),
                    queue_name=task_data.get(TaskField.QUEUE_NAME, "unknown"),
                )
            )

        return recent_tasks

    def get_task_detail(self, task_id: str) -> TaskDetail | None:
        """Get detailed information about a specific task from Redis."""
        task_data: dict = self.client.hgetall(
            REDIS_KEY_TASK_DETAILS.format(task_id=task_id)
        )
        if not task_data:
            return self.workers_monitor.get_task_detail(task_id)

        task_args = task_kwargs = result = exception = exception_type = traceback = None
        payload_raw = self.bytes_client.get(
            REDIS_KEY_TASK_PAYLOAD.format(task_id=task_id)
        )
        if payload_raw:
            payload_data = json.loads(lz4.frame.decompress(payload_raw))
            task_args = payload_data.get(TaskField.TASK_ARGS)
            task_kwargs = payload_data.get(TaskField.TASK_KWARGS)
            result = payload_data.get(TaskField.RESULT)
            exception = payload_data.get(TaskField.EXCEPTION)
            exception_type = payload_data.get(TaskField.EXCEPTION_TYPE)

        date_started = get_timestamp(task_data, TaskField.DATE_STARTED)
        date_done = get_timestamp(task_data, TaskField.DATE_DONE)
        date_created = get_timestamp(task_data, TaskField.DATE_CREATED)

        return TaskDetail(
            task_id=task_id,
            task_name=task_data.get(TaskField.TASK_NAME),
            status=task_data.get(TaskField.STATUS),
            worker=task_data.get(TaskField.WORKER, "unknown"),
            queue_name=task_data.get(TaskField.QUEUE_NAME, "unknown"),
            date_started=date_started.isoformat() if date_started else None,
            date_created=date_created.isoformat() if date_created else None,
            date_done=date_done.isoformat() if date_done else None,
            task_args=task_args,
            task_kwargs=task_kwargs,
            result=result,
            traceback=traceback,
            meta=None,
            periodic_task_name=None,
            exception=exception,
            exception_type=exception_type,
        )

    def get_tasks(
        self,
        status: str | None = None,
        task_name: str | None = None,
        queue_name: str | None = None,
        worker: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        page: int = 0,
        page_size: int = 50,
    ) -> TasksPage:
        start_time = date_from.timestamp() if date_from else float("-inf")
        end_time = date_to.timestamp() if date_to else float("inf")

        name_queue_requested = bool(task_name or queue_name)
        filter_skipped = (
            name_queue_requested
            and self.client.zcount(REDIS_KEY_RECENT_TASKS, start_time, end_time)
            > SCAN_THRESHOLD
        )
        effective_task_name = None if filter_skipped else task_name
        effective_queue_name = None if filter_skipped else queue_name

        all_task_ids = list(
            reversed(
                self.client.zrangebyscore(REDIS_KEY_RECENT_TASKS, start_time, end_time)
            )
        )

        needs_filter = status or worker or effective_task_name or effective_queue_name

        if needs_filter:
            pipeline = self.client.pipeline()
            for task_id in all_task_ids:
                pipeline.hgetall(REDIS_KEY_TASK_DETAILS.format(task_id=task_id))
            filtered = [
                (tid, td)
                for tid, td in zip(all_task_ids, pipeline.execute(), strict=False)
                if (not status or td.get(TaskField.STATUS) == status)
                and (not worker or td.get(TaskField.WORKER) == worker)
                and (
                    not effective_task_name
                    or td.get(TaskField.TASK_NAME) == effective_task_name
                )
                and (
                    not effective_queue_name
                    or td.get(TaskField.QUEUE_NAME) == effective_queue_name
                )
            ]
            total = len(filtered)
            start = page * page_size
            page_data = filtered[start : start + page_size]
            tasks = [
                TaskOverview(
                    task_id=tid,
                    task_name=td.get(TaskField.TASK_NAME),
                    status=td.get(TaskField.STATUS),
                    worker=td.get(TaskField.WORKER),
                    date_started=get_timestamp(td, TaskField.DATE_STARTED),
                    date_done=get_timestamp(td, TaskField.DATE_DONE),
                    execution_time=get_execution_time(td),
                    queue_name=td.get(TaskField.QUEUE_NAME, "unknown"),
                )
                for tid, td in page_data
            ]
            return TasksPage(tasks=tasks, total=total, filter_skipped=filter_skipped)

        total = len(all_task_ids)
        start = page * page_size
        task_ids = all_task_ids[start : start + page_size]
        return TasksPage(
            tasks=self._get_tasks_overviews(task_ids),
            total=total,
            filter_skipped=filter_skipped,
        )

    def _get_tasks_overviews(self, task_ids: list[str]) -> list[TaskOverview]:
        pipeline = self.client.pipeline()
        for task_id in task_ids:
            pipeline.hgetall(REDIS_KEY_TASK_DETAILS.format(task_id=task_id))

        return [
            TaskOverview(
                task_id=task_id,
                task_name=task_data.get(TaskField.TASK_NAME),
                status=task_data.get(TaskField.STATUS),
                worker=task_data.get(TaskField.WORKER),
                date_started=get_timestamp(task_data, TaskField.DATE_STARTED),
                date_done=get_timestamp(task_data, TaskField.DATE_DONE),
                execution_time=get_execution_time(task_data),
                queue_name=task_data.get(TaskField.QUEUE_NAME, "unknown"),
            )
            for task_id, task_data in zip(task_ids, pipeline.execute(), strict=False)
        ]

    def get_task_type_time_series(
        self,
        task_name: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[TaskTypeTimeSeries]:
        bucket_timestamps = self._get_bucket_timestamps(
            REDIS_KEY_STATS_TASK_INDEX, task_name, date_from, date_to
        )
        if not bucket_timestamps:
            return []

        pipeline = self.client.pipeline()
        for ts in bucket_timestamps:
            pipeline.hgetall(REDIS_KEY_STATS_TASK.format(name=task_name, bucket_ts=ts))

        return sorted(
            [
                _hash_to_time_series(ts, data)
                for ts, data in zip(bucket_timestamps, pipeline.execute(), strict=False)
                if data
            ],
            key=lambda x: x.bucket,
        )

    def get_throughput_time_series(
        self,
        task_name: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[ThroughputBucket]:
        bucket_timestamps = self._get_bucket_timestamps(
            REDIS_KEY_THROUGHPUT_TASK_INDEX, task_name, date_from, date_to
        )
        if not bucket_timestamps:
            return []

        pipeline = self.client.pipeline()
        for ts in bucket_timestamps:
            pipeline.hgetall(
                REDIS_KEY_THROUGHPUT_TASK.format(name=task_name, bucket_ts=ts)
            )

        return sorted(
            [
                ThroughputBucket(
                    bucket=datetime.fromtimestamp(ts, tz=dt_timezone.utc),
                    queued_count=int(data.get("queued_count", 0)),
                    started_count=int(data.get("started_count", 0)),
                )
                for ts, data in zip(bucket_timestamps, pipeline.execute(), strict=False)
                if data
            ],
            key=lambda x: x.bucket,
        )

    def get_queue_time_series(
        self,
        queue_name: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[TaskTypeTimeSeries]:
        bucket_timestamps = self._get_bucket_timestamps(
            REDIS_KEY_STATS_QUEUE_INDEX, queue_name, date_from, date_to
        )
        if not bucket_timestamps:
            return []

        pipeline = self.client.pipeline()
        for ts in bucket_timestamps:
            pipeline.hgetall(
                REDIS_KEY_STATS_QUEUE.format(name=queue_name, bucket_ts=ts)
            )

        return sorted(
            [
                _hash_to_time_series(ts, data)
                for ts, data in zip(bucket_timestamps, pipeline.execute(), strict=False)
                if data
            ],
            key=lambda x: x.bucket,
        )

    def _get_bucket_timestamps(
        self,
        index_key: str,
        name: str,
        date_from: datetime | None,
        date_to: datetime | None,
    ) -> list[int]:
        start_ts = date_from.timestamp() if date_from else float("-inf")
        end_ts = date_to.timestamp() if date_to else float("inf")
        members = self.client.zrangebyscore(index_key, start_ts, end_ts)
        prefix = f"{name}:"
        return [int(m[len(prefix) :]) for m in members if m.startswith(prefix)]

    def get_queue_throughput_time_series(
        self,
        queue_name: str,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
    ) -> list[ThroughputBucket]:
        bucket_timestamps = self._get_bucket_timestamps(
            REDIS_KEY_THROUGHPUT_QUEUE_INDEX, queue_name, date_from, date_to
        )
        if not bucket_timestamps:
            return []

        pipeline = self.client.pipeline()
        for ts in bucket_timestamps:
            pipeline.hgetall(
                REDIS_KEY_THROUGHPUT_QUEUE.format(name=queue_name, bucket_ts=ts)
            )

        return sorted(
            [
                ThroughputBucket(
                    bucket=datetime.fromtimestamp(ts, tz=dt_timezone.utc),
                    queued_count=int(data.get("queued_count", 0)),
                    started_count=int(data.get("started_count", 0)),
                )
                for ts, data in zip(bucket_timestamps, pipeline.execute(), strict=False)
                if data
            ],
            key=lambda x: x.bucket,
        )

    def get_tasks_names(self) -> list[str]:
        return sorted(self.client.smembers(REDIS_KEY_TASKS_NAMES))

    def get_workers_names(self) -> list[str]:
        return sorted(self.client.smembers(REDIS_KEY_WORKERS_NAMES))


def _hash_to_time_series(bucket_ts: int, data: dict) -> TaskTypeTimeSeries:
    return TaskTypeTimeSeries(
        bucket=datetime.fromtimestamp(bucket_ts, tz=dt_timezone.utc),
        count=int(data["count"]),
        success_count=int(data["success_count"]),
        failure_count=int(data["failure_count"]),
        avg_runtime=float_or(data.get("avg_runtime")),
        min_runtime=float_or(data.get("min_runtime")),
        max_runtime=float_or(data.get("max_runtime")),
        avg_wait=float_or(data.get("avg_wait")),
        min_wait=float_or(data.get("min_wait")),
        max_wait=float_or(data.get("max_wait")),
    )
