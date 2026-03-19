import logging
from collections import defaultdict
from typing import Literal

from celery import shared_task
from django.utils import timezone

from celery_monitor.redis.client import get_results_client
from celery_monitor.redis.constants import (
    STATS_BUCKET_DURATION,
    STATS_TTL_DEFAULT,
    TASK_ID_CHUNK_SIZE,
    THROUGHPUT_BUCKET_DURATION,
)
from celery_monitor.redis.enums import TaskField
from celery_monitor.redis.keys import (
    REDIS_KEY_LAST_CALCULATION_TIMESTAMP,
    REDIS_KEY_RECENT_TASKS,
    REDIS_KEY_STATS_QUEUE,
    REDIS_KEY_STATS_QUEUE_INDEX,
    REDIS_KEY_STATS_TASK,
    REDIS_KEY_STATS_TASK_INDEX,
    REDIS_KEY_STATS_TASK_ROLLUP,
    REDIS_KEY_STATUS_COUNTS,
    REDIS_KEY_TASK_DETAILS,
    REDIS_KEY_TASKS_NAMES,
    REDIS_KEY_THROUGHPUT_QUEUE,
    REDIS_KEY_THROUGHPUT_QUEUE_INDEX,
    REDIS_KEY_THROUGHPUT_TASK,
    REDIS_KEY_THROUGHPUT_TASK_INDEX,
    REDIS_KEY_WORKERS_NAMES,
)
from celery_monitor.redis.utils import (
    get_execution_time,
    get_wait_time,
)

logger = logging.getLogger("celery_monitor")


def _get_stats_ttl_seconds() -> int:
    from django.conf import settings

    return int(getattr(settings, "DJANGO_CELERY_MONITOR_STATS_TTL", STATS_TTL_DEFAULT))


@shared_task
def calculate_celery_stats(overwrite: bool = False):
    client = get_results_client()

    if overwrite:
        _clear_stats(client)

    last_timestamp = client.get(REDIS_KEY_LAST_CALCULATION_TIMESTAMP)

    now = timezone.now().timestamp()
    start_time = (float(last_timestamp) if last_timestamp else None) or float("-inf")

    task_buckets: dict = defaultdict(
        lambda: {"count": 0, "success": 0, "failure": 0, "runtimes": [], "waits": []}
    )
    queue_buckets: dict = defaultdict(
        lambda: {"count": 0, "success": 0, "failure": 0, "runtimes": [], "waits": []}
    )
    task_throughput: dict = defaultdict(lambda: {"queued_count": 0, "started_count": 0})
    queue_throughput: dict = defaultdict(
        lambda: {"queued_count": 0, "started_count": 0}
    )

    offset = 0
    processed_any = False
    while True:
        task_ids = client.zrangebyscore(
            REDIS_KEY_RECENT_TASKS,
            start_time,
            now,
            start=offset,
            num=TASK_ID_CHUNK_SIZE,
        )
        if not task_ids:
            break

        processed_any = True
        pipeline = client.pipeline()
        for task_id in task_ids:
            pipeline.hgetall(REDIS_KEY_TASK_DETAILS.format(task_id=task_id))
        tasks = list(pipeline.execute())

        _merge_buckets(task_buckets, _collect_buckets(tasks, TaskField.TASK_NAME))
        _merge_buckets(queue_buckets, _collect_buckets(tasks, TaskField.QUEUE_NAME))
        _merge_throughput_buckets(
            task_throughput, _collect_throughput_buckets(tasks, TaskField.TASK_NAME)
        )
        _merge_throughput_buckets(
            queue_throughput, _collect_throughput_buckets(tasks, TaskField.QUEUE_NAME)
        )

        offset += len(task_ids)
        if len(task_ids) < TASK_ID_CHUNK_SIZE:
            break

    if not processed_any:
        return

    _save_stats(client, task_buckets, REDIS_KEY_STATS_TASK, REDIS_KEY_STATS_TASK_INDEX)
    _save_stats(
        client, queue_buckets, REDIS_KEY_STATS_QUEUE, REDIS_KEY_STATS_QUEUE_INDEX
    )

    _save_throughput_stats(
        client,
        task_throughput,
        REDIS_KEY_THROUGHPUT_TASK,
        REDIS_KEY_THROUGHPUT_TASK_INDEX,
    )
    _save_throughput_stats(
        client,
        queue_throughput,
        REDIS_KEY_THROUGHPUT_QUEUE,
        REDIS_KEY_THROUGHPUT_QUEUE_INDEX,
    )

    _update_task_rollups(client, task_buckets, overwrite=overwrite)

    client.set(REDIS_KEY_LAST_CALCULATION_TIMESTAMP, now)


@shared_task
def prune_stale_recent_tasks() -> int:
    """Remove task IDs from the recent tasks index that have no detail hash."""
    client = get_results_client()
    removed = 0
    offset = 0

    while True:
        task_ids = client.zrange(
            REDIS_KEY_RECENT_TASKS, offset, offset + TASK_ID_CHUNK_SIZE - 1
        )
        if not task_ids:
            break

        pipe = client.pipeline(transaction=False)
        for task_id in task_ids:
            pipe.exists(REDIS_KEY_TASK_DETAILS.format(task_id=task_id))
        stale = [
            task_id
            for task_id, exists in zip(task_ids, pipe.execute(), strict=False)
            if not exists
        ]

        if stale:
            client.zrem(REDIS_KEY_RECENT_TASKS, *stale)
            removed += len(stale)
        else:
            offset += len(task_ids)

        if len(task_ids) < TASK_ID_CHUNK_SIZE:
            break

    logger.info("prune_stale_recent_tasks: removed %d stale entries", removed)
    return removed


@shared_task
def clear_celery_stats() -> None:
    """Delete all pre-computed stats and reset the monitor class cache."""
    client = get_results_client()
    _clear_stats(client)
    _reset_monitor_cache()


@shared_task
def clear_celery_results() -> None:
    """Delete all raw task result data and reset the monitor class cache."""
    client = get_results_client()
    _delete_results(client)
    _reset_monitor_cache()


@shared_task
def clear_all_celery_data() -> None:
    """Delete all computed stats and raw task results, then reset the monitor class cache."""
    client = get_results_client()
    _clear_stats(client)
    _delete_results(client)
    _reset_monitor_cache()


def _merge_buckets(acc: dict, chunk: dict) -> None:
    for key, data in chunk.items():
        acc[key]["count"] += data["count"]
        acc[key]["success"] += data["success"]
        acc[key]["failure"] += data["failure"]
        acc[key]["runtimes"].extend(data["runtimes"])
        acc[key]["waits"].extend(data["waits"])


def _merge_throughput_buckets(acc: dict, chunk: dict) -> None:
    for key, data in chunk.items():
        acc[key]["queued_count"] += data["queued_count"]
        acc[key]["started_count"] += data["started_count"]


def _collect_buckets(tasks: list[dict], group_key: str) -> dict:
    bucket_seconds = STATS_BUCKET_DURATION.total_seconds()
    buckets: dict = defaultdict(
        lambda: {
            "count": 0,
            "success": 0,
            "failure": 0,
            "runtimes": [],
            "waits": [],
        }
    )

    for task_data in tasks:
        name = task_data.get(group_key)
        status = task_data.get(TaskField.STATUS)
        done_ts = task_data.get(TaskField.DATE_DONE)
        if not name or not status or not done_ts:
            continue

        bucket_ts = _get_bucket_timestamp(done_ts, bucket_seconds=bucket_seconds)
        if not bucket_ts:
            continue

        key = (name, bucket_ts)
        buckets[key]["count"] += 1
        if status == "SUCCESS":
            buckets[key]["success"] += 1
        elif status == "FAILURE":
            buckets[key]["failure"] += 1

        if (runtime := get_execution_time(task_data)) is not None:
            buckets[key]["runtimes"].append(runtime)

        if (wait := get_wait_time(task_data)) is not None:
            buckets[key]["waits"].append(wait)

    return buckets


def _collect_throughput_buckets(tasks: list[dict], group_key: str) -> dict:
    buckets: dict = defaultdict(lambda: {"queued_count": 0, "started_count": 0})
    bucket_seconds = THROUGHPUT_BUCKET_DURATION.total_seconds()

    for task_data in tasks:
        name = task_data.get(group_key)
        if not name:
            continue

        if created_ts := task_data.get(TaskField.DATE_CREATED):
            bucket_ts = _get_bucket_timestamp(created_ts, bucket_seconds=bucket_seconds)
            if not bucket_ts:
                continue

            buckets[(name, bucket_ts)]["queued_count"] += 1

        if started_ts := task_data.get(TaskField.DATE_STARTED):
            bucket_ts = _get_bucket_timestamp(started_ts, bucket_seconds=bucket_seconds)
            if not bucket_ts:
                continue
            buckets[(name, bucket_ts)]["started_count"] += 1

    return buckets


def _accumulate_metric(
    values: list, agg: dict, prefix: Literal["runtime", "wait"]
) -> None:
    if not values:
        return

    agg[f"sum_{prefix}"] += sum(values)
    agg[f"{prefix}_count"] += len(values)

    mn, mx = min(values), max(values)

    current_min = agg[f"min_{prefix}"]
    agg[f"min_{prefix}"] = mn if current_min is None else min(current_min, mn)

    current_max = agg[f"max_{prefix}"]
    agg[f"max_{prefix}"] = mx if current_max is None else max(current_max, mx)


def _merge_minmax(
    pipeline, redis_key: str, prefix: str, new_agg: dict, existing_raw: tuple
) -> None:
    count = new_agg[f"{prefix}_count"]
    if not count:
        return

    cur_min = float(existing_raw[0]) if existing_raw[0] else None
    cur_max = float(existing_raw[1]) if existing_raw[1] else None
    pipeline.hincrbyfloat(redis_key, f"sum_{prefix}", new_agg[f"sum_{prefix}"])
    pipeline.hincrby(redis_key, f"{prefix}_count", count)
    pipeline.hset(
        redis_key,
        mapping={
            f"min_{prefix}": new_agg[f"min_{prefix}"]
            if cur_min is None
            else min(cur_min, new_agg[f"min_{prefix}"]),
            f"max_{prefix}": new_agg[f"max_{prefix}"]
            if cur_max is None
            else max(cur_max, new_agg[f"max_{prefix}"]),
        },
    )


def _update_task_rollups(client, task_buckets: dict, overwrite: bool = False):
    stats_ttl = _get_stats_ttl_seconds()

    name_agg: dict = defaultdict(
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

    for (name, _), data in task_buckets.items():
        aggregated = name_agg[name]
        aggregated["total"] += data["count"]
        aggregated["success"] += data["success"]
        aggregated["failure"] += data["failure"]

        _accumulate_metric(data["runtimes"], aggregated, "runtime")
        _accumulate_metric(data["waits"], aggregated, "wait")

    names = list(name_agg.keys())
    if not names:
        return

    if overwrite:
        # Direct write — safe regardless of whether _clear_stats cleared the keys.
        # Uses hset so existing stale values are overwritten, not accumulated.
        pipeline = client.pipeline()
        for name in names:
            aggregated = name_agg[name]
            redis_key = REDIS_KEY_STATS_TASK_ROLLUP.format(name=name)
            mapping: dict = {
                "total_count": aggregated["total"],
                "success_count": aggregated["success"],
                "failure_count": aggregated["failure"],
            }
            if aggregated["runtime_count"]:
                mapping.update(
                    {
                        "sum_runtime": aggregated["sum_runtime"],
                        "runtime_count": aggregated["runtime_count"],
                        "min_runtime": aggregated["min_runtime"],
                        "max_runtime": aggregated["max_runtime"],
                    }
                )
            if aggregated["wait_count"]:
                mapping.update(
                    {
                        "sum_wait": aggregated["sum_wait"],
                        "wait_count": aggregated["wait_count"],
                        "min_wait": aggregated["min_wait"],
                        "max_wait": aggregated["max_wait"],
                    }
                )
            pipeline.hset(redis_key, mapping=mapping)
            pipeline.expire(redis_key, stats_ttl)
        pipeline.execute()
        return

    # Incremental path: read existing min/max from Redis for correct merge.
    pipeline = client.pipeline()
    for name in names:
        pipeline.hmget(
            REDIS_KEY_STATS_TASK_ROLLUP.format(name=name),
            "min_runtime",
            "max_runtime",
            "min_wait",
            "max_wait",
        )

    current_vals = pipeline.execute()

    pipeline = client.pipeline()
    for name, existing in zip(names, current_vals, strict=False):
        aggregated = name_agg[name]
        redis_key = REDIS_KEY_STATS_TASK_ROLLUP.format(name=name)
        pipeline.hincrby(redis_key, "total_count", aggregated["total"])
        pipeline.hincrby(redis_key, "success_count", aggregated["success"])
        pipeline.hincrby(redis_key, "failure_count", aggregated["failure"])
        _merge_minmax(pipeline, redis_key, "runtime", aggregated, existing[0:2])
        _merge_minmax(pipeline, redis_key, "wait", aggregated, existing[2:4])
        pipeline.expire(redis_key, stats_ttl)
    pipeline.execute()


def _save_throughput_stats(client, buckets: dict, key_template: str, index_key: str):
    stats_ttl = _get_stats_ttl_seconds()
    pipeline = client.pipeline()
    for (name, bucket_ts), data in buckets.items():
        redis_key = key_template.format(name=name, bucket_ts=bucket_ts)
        pipeline.hset(
            redis_key,
            mapping={
                "queued_count": data["queued_count"],
                "started_count": data["started_count"],
            },
        )
        pipeline.expire(redis_key, stats_ttl)
        pipeline.zadd(index_key, {f"{name}:{bucket_ts}": bucket_ts})

    pipeline.expire(index_key, stats_ttl)
    pipeline.execute()


def _or_empty(v):
    return v if v is not None else ""


def _save_stats(client, buckets: dict, key_template: str, index_key: str):
    stats_ttl = _get_stats_ttl_seconds()

    pipeline = client.pipeline()
    for (name, bucket_ts), data in buckets.items():
        runtimes = data["runtimes"]
        waits = data["waits"]
        redis_key = key_template.format(name=name, bucket_ts=bucket_ts)
        pipeline.hset(
            redis_key,
            mapping={
                "count": data["count"],
                "success_count": data["success"],
                "failure_count": data["failure"],
                "sum_runtime": sum(runtimes) if runtimes else "",
                "runtime_count": len(runtimes),
                "avg_runtime": _or_empty(
                    sum(runtimes) / len(runtimes) if runtimes else None
                ),
                "min_runtime": _or_empty(min(runtimes) if runtimes else None),
                "max_runtime": _or_empty(max(runtimes) if runtimes else None),
                "sum_wait": sum(waits) if waits else "",
                "wait_count": len(waits),
                "avg_wait": _or_empty(sum(waits) / len(waits) if waits else None),
                "min_wait": _or_empty(min(waits) if waits else None),
                "max_wait": _or_empty(max(waits) if waits else None),
            },
        )
        pipeline.expire(redis_key, stats_ttl)
        pipeline.zadd(index_key, {f"{name}:{bucket_ts}": bucket_ts})

    pipeline.expire(index_key, stats_ttl)
    pipeline.execute()


def _get_bucket_timestamp(
    raw_timestamp: str, bucket_seconds: int | float
) -> int | None:
    bucket_seconds = int(bucket_seconds)
    try:
        return int(float(raw_timestamp) // bucket_seconds) * bucket_seconds
    except (ValueError, TypeError):
        return None


def _delete_results(client) -> None:
    pipeline = client.pipeline()
    for key in client.scan_iter("celery:monitor:tasks:*", count=500):
        pipeline.delete(key)
    for key in client.scan_iter("celery:monitor:task_payload:*", count=500):
        pipeline.delete(key)
    pipeline.delete(REDIS_KEY_STATUS_COUNTS)
    pipeline.delete(REDIS_KEY_TASKS_NAMES)
    pipeline.delete(REDIS_KEY_WORKERS_NAMES)
    pipeline.execute()


def _reset_monitor_cache() -> None:
    import celery_monitor.results_monitor as rm

    rm._cached_monitor_class = None
    rm._cache_expires_at = 0.0


def _clear_stats(client) -> None:
    """Delete all pre-computed stats keys and reset the calculation timestamp."""
    pipeline = client.pipeline()

    for index_key, key_template in [
        (REDIS_KEY_STATS_TASK_INDEX, REDIS_KEY_STATS_TASK),
        (REDIS_KEY_STATS_QUEUE_INDEX, REDIS_KEY_STATS_QUEUE),
        (REDIS_KEY_THROUGHPUT_TASK_INDEX, REDIS_KEY_THROUGHPUT_TASK),
        (REDIS_KEY_THROUGHPUT_QUEUE_INDEX, REDIS_KEY_THROUGHPUT_QUEUE),
    ]:
        members = client.zrange(index_key, 0, -1)
        for member in members:
            name, _, bucket_ts = member.rpartition(":")
            pipeline.delete(key_template.format(name=name, bucket_ts=bucket_ts))
        pipeline.delete(index_key)

    for key in client.scan_iter("celery:monitor:stats:task_rollup:*"):
        pipeline.delete(key)

    pipeline.delete(REDIS_KEY_LAST_CALCULATION_TIMESTAMP)
    pipeline.execute()
