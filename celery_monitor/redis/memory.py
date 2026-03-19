from celery_monitor.redis.client import get_results_client
from celery_monitor.redis.keys import REDIS_KEY_PREFIX

_SCAN_PATTERNS = {
    "task_details": f"{REDIS_KEY_PREFIX}:tasks:*",
    "task_payloads": f"{REDIS_KEY_PREFIX}:task_payload:*",
    "queue_history": f"{REDIS_KEY_PREFIX}:queue:*",
    "stats": f"{REDIS_KEY_PREFIX}:stats:*",
    "throughput": f"{REDIS_KEY_PREFIX}:throughput:*",
}

_SINGLE_KEYS = [
    f"{REDIS_KEY_PREFIX}:tasks:recent",
    f"{REDIS_KEY_PREFIX}:status_counts",
    f"{REDIS_KEY_PREFIX}:task_names",
    f"{REDIS_KEY_PREFIX}:workers",
    f"{REDIS_KEY_PREFIX}:last_calculation_timestamp",
]


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


_MEMORY_CHUNK_SIZE = 1000


def _sum_memory(client, keys: list[str]) -> int:
    if not keys:
        return 0
    total = 0
    for i in range(0, len(keys), _MEMORY_CHUNK_SIZE):
        pipe = client.pipeline(transaction=False)
        for key in keys[i : i + _MEMORY_CHUNK_SIZE]:
            pipe.memory_usage(key, samples=0)
        total += sum(r for r in pipe.execute() if r)
    return total


def _scan_keys(client, pattern: str) -> list[str]:
    keys = []
    cursor = 0
    while True:
        cursor, batch = client.scan(cursor, match=pattern, count=500)
        keys.extend(batch)
        if cursor == 0:
            break
    return keys


def get_redis_memory_stats() -> dict:
    try:
        client = get_results_client()
    except Exception:
        return {"error": "Redis not available"}

    try:
        categories = {}
        total_bytes = 0
        total_keys = 0

        for category, pattern in _SCAN_PATTERNS.items():
            keys = _scan_keys(client, pattern)
            # exclude single keys tracked separately to avoid double counting
            keys = [k for k in keys if k not in _SINGLE_KEYS]
            size = _sum_memory(client, keys)
            categories[category] = {
                "size_bytes": size,
                "size": _fmt_bytes(size),
                "count": len(keys),
            }
            total_bytes += size
            total_keys += len(keys)

        pipe = client.pipeline(transaction=False)
        for k in _SINGLE_KEYS:
            pipe.exists(k)
        single_keys_existing = [
            k for k, exists in zip(_SINGLE_KEYS, pipe.execute(), strict=False) if exists
        ]
        single_size = _sum_memory(client, single_keys_existing)
        categories["other"] = {
            "size_bytes": single_size,
            "size": _fmt_bytes(single_size),
            "count": len(single_keys_existing),
        }
        total_bytes += single_size
        total_keys += len(single_keys_existing)

        return {
            "total_bytes": total_bytes,
            "total": _fmt_bytes(total_bytes),
            "total_keys": total_keys,
            "categories": categories,
        }
    except Exception:
        return {}
