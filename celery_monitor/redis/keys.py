REDIS_KEY_PREFIX = "celery:monitor"
REDIS_KEY_TASK_DETAILS = "celery:monitor:tasks:{task_id}"  # format with task_id=
REDIS_KEY_RECENT_TASKS = f"{REDIS_KEY_PREFIX}:tasks:recent"
REDIS_KEY_STATUS_COUNTS = f"{REDIS_KEY_PREFIX}:status_counts"
REDIS_KEY_TASKS_NAMES = f"{REDIS_KEY_PREFIX}:task_names"
REDIS_KEY_WORKERS_NAMES = f"{REDIS_KEY_PREFIX}:workers"
REDIS_KEY_QUEUE_LEN_STREAM = "celery:monitor:queue:{queue_name}:history"
REDIS_KEY_QUEUE_LEN_SAMPLE_LOCK = "celery:monitor:queue:{queue_name}:sample_lock"


REDIS_KEY_LAST_CALCULATION_TIMESTAMP = f"{REDIS_KEY_PREFIX}:last_calculation_timestamp"

REDIS_KEY_STATS_TASK = "celery:monitor:stats:task:{name}:{bucket_ts}"
REDIS_KEY_STATS_QUEUE = "celery:monitor:stats:queue:{name}:{bucket_ts}"
REDIS_KEY_STATS_TASK_INDEX = "celery:monitor:stats:task:index"
REDIS_KEY_STATS_QUEUE_INDEX = "celery:monitor:stats:queue:index"

REDIS_KEY_STATS_TASK_ROLLUP = "celery:monitor:stats:task_rollup:{name}"

REDIS_KEY_TASKS_BY_TASK = "celery:monitor:tasks:by_task:{task_name}"
REDIS_KEY_TASKS_BY_QUEUE = "celery:monitor:tasks:by_queue:{queue_name}"
REDIS_KEY_TASK_PAYLOAD = "celery:monitor:task_payload:{task_id}"

REDIS_KEY_THROUGHPUT_TASK = "celery:monitor:throughput:task:{name}:{bucket_ts}"
REDIS_KEY_THROUGHPUT_QUEUE = "celery:monitor:throughput:queue:{name}:{bucket_ts}"
REDIS_KEY_THROUGHPUT_TASK_INDEX = "celery:monitor:throughput:task:index"
REDIS_KEY_THROUGHPUT_QUEUE_INDEX = "celery:monitor:throughput:queue:index"
