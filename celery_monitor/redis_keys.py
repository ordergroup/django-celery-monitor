REDIS_KEY_PREFIX = "celery:monitor"
REDIS_KEY_TASK_DETAILS = "celery:monitor:tasks:{task_id}"  # format with task_id=
REDIS_KEY_RECENT_TASKS = f"{REDIS_KEY_PREFIX}:tasks:recent"
REDIS_KEY_STATUS_COUNTS = f"{REDIS_KEY_PREFIX}:status_counts"
REDIS_KEY_TASKS_NAMES = f"{REDIS_KEY_PREFIX}:task_names"
REDIS_KEY_WORKERS_NAMES = f"{REDIS_KEY_PREFIX}:workers"
REDIS_KEY_QUEUE_LEN_STREAM = "celery:monitor:queue:{queue_name}:history"
REDIS_KEY_QUEUE_LEN_SAMPLE_LOCK = "celery:monitor:queue:{queue_name}:sample_lock"
