from datetime import timedelta

SCAN_THRESHOLD = 100_000
STATS_BUCKET_DURATION = timedelta(minutes=5)
THROUGHPUT_BUCKET_DURATION = timedelta(seconds=15)
TASK_ID_CHUNK_SIZE = 5_000

STATS_TTL_DEFAULT = 7 * 24 * 60 * 60  # 7 days
