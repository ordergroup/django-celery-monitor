import redis
from celery import current_app


def get_results_client(decode_responses: bool = True) -> redis.Redis:
    redis_url = current_app.conf.result_backend or current_app.conf.broker_url
    if not redis_url:
        raise ValueError(
            "Cannot initialize Redis client. Celery broker_url or result_backend must be set and use Redis"
        )

    return redis.from_url(
        redis_url,
        decode_responses=decode_responses,
        socket_connect_timeout=3,
    )
