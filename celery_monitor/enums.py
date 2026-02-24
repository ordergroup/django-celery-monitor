from enum import Enum


class BackendType(Enum):
    REDIS = "redis"
    CELERY_RESULTS = "celery_results"
    UNKNOWN = "unknown"

    @classmethod
    def from_str(cls, value: str) -> "BackendType":
        try:
            return cls(value.lower())
        except ValueError:
            return cls.UNKNOWN
