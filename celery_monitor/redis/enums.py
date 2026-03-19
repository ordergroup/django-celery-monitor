import sys

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from enum import Enum

    class StrEnum(str, Enum):
        pass


class TaskField(StrEnum):
    """Task detail hash field names (short to keep hashes in listpack encoding)"""

    TASK_NAME = "n"
    QUEUE_NAME = "q"
    STATUS = "s"
    DATE_CREATED = "dc"
    DATE_STARTED = "ds"
    DATE_DONE = "dd"
    WORKER = "w"
    TASK_ARGS = "a"
    TASK_KWARGS = "kw"
    RESULT = "r"
    EXCEPTION = "e"
    EXCEPTION_TYPE = "et"
    TRACEBACK = "tb"
    RETRY_COUNT = "rc"
    RETRY_REASON = "rr"
    TERMINATED = "t"
