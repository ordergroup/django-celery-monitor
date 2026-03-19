import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

from dateutil.tz import UTC
from django.http import HttpRequest

logger = logging.getLogger("celery_monitor")
VALID_STATUSES = frozenset(
    {"SUCCESS", "FAILURE", "STARTED", "PENDING", "QUEUED", "RETRY", "REVOKED"}
)


@dataclass
class RecentTasksFilters:
    status: str | None = None
    task_name: str | None = None
    queue_name: str | None = None
    worker: str | None = None
    limit: int = 50

    @classmethod
    def from_request(cls, request: HttpRequest) -> "RecentTasksFilters":
        return cls(
            status=_parse_status(request),
            task_name=_clean(request.GET.get("task_name", "")),
            queue_name=_clean(request.GET.get("queue_name", "")),
            worker=_clean(request.GET.get("worker", "")),
        )


@dataclass
class TaskExecutionStatsFilters:
    VALID_SORTS: frozenset = frozenset(
        {
            "task_name",
            "total_count",
            "success_count",
            "failure_count",
            "avg_runtime",
            "min_runtime",
            "max_runtime",
            "avg_wait",
            "min_wait",
            "max_wait",
        }
    )

    sort_by: str = "total_count"
    sort_order: str = "desc"
    date_from: datetime | None = None
    date_to: datetime | None = None

    @classmethod
    def from_request(cls, request: HttpRequest) -> "TaskExecutionStatsFilters":
        date_from, date_to = _resolve_date_range(request, default_hours=1)

        sort_by = request.GET.get("sort", "total_count").strip()
        if not sort_by or sort_by not in cls.VALID_SORTS:
            sort_by = "total_count"

        sort_order = request.GET.get("order", "desc").strip()
        if not sort_order or sort_order not in {"asc", "desc"}:
            sort_order = "desc"

        return cls(
            sort_by=sort_by,
            sort_order=sort_order,
            date_from=date_from,
            date_to=date_to,
        )


@dataclass
class TaskTypeDetailFilters:
    task_name: str = ""
    date_from: datetime | None = None
    date_to: datetime | None = None
    status: str | None = None
    worker: str | None = None
    page: int = 0
    page_size: int = 50
    hours_param: str = "24"

    @classmethod
    def from_request(cls, request: HttpRequest) -> "TaskTypeDetailFilters":
        date_from, date_to = _resolve_date_range(request, default_hours=24)
        return cls(
            task_name=_clean(request.GET.get("task_name", "")) or "",
            date_from=date_from,
            date_to=date_to,
            status=_parse_status(request),
            worker=_clean(request.GET.get("worker", "")),
            page=_parse_page(request),
            hours_param=request.GET.get("hours", "24"),
        )


@dataclass
class QueueDetailFilters:
    queue_name: str = ""
    date_from: datetime | None = None
    date_to: datetime | None = None
    status: str | None = None
    worker: str | None = None
    page: int = 0
    page_size: int = 50
    hours_param: str = "24"

    @classmethod
    def from_request(cls, request: HttpRequest) -> "QueueDetailFilters":
        date_from, date_to = _resolve_date_range(request, default_hours=24)
        return cls(
            queue_name=_clean(request.GET.get("queue_name", "")) or "",
            date_from=date_from,
            date_to=date_to,
            status=_parse_status(request),
            worker=_clean(request.GET.get("worker", "")),
            page=_parse_page(request),
            hours_param=request.GET.get("hours", "24"),
        )


@dataclass
class WorkerStatsFilters:
    include_offline: bool = False

    @classmethod
    def from_request(cls, request: HttpRequest) -> "WorkerStatsFilters":
        return cls(
            include_offline=request.GET.get("include_offline") == "true",
        )


@dataclass
class TaskResultsFilters:
    status: str | None = None
    task_name: str | None = None
    queue_name: str | None = None
    worker: str | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    page: int = 0
    page_size: int = 50

    @classmethod
    def from_request(cls, request: HttpRequest) -> "TaskResultsFilters":
        date_from, date_to = get_date_range(request)
        return cls(
            status=_parse_status(request),
            task_name=_clean(request.GET.get("task_name", "")),
            queue_name=_clean(request.GET.get("queue_name", "")),
            worker=_clean(request.GET.get("worker", "")),
            date_from=date_from,
            date_to=date_to,
            page=_parse_page(request),
        )


def _clean(value: str | None) -> str | None:
    """Strip whitespace and treat empty string or literal 'None' as None."""
    if not value:
        return None
    value = value.strip()
    return None if value in ("", "None") else value


def _parse_status(request: HttpRequest) -> str | None:
    status = request.GET.get("status", "").strip() or None
    return status if status in VALID_STATUSES else None


def _parse_page(request: HttpRequest) -> int:
    try:
        return max(0, int(request.GET.get("page", 0)))
    except (ValueError, TypeError):
        return 0


def _resolve_date_range(
    request: HttpRequest, default_hours: int
) -> tuple[datetime | None, datetime | None]:
    date_from, date_to = get_date_range(request)
    if date_from or date_to:
        return date_from, date_to

    hours_param = request.GET.get("hours", str(default_hours))
    if hours_param == "all":
        return None, None

    try:
        hours = int(hours_param)
        if hours <= 0:
            hours = default_hours
        date_to = datetime.now(tz=UTC)
        return date_to - timedelta(hours=hours), date_to
    except (ValueError, TypeError):
        return None, None


def get_date_range(request: HttpRequest) -> tuple[datetime | None, datetime | None]:
    date_from_param = request.GET.get("date_from", "").strip()
    date_to_param = request.GET.get("date_to", "").strip()
    date_from: datetime | None = None
    date_to: datetime | None = None
    if date_from_param:
        try:
            date_from = datetime.fromisoformat(date_from_param)
        except ValueError:
            logger.exception("failed to parse date params")
    if date_to_param:
        try:
            date_to = datetime.fromisoformat(date_to_param)
        except ValueError:
            logger.exception("failed to parse date params")

    return date_from, date_to
