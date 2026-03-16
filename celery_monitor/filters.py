import logging
from dataclasses import dataclass, field
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
        status = request.GET.get("status", "").strip()
        if status not in VALID_STATUSES:
            status = None
        return cls(
            status=status,
            task_name=request.GET.get("task_name", "").strip() or None,
            queue_name=request.GET.get("queue_name", "").strip() or None,
            worker=request.GET.get("worker", "").strip() or None,
        )


@dataclass
class TaskExecutionStatsFilters:
    sort_by: str = "total_count"
    sort_order: str = "desc"
    date_from: datetime | None = None
    date_to: datetime | None = None

    VALID_SORTS: frozenset = field(
        default_factory=lambda: frozenset(
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
        ),
        repr=False,
        compare=False,
    )

    @classmethod
    def from_request(cls, request: HttpRequest) -> "TaskExecutionStatsFilters":
        hours_param_raw = request.GET.get("hours", "1")
        date_from, date_to = get_date_range(request)

        if not date_from and not date_to:
            if hours_param_raw == "all":
                date_from = None
                date_to = None
            else:
                try:
                    hours = int(hours_param_raw)
                    if hours <= 0:
                        hours = 1

                    date_to = datetime.now(tz=UTC)
                    date_from = date_to - timedelta(hours=hours)
                except (ValueError, TypeError):
                    date_from = None
                    date_to = None

        valid_sorts = frozenset(
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
        sort_by = request.GET.get("sort", "total_count").strip()
        if not sort_by or sort_by not in valid_sorts:
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
        hours_param_raw = request.GET.get("hours", "24")
        date_from, date_to = get_date_range(request)

        if not date_from and not date_to:
            if hours_param_raw == "all":
                date_from = None
                date_to = None
            else:
                try:
                    hours = int(hours_param_raw)
                    if hours <= 0:
                        hours = 24
                    date_to = datetime.now(tz=UTC)
                    date_from = date_to - timedelta(hours=hours)
                except (ValueError, TypeError):
                    date_from = None
                    date_to = None

        status = request.GET.get("status", "").strip() or None
        if status not in VALID_STATUSES:
            status = None

        try:
            page = max(0, int(request.GET.get("page", 0)))
        except (ValueError, TypeError):
            page = 0

        return cls(
            task_name=request.GET.get("task_name", "").strip(),
            date_from=date_from,
            date_to=date_to,
            status=status,
            worker=request.GET.get("worker", "").strip() or None,
            page=page,
            hours_param=hours_param_raw,
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
        hours_param_raw = request.GET.get("hours", "24")
        date_from, date_to = get_date_range(request)

        if not date_from and not date_to:
            if hours_param_raw == "all":
                date_from = None
                date_to = None
            else:
                try:
                    hours = int(hours_param_raw)
                    if hours <= 0:
                        hours = 24
                    date_to = datetime.now(tz=UTC)
                    date_from = date_to - timedelta(hours=hours)
                except (ValueError, TypeError):
                    date_from = None
                    date_to = None

        status = request.GET.get("status", "").strip() or None
        if status not in VALID_STATUSES:
            status = None

        try:
            page = max(0, int(request.GET.get("page", 0)))
        except (ValueError, TypeError):
            page = 0

        return cls(
            queue_name=request.GET.get("queue_name", "").strip(),
            date_from=date_from,
            date_to=date_to,
            status=status,
            worker=request.GET.get("worker", "").strip() or None,
            page=page,
            hours_param=hours_param_raw,
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
    worker: str | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    page: int = 0
    page_size: int = 50

    @classmethod
    def from_request(cls, request: HttpRequest) -> "TaskResultsFilters":
        date_from, date_to = get_date_range(request)
        try:
            page = max(0, int(request.GET.get("page", 0)))
        except (ValueError, TypeError):
            page = 0

        status = request.GET.get("status", "").strip() or None
        if status not in VALID_STATUSES:
            status = None

        return cls(
            status=status,
            task_name=request.GET.get("task_name", "").strip() or None,
            worker=request.GET.get("worker", "").strip() or None,
            date_from=date_from,
            date_to=date_to,
            page=page,
        )


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
