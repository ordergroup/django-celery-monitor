from celery_monitor.filters import (
    RecentTasksFilters,
    TaskExecutionStatsFilters,
    TaskResultsFilters,
    WorkerStatsFilters,
    get_date_range,
)


class TestRecentTasksFilters:
    def test_valid_status(self, rf):
        f = RecentTasksFilters.from_request(rf.get("/", {"status": "SUCCESS"}))
        assert f.status == "SUCCESS"

    def test_all_valid_statuses(self, rf):
        for status in ("SUCCESS", "FAILURE", "STARTED", "PENDING", "RETRY", "REVOKED"):
            f = RecentTasksFilters.from_request(rf.get("/", {"status": status}))
            assert f.status == status

    def test_invalid_status_is_none(self, rf):
        f = RecentTasksFilters.from_request(rf.get("/", {"status": "INVALID"}))
        assert f.status is None

    def test_empty_status_is_none(self, rf):
        f = RecentTasksFilters.from_request(rf.get("/"))
        assert f.status is None

    def test_task_name(self, rf):
        f = RecentTasksFilters.from_request(rf.get("/", {"task_name": "tasks.add"}))
        assert f.task_name == "tasks.add"

    def test_empty_task_name_is_none(self, rf):
        f = RecentTasksFilters.from_request(rf.get("/"))
        assert f.task_name is None

    def test_worker(self, rf):
        f = RecentTasksFilters.from_request(rf.get("/", {"worker": "worker1@host"}))
        assert f.worker == "worker1@host"

    def test_whitespace_stripped(self, rf):
        f = RecentTasksFilters.from_request(rf.get("/", {"task_name": "  tasks.add  "}))
        assert f.task_name == "tasks.add"


class TestTaskExecutionStatsFilters:
    def test_default_one_hour(self, rf):
        f = TaskExecutionStatsFilters.from_request(rf.get("/"))
        assert f.date_from is not None
        assert f.date_to is not None
        diff = f.date_to - f.date_from
        assert abs(diff.total_seconds() - 3600) < 5

    def test_hours_all_gives_no_dates(self, rf):
        f = TaskExecutionStatsFilters.from_request(rf.get("/", {"hours": "all"}))
        assert f.date_from is None
        assert f.date_to is None

    def test_custom_hours(self, rf):
        f = TaskExecutionStatsFilters.from_request(rf.get("/", {"hours": "24"}))
        diff = f.date_to - f.date_from
        assert abs(diff.total_seconds() - 24 * 3600) < 5

    def test_hours_zero_uses_one_hour(self, rf):
        f = TaskExecutionStatsFilters.from_request(rf.get("/", {"hours": "0"}))
        diff = f.date_to - f.date_from
        assert abs(diff.total_seconds() - 3600) < 5

    def test_hours_negative_uses_one_hour(self, rf):
        f = TaskExecutionStatsFilters.from_request(rf.get("/", {"hours": "-5"}))
        diff = f.date_to - f.date_from
        assert abs(diff.total_seconds() - 3600) < 5

    def test_hours_invalid_gives_no_dates(self, rf):
        f = TaskExecutionStatsFilters.from_request(rf.get("/", {"hours": "invalid"}))
        assert f.date_from is None
        assert f.date_to is None

    def test_date_params_override_hours(self, rf):
        f = TaskExecutionStatsFilters.from_request(
            rf.get(
                "/",
                {
                    "date_from": "2024-01-01T00:00:00",
                    "date_to": "2024-01-31T23:59:59",
                    "hours": "1",
                },
            )
        )
        assert f.date_from.year == 2024
        assert f.date_from.month == 1
        assert f.date_from.day == 1

    def test_valid_sort(self, rf):
        f = TaskExecutionStatsFilters.from_request(
            rf.get("/", {"sort": "failure_count"})
        )
        assert f.sort_by == "failure_count"

    def test_invalid_sort_defaults_to_total_count(self, rf):
        f = TaskExecutionStatsFilters.from_request(rf.get("/", {"sort": "invalid"}))
        assert f.sort_by == "total_count"

    def test_sort_order_asc(self, rf):
        f = TaskExecutionStatsFilters.from_request(rf.get("/", {"order": "asc"}))
        assert f.sort_order == "asc"

    def test_invalid_sort_order_defaults_to_desc(self, rf):
        f = TaskExecutionStatsFilters.from_request(rf.get("/", {"order": "invalid"}))
        assert f.sort_order == "desc"


class TestWorkerStatsFilters:
    def test_include_offline_true(self, rf):
        f = WorkerStatsFilters.from_request(rf.get("/", {"include_offline": "true"}))
        assert f.include_offline is True

    def test_include_offline_default_false(self, rf):
        f = WorkerStatsFilters.from_request(rf.get("/"))
        assert f.include_offline is False

    def test_include_offline_other_value_is_false(self, rf):
        f = WorkerStatsFilters.from_request(rf.get("/", {"include_offline": "1"}))
        assert f.include_offline is False


class TestTaskResultsFilters:
    def test_defaults(self, rf):
        f = TaskResultsFilters.from_request(rf.get("/"))
        assert f.status is None
        assert f.task_name is None
        assert f.worker is None
        assert f.page == 0
        assert f.page_size == 50

    def test_page_param(self, rf):
        f = TaskResultsFilters.from_request(rf.get("/", {"page": "2"}))
        assert f.page == 2

    def test_page_negative_clamps_to_zero(self, rf):
        f = TaskResultsFilters.from_request(rf.get("/", {"page": "-1"}))
        assert f.page == 0

    def test_page_invalid_defaults_to_zero(self, rf):
        f = TaskResultsFilters.from_request(rf.get("/", {"page": "abc"}))
        assert f.page == 0

    def test_invalid_status_is_none(self, rf):
        f = TaskResultsFilters.from_request(rf.get("/", {"status": "INVALID"}))
        assert f.status is None

    def test_valid_status(self, rf):
        f = TaskResultsFilters.from_request(rf.get("/", {"status": "FAILURE"}))
        assert f.status == "FAILURE"

    def test_date_from_to(self, rf):
        f = TaskResultsFilters.from_request(
            rf.get(
                "/",
                {"date_from": "2024-01-01T00:00:00", "date_to": "2024-01-31T23:59:59"},
            )
        )
        assert f.date_from is not None
        assert f.date_to is not None


class TestGetDateRange:
    def test_valid_dates(self, rf):
        date_from, date_to = get_date_range(
            rf.get(
                "/",
                {"date_from": "2024-01-01T00:00:00", "date_to": "2024-01-31T23:59:59"},
            )
        )
        assert date_from is not None
        assert date_to is not None
        assert date_from.year == 2024

    def test_invalid_date_from_returns_none(self, rf):
        date_from, _ = get_date_range(rf.get("/", {"date_from": "not-a-date"}))
        assert date_from is None

    def test_invalid_date_to_returns_none(self, rf):
        _, date_to = get_date_range(rf.get("/", {"date_to": "not-a-date"}))
        assert date_to is None

    def test_empty_params_return_none(self, rf):
        date_from, date_to = get_date_range(rf.get("/"))
        assert date_from is None
        assert date_to is None
