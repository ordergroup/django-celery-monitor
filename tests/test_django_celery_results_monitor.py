from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest
import time_machine
from django.conf import settings
from django.utils import timezone

HAS_CELERY_RESULTS = "django_celery_results" in settings.INSTALLED_APPS

if HAS_CELERY_RESULTS:
    from django_celery_results.models import TaskResult

    from celery_monitor.results_monitor.django_celery_results import (
        DjangoCeleryResultsMonitor,
    )
    from tests.factories import TaskResultFactory


@pytest.mark.skipif(
    not HAS_CELERY_RESULTS, reason="django_celery_results not in INSTALLED_APPS"
)
class TestDjangoCeleryResultsMonitor:
    """Test the CeleryResultsMixin class.

    Note: These tests require django-celery-results to be installed.
    Run with: pytest --ds=tests.settings_with_celery_results
    """

    @pytest.mark.django_db(transaction=True)
    def test_get_overall_status_counts_without_postgres(self):
        """Test get_overall_status_counts without PostgreSQL."""
        # Create test data
        TaskResultFactory.create_batch(5, status="SUCCESS")
        TaskResultFactory.create_batch(2, status="FAILURE")
        TaskResultFactory.create_batch(1, status="PENDING")

        monitor = DjangoCeleryResultsMonitor()
        monitor.is_postgres = False

        result = monitor.get_overall_status_counts()

        assert len(result) == 4  # total + SUCCESS + FAILURE + PENDING
        assert result[0].status == "total"
        assert result[0].count == 8
        # Results are ordered by status
        status_counts = {item.status: item.count for item in result[1:]}
        assert status_counts["SUCCESS"] == 5
        assert status_counts["FAILURE"] == 2
        assert status_counts["PENDING"] == 1

    @pytest.mark.django_db(transaction=True)
    @time_machine.travel("2024-01-15 12:00:00+00:00", tick=False)
    def test_get_last_hour_status_counts(self):
        """Test get_last_hour_status_counts."""
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

        for _ in range(3):
            task = TaskResultFactory.create(status="SUCCESS")
            TaskResult.objects.filter(pk=task.pk).update(
                date_created=now - timedelta(minutes=30),
                date_done=now - timedelta(minutes=30),
            )

        for _ in range(2):
            task = TaskResultFactory.create(status="FAILURE")
            TaskResult.objects.filter(pk=task.pk).update(
                date_created=now - timedelta(minutes=15),
                date_done=now - timedelta(minutes=15),
            )

        for _ in range(5):
            task = TaskResultFactory.create(status="SUCCESS")
            TaskResult.objects.filter(pk=task.pk).update(
                date_created=now - timedelta(hours=2),
                date_done=now - timedelta(hours=2),
            )

        monitor = DjangoCeleryResultsMonitor()

        result = monitor.get_last_hour_status_counts()

        assert len(result) == 3  # total + SUCCESS + FAILURE
        assert result[0].status == "total"
        assert result[0].count == 5

        status_counts = {item.status: item.count for item in result[1:]}
        assert status_counts["SUCCESS"] == 3
        assert status_counts["FAILURE"] == 2

    @pytest.mark.django_db(transaction=True)
    @patch("celery_monitor.results_monitor.workers_results.current_app")
    def test_get_worker_stats_with_offline_workers(self, mock_app):
        """Test get_worker_stats includes offline workers from database."""
        # Create tasks with workers
        TaskResultFactory.create_batch(2, worker="worker1@host")
        TaskResultFactory.create_batch(3, worker="worker2@host")

        # Mock only worker1 as online
        mock_inspect = Mock()
        mock_inspect.ping.return_value = {"worker1@host": {"ok": "pong"}}
        mock_inspect.active.return_value = {
            "worker1@host": [{"id": "task1"}, {"id": "task2"}]
        }
        mock_inspect.reserved.return_value = {"worker1@host": []}
        mock_inspect.active_queues.return_value = {"worker1@host": [{"name": "celery"}]}
        mock_app.control.inspect.return_value = mock_inspect

        monitor = DjangoCeleryResultsMonitor()

        result = monitor.get_worker_stats(include_offline=True)

        assert len(result) == 2
        assert result[0].name == "worker1@host"
        assert result[0].status == "online"
        assert result[0].active_tasks == 2
        assert result[1].name == "worker2@host"
        assert result[1].status == "offline"
        assert result[1].active_tasks == 0

    @pytest.mark.django_db(transaction=True)
    def test_get_recent_tasks_with_filters(self):
        """Test get_recent_tasks with status filter."""
        # Create tasks with different statuses
        success_tasks = TaskResultFactory.create_batch(3, status="SUCCESS")
        TaskResultFactory.create_batch(2, status="FAILURE")

        monitor = DjangoCeleryResultsMonitor()

        result = monitor.get_recent_tasks(status="SUCCESS")

        assert isinstance(result, list)
        assert len(result) == 3
        assert all(task.status == "SUCCESS" for task in result)
        # Verify the task IDs match
        task_ids = {task.task_id for task in result}
        expected_ids = {task.task_id for task in success_tasks}
        assert task_ids == expected_ids

    @pytest.mark.django_db(transaction=True)
    def test_get_recent_tasks_execution_time_calculation(self):
        """Test execution time is calculated correctly."""
        start_time = timezone.now() - timedelta(seconds=10)
        end_time = timezone.now()

        TaskResultFactory(
            task_id="test-task-123",
            status="SUCCESS",
            date_started=start_time,
            date_done=end_time,
        )

        monitor = DjangoCeleryResultsMonitor()
        result = monitor.get_recent_tasks()

        assert len(result) == 1
        assert result[0].task_id == "test-task-123"
        assert result[0].execution_time is not None
        assert 9.0 <= result[0].execution_time <= 11.0  # Should be around 10 seconds

    @pytest.mark.django_db(transaction=True)
    def test_get_recent_tasks_no_execution_time(self):
        """Test execution time is None when dates are missing."""
        TaskResultFactory(
            task_id="test-task-456",
            task_name="tasks.pending_task",
            status="PENDING",
            worker=None,
            date_started=None,
            date_done=None,
        )

        monitor = DjangoCeleryResultsMonitor()
        result = monitor.get_recent_tasks()

        assert len(result) == 1
        assert result[0].task_id == "test-task-456"
        assert result[0].execution_time is None

    @pytest.mark.django_db(transaction=True)
    def test_get_recent_tasks_includes_task_names_and_workers(self):
        """Test get_recent_tasks returns task names and workers lists."""
        TaskResultFactory(task_name="tasks.add", worker="worker1@host")
        TaskResultFactory(task_name="tasks.process", worker="worker2@host")
        TaskResultFactory(task_name="tasks.add", worker="worker1@host")  # duplicate

        monitor = DjangoCeleryResultsMonitor()
        monitor.get_recent_tasks()
        task_names = monitor.get_tasks_names()
        workers = monitor.get_workers_names()

        assert len(task_names) == 2
        assert "tasks.add" in task_names
        assert "tasks.process" in task_names

        assert len(workers) == 2
        assert "worker1@host" in workers
        assert "worker2@host" in workers

    @pytest.mark.django_db(transaction=True)
    def test_get_recent_tasks_with_task_name_filter(self):
        """Test get_recent_tasks with task_name filter."""
        TaskResultFactory.create_batch(3, task_name="tasks.add")
        TaskResultFactory.create_batch(2, task_name="tasks.process")

        monitor = DjangoCeleryResultsMonitor()
        result = monitor.get_recent_tasks(task_name="tasks.add")

        assert len(result) == 3
        assert all(task.task_name == "tasks.add" for task in result)

    @pytest.mark.django_db(transaction=True)
    def test_get_recent_tasks_with_worker_filter(self):
        """Test get_recent_tasks with worker filter."""
        TaskResultFactory.create_batch(4, worker="worker1@host")
        TaskResultFactory.create_batch(2, worker="worker2@host")

        monitor = DjangoCeleryResultsMonitor()
        result = monitor.get_recent_tasks(worker="worker1@host")

        assert len(result) == 4
        assert all(task.worker == "worker1@host" for task in result)

    @pytest.mark.django_db(transaction=True)
    def test_get_recent_tasks_limit(self):
        """Test get_recent_tasks respects the limit parameter."""
        TaskResultFactory.create_batch(50, status="SUCCESS")

        monitor = DjangoCeleryResultsMonitor()
        result = monitor.get_recent_tasks()

        # Default limit should be 50
        assert len(result) <= 50

    @pytest.mark.django_db(transaction=True)
    @time_machine.travel("2024-01-15 12:00:00+00:00", tick=False)
    def test_get_task_execution_stats(self):
        """Test get_task_execution_stats."""
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

        for _ in range(8):
            task = TaskResultFactory.create(
                task_name="tasks.add",
                status="SUCCESS",
                date_started=now - timedelta(minutes=30, seconds=5),
            )
            TaskResult.objects.filter(pk=task.pk).update(
                date_created=now - timedelta(minutes=30, seconds=6),
                date_done=now - timedelta(minutes=30),
            )

        for _ in range(2):
            task = TaskResultFactory.create(
                task_name="tasks.add",
                status="FAILURE",
                date_started=now - timedelta(minutes=30, seconds=5),
            )
            TaskResult.objects.filter(pk=task.pk).update(
                date_created=now - timedelta(minutes=30, seconds=6),
                date_done=now - timedelta(minutes=30),
            )

        for _ in range(5):
            task = TaskResultFactory.create(
                task_name="tasks.process",
                status="SUCCESS",
                date_started=now - timedelta(minutes=30, seconds=10),
            )
            TaskResult.objects.filter(pk=task.pk).update(
                date_created=now - timedelta(minutes=30, seconds=11),
                date_done=now - timedelta(minutes=30),
            )

        monitor = DjangoCeleryResultsMonitor()
        hour_ago = timezone.now() - timedelta(hours=1)
        result = monitor.get_task_execution_stats(date_from=hour_ago)

        assert len(result) == 2

        # Find stats for each task
        add_stats = next(s for s in result if s.task_name == "tasks.add")
        process_stats = next(s for s in result if s.task_name == "tasks.process")

        assert add_stats.total_count == 10
        assert add_stats.success_count == 8
        assert add_stats.failure_count == 2
        assert add_stats.avg_runtime is not None
        assert 4.5 <= add_stats.avg_runtime <= 5.5  # Around 5 seconds

        assert process_stats.total_count == 5
        assert process_stats.success_count == 5
        assert process_stats.failure_count == 0
        assert process_stats.avg_runtime is not None
        assert 9.5 <= process_stats.avg_runtime <= 10.5  # Around 10 seconds

    @pytest.mark.django_db(transaction=True)
    @time_machine.travel("2024-01-15 12:00:00+00:00", tick=False)
    def test_get_task_execution_stats_time_filter(self):
        """Test get_task_execution_stats filters by time correctly."""

        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

        for _ in range(5):
            task = TaskResultFactory.create(
                task_name="tasks.add",
                status="SUCCESS",
                date_started=now - timedelta(minutes=30, seconds=10),
            )
            TaskResult.objects.filter(pk=task.pk).update(
                date_created=now - timedelta(minutes=31),
                date_done=now - timedelta(minutes=30),
            )

        for _ in range(3):
            task = TaskResultFactory.create(
                task_name="tasks.add",
                status="SUCCESS",
                date_started=now - timedelta(hours=2, seconds=5),
            )
            TaskResult.objects.filter(pk=task.pk).update(
                date_created=now - timedelta(hours=2, seconds=10),
                date_done=now - timedelta(hours=2),
            )

        monitor = DjangoCeleryResultsMonitor()
        hour_ago = timezone.now() - timedelta(hours=1)
        result = monitor.get_task_execution_stats(date_from=hour_ago)

        assert len(result) == 1
        assert result[0].task_name == "tasks.add"
        assert result[0].total_count == 5  # Only recent tasks counted

    @pytest.mark.django_db(transaction=True)
    def test_get_task_execution_stats_exception_handling(self):
        """Test get_task_execution_stats handles exceptions."""
        monitor = DjangoCeleryResultsMonitor()

        with patch.object(TaskResult.objects, "all") as mock_all:
            mock_all.side_effect = Exception("Database error")

            result = monitor.get_task_execution_stats()

            assert result == []

    @pytest.mark.django_db(transaction=True)
    @time_machine.travel("2024-01-15 12:00:00+00:00", tick=False)
    def test_get_task_execution_stats_with_min_max_runtime(self):
        """Test get_task_execution_stats includes min and max runtime."""
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

        task1 = TaskResultFactory.create(
            task_name="tasks.add",
            status="SUCCESS",
            date_started=now - timedelta(minutes=30, seconds=2),
        )
        TaskResult.objects.filter(pk=task1.pk).update(
            date_created=now - timedelta(minutes=30, seconds=3),
            date_done=now - timedelta(minutes=30),
        )

        task2 = TaskResultFactory.create(
            task_name="tasks.add",
            status="SUCCESS",
            date_started=now - timedelta(minutes=30, seconds=10),
        )
        TaskResult.objects.filter(pk=task2.pk).update(
            date_created=now - timedelta(minutes=30, seconds=11),
            date_done=now - timedelta(minutes=30),
        )

        task3 = TaskResultFactory.create(
            task_name="tasks.add",
            status="SUCCESS",
            date_started=now - timedelta(minutes=30, seconds=5),
        )
        TaskResult.objects.filter(pk=task3.pk).update(
            date_created=now - timedelta(minutes=30, seconds=6),
            date_done=now - timedelta(minutes=30),
        )

        monitor = DjangoCeleryResultsMonitor()
        hour_ago = timezone.now() - timedelta(hours=1)
        result = monitor.get_task_execution_stats(date_from=hour_ago)

        assert len(result) == 1
        stats = result[0]

        assert stats.task_name == "tasks.add"
        assert stats.total_count == 3
        assert stats.success_count == 3
        assert stats.failure_count == 0

        assert stats.min_runtime is not None
        assert 1.5 <= stats.min_runtime <= 2.5

        assert stats.avg_runtime is not None
        assert 5.0 <= stats.avg_runtime <= 6.5

        assert stats.max_runtime is not None
        assert 9.5 <= stats.max_runtime <= 10.5

    @pytest.mark.django_db(transaction=True)
    @time_machine.travel("2024-01-15 12:00:00+00:00", tick=False)
    def test_get_task_execution_stats_min_max_only_success_tasks(self):
        """Test min/max runtime only includes successful tasks."""
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

        task1 = TaskResultFactory.create(
            task_name="tasks.add",
            status="SUCCESS",
            date_started=now - timedelta(minutes=30, seconds=5),
        )
        TaskResult.objects.filter(pk=task1.pk).update(
            date_created=now - timedelta(minutes=30, seconds=6),
            date_done=now - timedelta(minutes=30),
        )

        task2 = TaskResultFactory.create(
            task_name="tasks.add",
            status="FAILURE",
            date_started=now - timedelta(minutes=30, seconds=100),
        )
        TaskResult.objects.filter(pk=task2.pk).update(
            date_created=now - timedelta(minutes=30, seconds=101),
            date_done=now - timedelta(minutes=30),
        )

        task3 = TaskResultFactory.create(
            task_name="tasks.add",
            status="SUCCESS",
            date_started=now - timedelta(minutes=30, seconds=10),
        )
        TaskResult.objects.filter(pk=task3.pk).update(
            date_created=now - timedelta(minutes=30, seconds=11),
            date_done=now - timedelta(minutes=30),
        )

        monitor = DjangoCeleryResultsMonitor()
        hour_ago = timezone.now() - timedelta(hours=1)
        result = monitor.get_task_execution_stats(date_from=hour_ago)

        assert len(result) == 1
        stats = result[0]

        assert stats.total_count == 3
        assert stats.success_count == 2
        assert stats.failure_count == 1

        assert stats.min_runtime is not None
        assert 4.5 <= stats.min_runtime <= 5.5

        assert stats.max_runtime is not None
        assert 9.5 <= stats.max_runtime <= 10.5

    @pytest.mark.django_db(transaction=True)
    @time_machine.travel("2024-01-15 12:00:00+00:00", tick=False)
    def test_get_task_execution_stats_min_max_none_when_no_success(self):
        """Test min/max runtime are None when there are no successful tasks."""
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

        for _ in range(3):
            task = TaskResultFactory.create(
                task_name="tasks.add",
                status="FAILURE",
                date_started=now - timedelta(minutes=30, seconds=5),
            )
            TaskResult.objects.filter(pk=task.pk).update(
                date_created=now - timedelta(minutes=30, seconds=6),
                date_done=now - timedelta(minutes=30),
            )

        monitor = DjangoCeleryResultsMonitor()
        hour_ago = timezone.now() - timedelta(hours=1)
        result = monitor.get_task_execution_stats(date_from=hour_ago)

        assert len(result) == 1
        stats = result[0]

        assert stats.total_count == 3
        assert stats.success_count == 0
        assert stats.failure_count == 3
        assert stats.avg_runtime is None
        assert stats.min_runtime is None
        assert stats.max_runtime is None

    @pytest.mark.django_db(transaction=True)
    @time_machine.travel("2024-01-15 12:00:00+00:00", tick=False)
    def test_get_task_execution_stats_min_max_none_when_missing_dates(self):
        """Test min/max runtime are None when date_started is missing."""
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

        task1 = TaskResultFactory.create(
            task_name="tasks.add",
            status="SUCCESS",
            date_started=None,
        )
        TaskResult.objects.filter(pk=task1.pk).update(
            date_created=now - timedelta(minutes=30),
            date_done=now - timedelta(minutes=30),
            date_started=None,
        )

        task2 = TaskResultFactory.create(
            task_name="tasks.add",
            status="SUCCESS",
            date_started=None,
        )
        TaskResult.objects.filter(pk=task2.pk).update(
            date_created=now - timedelta(minutes=30, seconds=6),
            date_done=now - timedelta(minutes=30),
            date_started=None,
        )

        monitor = DjangoCeleryResultsMonitor()
        hour_ago = timezone.now() - timedelta(hours=1)
        result = monitor.get_task_execution_stats(date_from=hour_ago)

        assert len(result) == 1
        stats = result[0]

        assert stats.total_count == 2
        assert stats.success_count == 2
        assert stats.avg_runtime is None
        assert stats.min_runtime is None
        assert stats.max_runtime is None

    @pytest.mark.django_db(transaction=True)
    @time_machine.travel("2024-01-15 12:00:00+00:00", tick=False)
    def test_get_task_execution_stats_sort_by_min_runtime(self):
        """Test sorting by min_runtime."""
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

        task1 = TaskResultFactory.create(
            task_name="tasks.fast",
            status="SUCCESS",
            date_started=now - timedelta(minutes=30, seconds=1),
        )
        TaskResult.objects.filter(pk=task1.pk).update(
            date_created=now - timedelta(minutes=30, seconds=2),
            date_done=now - timedelta(minutes=30),
        )

        task2 = TaskResultFactory.create(
            task_name="tasks.slow",
            status="SUCCESS",
            date_started=now - timedelta(minutes=30, seconds=10),
        )
        TaskResult.objects.filter(pk=task2.pk).update(
            date_created=now - timedelta(minutes=30, seconds=11),
            date_done=now - timedelta(minutes=30),
        )

        monitor = DjangoCeleryResultsMonitor()
        hour_ago = timezone.now() - timedelta(hours=1)
        result = monitor.get_task_execution_stats(
            date_from=hour_ago, sort_by="min_runtime", sort_order="asc"
        )

        assert len(result) == 2
        assert result[0].task_name == "tasks.fast"
        assert result[1].task_name == "tasks.slow"

        result_desc = monitor.get_task_execution_stats(
            date_from=hour_ago, sort_by="min_runtime", sort_order="desc"
        )
        assert result_desc[0].task_name == "tasks.slow"
        assert result_desc[1].task_name == "tasks.fast"

    @pytest.mark.django_db(transaction=True)
    @time_machine.travel("2024-01-15 12:00:00+00:00", tick=False)
    def test_get_task_execution_stats_sort_by_max_runtime(self):
        """Test sorting by max_runtime."""
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

        task1 = TaskResultFactory.create(
            task_name="tasks.fast",
            status="SUCCESS",
            date_started=now - timedelta(minutes=30, seconds=5),
        )
        TaskResult.objects.filter(pk=task1.pk).update(
            date_created=now - timedelta(minutes=30, seconds=6),
            date_done=now - timedelta(minutes=30),
        )

        task2 = TaskResultFactory.create(
            task_name="tasks.slow",
            status="SUCCESS",
            date_started=now - timedelta(minutes=30, seconds=20),
        )
        TaskResult.objects.filter(pk=task2.pk).update(
            date_created=now - timedelta(minutes=30, seconds=21),
            date_done=now - timedelta(minutes=30),
        )

        monitor = DjangoCeleryResultsMonitor()
        hour_ago = timezone.now() - timedelta(hours=1)
        result = monitor.get_task_execution_stats(
            date_from=hour_ago, sort_by="max_runtime", sort_order="asc"
        )

        assert len(result) == 2
        assert result[0].task_name == "tasks.fast"
        assert result[1].task_name == "tasks.slow"

        result_desc = monitor.get_task_execution_stats(
            date_from=hour_ago, sort_by="max_runtime", sort_order="desc"
        )
        assert result_desc[0].task_name == "tasks.slow"
        assert result_desc[1].task_name == "tasks.fast"

    @pytest.mark.django_db(transaction=True)
    @time_machine.travel("2024-01-15 12:00:00+00:00", tick=False)
    def test_get_task_execution_stats_multiple_tasks_with_varying_runtimes(self):
        """Test stats with multiple tasks having varying execution times."""
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

        runtimes = [2, 5, 3, 8, 4]
        for runtime in runtimes:
            task = TaskResultFactory.create(
                task_name="tasks.variable",
                status="SUCCESS",
                date_started=now - timedelta(minutes=30, seconds=runtime),
            )
            TaskResult.objects.filter(pk=task.pk).update(
                date_created=now - timedelta(minutes=30, seconds=runtime + 1),
                date_done=now - timedelta(minutes=30),
            )

        monitor = DjangoCeleryResultsMonitor()
        hour_ago = timezone.now() - timedelta(hours=1)
        result = monitor.get_task_execution_stats(date_from=hour_ago)

        assert len(result) == 1
        stats = result[0]

        assert stats.task_name == "tasks.variable"
        assert stats.total_count == 5
        assert stats.success_count == 5

        assert stats.min_runtime is not None
        assert 1.5 <= stats.min_runtime <= 2.5

        assert stats.avg_runtime is not None
        assert 4.0 <= stats.avg_runtime <= 5.0

        assert stats.max_runtime is not None
        assert 7.5 <= stats.max_runtime <= 8.5

    @pytest.mark.django_db(transaction=True)
    @time_machine.travel("2024-01-15 12:00:00+00:00", tick=False)
    def test_get_task_execution_stats_with_custom_date_range(self):
        """Test get_task_execution_stats with custom date range."""
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

        for _i in range(3):
            task = TaskResultFactory.create(
                task_name="tasks.in_range",
                status="SUCCESS",
                date_started=now - timedelta(days=2, seconds=5),
            )
            TaskResult.objects.filter(pk=task.pk).update(
                date_created=now - timedelta(days=2, seconds=6),
                date_done=now - timedelta(days=2),
            )

        for _i in range(2):
            task = TaskResultFactory.create(
                task_name="tasks.out_of_range",
                status="SUCCESS",
                date_started=now - timedelta(days=5, seconds=5),
            )
            TaskResult.objects.filter(pk=task.pk).update(
                date_created=now - timedelta(days=5, seconds=6),
                date_done=now - timedelta(days=5),
            )

        monitor = DjangoCeleryResultsMonitor()
        date_from = now - timedelta(days=3)
        date_to = now - timedelta(days=1)

        result = monitor.get_task_execution_stats(date_from=date_from, date_to=date_to)

        assert len(result) == 1
        assert result[0].task_name == "tasks.in_range"
        assert result[0].total_count == 3

    @pytest.mark.django_db(transaction=True)
    @time_machine.travel("2024-01-15 12:00:00+00:00", tick=False)
    def test_get_task_execution_stats_custom_date_range_with_naive_datetime(self):
        """Test custom date range handles naive datetime correctly."""
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

        for _i in range(2):
            task = TaskResultFactory.create(
                task_name="tasks.test",
                status="SUCCESS",
                date_started=now - timedelta(hours=6, seconds=5),
            )
            TaskResult.objects.filter(pk=task.pk).update(
                date_created=now - timedelta(hours=6, seconds=6),
                date_done=now - timedelta(hours=6),
            )

        monitor = DjangoCeleryResultsMonitor()
        date_from = datetime.fromisoformat("2024-01-15T00:00:00")
        date_to = datetime.fromisoformat("2024-01-15T23:59:59")

        result = monitor.get_task_execution_stats(date_from=date_from, date_to=date_to)

        assert len(result) == 1
        assert result[0].task_name == "tasks.test"
        assert result[0].total_count == 2

    @pytest.mark.django_db(transaction=True)
    @time_machine.travel("2024-01-15 12:00:00+00:00", tick=False)
    def test_get_task_execution_stats_custom_date_range_with_sorting(self):
        """Test custom date range works with sorting."""
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=ZoneInfo("UTC"))

        task1 = TaskResultFactory.create(
            task_name="tasks.fast",
            status="SUCCESS",
            date_started=now - timedelta(days=1, seconds=2),
        )
        TaskResult.objects.filter(pk=task1.pk).update(
            date_created=now - timedelta(days=1, seconds=3),
            date_done=now - timedelta(days=1),
        )

        task2 = TaskResultFactory.create(
            task_name="tasks.slow",
            status="SUCCESS",
            date_started=now - timedelta(days=1, seconds=10),
        )
        TaskResult.objects.filter(pk=task2.pk).update(
            date_created=now - timedelta(days=1, seconds=11),
            date_done=now - timedelta(days=1),
        )

        monitor = DjangoCeleryResultsMonitor()
        date_from = now - timedelta(days=2)
        date_to = now

        result = monitor.get_task_execution_stats(
            date_from=date_from,
            date_to=date_to,
            sort_by="min_runtime",
            sort_order="asc",
        )

        assert len(result) == 2
        assert result[0].task_name == "tasks.fast"
        assert result[1].task_name == "tasks.slow"

    @pytest.mark.django_db(transaction=True)
    def test_get_task_detail_found(self):
        TaskResultFactory(
            task_id="detail-task-1",
            task_name="tasks.add",
            status="SUCCESS",
            worker="worker1@host",
        )

        monitor = DjangoCeleryResultsMonitor()
        result = monitor.get_task_detail("detail-task-1")

        assert result is not None
        assert result.task_id == "detail-task-1"
        assert result.task_name == "tasks.add"
        assert result.status == "SUCCESS"
        assert result.worker == "worker1@host"

    @pytest.mark.django_db(transaction=True)
    def test_get_task_detail_not_found_falls_back_to_workers_monitor(self):
        monitor = DjangoCeleryResultsMonitor()
        with patch.object(
            monitor.workers_monitor, "get_task_detail", return_value=None
        ):
            result = monitor.get_task_detail("nonexistent-id")
        assert result is None

    @pytest.mark.django_db(transaction=True)
    def test_get_tasks_returns_paginated_results(self):
        TaskResultFactory.create_batch(10, status="SUCCESS")

        monitor = DjangoCeleryResultsMonitor()
        result = monitor.get_tasks(page=0, page_size=5)

        assert result.total == 10
        assert len(result.tasks) == 5

    @pytest.mark.django_db(transaction=True)
    def test_get_tasks_second_page(self):
        TaskResultFactory.create_batch(10, status="SUCCESS")

        monitor = DjangoCeleryResultsMonitor()
        result = monitor.get_tasks(page=1, page_size=5)

        assert result.total == 10
        assert len(result.tasks) == 5

    @pytest.mark.django_db(transaction=True)
    def test_get_tasks_filter_by_status(self):
        TaskResultFactory.create_batch(3, status="SUCCESS")
        TaskResultFactory.create_batch(2, status="FAILURE")

        monitor = DjangoCeleryResultsMonitor()
        result = monitor.get_tasks(status="SUCCESS")

        assert result.total == 3
        assert all(t.status == "SUCCESS" for t in result.tasks)

    @pytest.mark.django_db(transaction=True)
    def test_get_tasks_filter_by_task_name(self):
        TaskResultFactory.create_batch(3, task_name="tasks.add")
        TaskResultFactory.create_batch(2, task_name="tasks.process")

        monitor = DjangoCeleryResultsMonitor()
        result = monitor.get_tasks(task_name="tasks.add")

        assert result.total == 3
        assert all(t.task_name == "tasks.add" for t in result.tasks)

    @pytest.mark.django_db(transaction=True)
    def test_get_tasks_filter_by_worker(self):
        TaskResultFactory.create_batch(4, worker="worker1@host")
        TaskResultFactory.create_batch(2, worker="worker2@host")

        monitor = DjangoCeleryResultsMonitor()
        result = monitor.get_tasks(worker="worker1@host")

        assert result.total == 4
        assert all(t.worker == "worker1@host" for t in result.tasks)

    @pytest.mark.django_db(transaction=True)
    def test_get_tasks_empty(self):
        monitor = DjangoCeleryResultsMonitor()
        result = monitor.get_tasks()

        assert result.total == 0
        assert result.tasks == []
