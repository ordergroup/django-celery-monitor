from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest
import time_machine

# Check if django_celery_results is in INSTALLED_APPS
# We need to do lazy imports because the models will raise RuntimeError if not in INSTALLED_APPS
from django.conf import settings
from django.utils import timezone

from celery_monitor.models import (
    RecentTasksData,
)
from celery_monitor.results_monitor.base import CeleryResultsMonitor

HAS_CELERY_RESULTS = "django_celery_results" in settings.INSTALLED_APPS

if HAS_CELERY_RESULTS:
    from django_celery_results.models import TaskResult

    from celery_monitor.results_monitor.celery_results_mixin import CeleryResultsMixin
    from tests.factories import TaskResultFactory


class TestCeleryResultsMonitor:
    """Test the base CeleryResultsMonitor class."""

    def test_init(self):
        """Test initialization of CeleryResultsMonitor."""
        with (
            patch("celery_monitor.results_monitor.base.is_postgres") as mock_postgres,
            patch(
                "celery_monitor.results_monitor.base.has_django_celery_result"
            ) as mock_has_results,
        ):
            mock_postgres.return_value = True
            mock_has_results.return_value = True

            monitor = CeleryResultsMonitor()

            assert monitor.is_postgres is True
            assert monitor.has_django_celery_result is True

    def test_get_overall_status_counts_empty(self):
        """Test get_overall_status_counts returns empty list by default."""
        monitor = CeleryResultsMonitor()
        result = monitor.get_overall_status_counts()
        assert result == []

    def test_get_last_hour_status_counts_empty(self):
        """Test get_last_hour_status_counts returns empty list by default."""
        monitor = CeleryResultsMonitor()
        result = monitor.get_last_hour_status_counts()
        assert result == []

    def test_get_task_execution_stats_empty(self):
        """Test get_task_execution_stats returns empty list by default."""
        monitor = CeleryResultsMonitor()
        result = monitor.get_task_execution_stats()
        assert result == []

    def test_get_recent_tasks_empty(self):
        """Test get_recent_tasks returns empty data by default."""
        monitor = CeleryResultsMonitor()
        result = monitor.get_recent_tasks()
        assert isinstance(result, RecentTasksData)
        assert result.recent_tasks == []
        assert result.task_names == []
        assert result.workers == []

    @patch("celery_monitor.results_monitor.base.current_app")
    def test_get_worker_stats_with_online_workers(self, mock_app):
        """Test get_worker_stats with online workers."""
        mock_inspect = Mock()
        mock_inspect.ping.return_value = {
            "worker1@host": {"ok": "pong"},
            "worker2@host": {"ok": "pong"},
        }
        mock_inspect.active.return_value = {
            "worker1@host": [{"id": "task1"}, {"id": "task2"}],
            "worker2@host": [],
        }
        mock_app.control.inspect.return_value = mock_inspect

        monitor = CeleryResultsMonitor()
        result = monitor.get_worker_stats()

        assert len(result) == 2
        assert result[0].name == "worker1@host"
        assert result[0].status == "online"
        assert result[0].active_tasks == 2
        assert result[1].name == "worker2@host"
        assert result[1].status == "online"
        assert result[1].active_tasks == 0

    @patch("celery_monitor.results_monitor.base.current_app")
    def test_get_worker_stats_no_workers(self, mock_app):
        """Test get_worker_stats with no workers."""
        mock_inspect = Mock()
        mock_inspect.ping.return_value = None
        mock_inspect.active.return_value = None
        mock_app.control.inspect.return_value = mock_inspect

        monitor = CeleryResultsMonitor()
        result = monitor.get_worker_stats()

        assert result == []

    @patch("celery_monitor.results_monitor.base.current_app")
    def test_get_worker_stats_exception_handling(self, mock_app):
        """Test get_worker_stats handles exceptions gracefully."""
        mock_app.control.inspect.side_effect = Exception("Connection error")

        monitor = CeleryResultsMonitor()
        result = monitor.get_worker_stats()

        assert result == []


@pytest.mark.skipif(
    not HAS_CELERY_RESULTS, reason="django_celery_results not in INSTALLED_APPS"
)
class TestCeleryResultsMixin:
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

        mixin = CeleryResultsMixin()
        mixin.is_postgres = False
        mixin.has_django_celery_result = True

        result = mixin.get_overall_status_counts()

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

        mixin = CeleryResultsMixin()

        result = mixin.get_last_hour_status_counts()

        assert len(result) == 3  # total + SUCCESS + FAILURE
        assert result[0].status == "total"
        assert result[0].count == 5

        status_counts = {item.status: item.count for item in result[1:]}
        assert status_counts["SUCCESS"] == 3
        assert status_counts["FAILURE"] == 2

    @pytest.mark.django_db(transaction=True)
    @patch("celery_monitor.results_monitor.base.current_app")
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
        mock_app.control.inspect.return_value = mock_inspect

        # The mixin needs to be used properly by inheriting from the base class
        # Create a test class that properly inherits
        class TestMonitor(CeleryResultsMixin, CeleryResultsMonitor):
            pass

        monitor = TestMonitor()
        monitor.is_postgres = False
        monitor.has_django_celery_result = True

        result = monitor.get_worker_stats()

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

        mixin = CeleryResultsMixin()

        result = mixin.get_recent_tasks(status="SUCCESS")

        assert isinstance(result, RecentTasksData)
        assert len(result.recent_tasks) == 3
        assert all(task.status == "SUCCESS" for task in result.recent_tasks)
        # Verify the task IDs match
        task_ids = {task.task_id for task in result.recent_tasks}
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

        mixin = CeleryResultsMixin()
        result = mixin.get_recent_tasks()

        assert len(result.recent_tasks) == 1
        assert result.recent_tasks[0].task_id == "test-task-123"
        assert result.recent_tasks[0].execution_time is not None
        assert (
            9.0 <= result.recent_tasks[0].execution_time <= 11.0
        )  # Should be around 10 seconds

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

        mixin = CeleryResultsMixin()
        result = mixin.get_recent_tasks()

        assert len(result.recent_tasks) == 1
        assert result.recent_tasks[0].task_id == "test-task-456"
        assert result.recent_tasks[0].execution_time is None

    @pytest.mark.django_db(transaction=True)
    def test_get_recent_tasks_includes_task_names_and_workers(self):
        """Test get_recent_tasks returns task names and workers lists."""
        TaskResultFactory(task_name="tasks.add", worker="worker1@host")
        TaskResultFactory(task_name="tasks.process", worker="worker2@host")
        TaskResultFactory(task_name="tasks.add", worker="worker1@host")  # duplicate

        mixin = CeleryResultsMixin()
        result = mixin.get_recent_tasks()

        assert len(result.task_names) == 2
        assert "tasks.add" in result.task_names
        assert "tasks.process" in result.task_names

        assert len(result.workers) == 2
        assert "worker1@host" in result.workers
        assert "worker2@host" in result.workers

    @pytest.mark.django_db(transaction=True)
    def test_get_recent_tasks_with_task_name_filter(self):
        """Test get_recent_tasks with task_name filter."""
        TaskResultFactory.create_batch(3, task_name="tasks.add")
        TaskResultFactory.create_batch(2, task_name="tasks.process")

        mixin = CeleryResultsMixin()
        result = mixin.get_recent_tasks(task_name="tasks.add")

        assert len(result.recent_tasks) == 3
        assert all(task.task_name == "tasks.add" for task in result.recent_tasks)

    @pytest.mark.django_db(transaction=True)
    def test_get_recent_tasks_with_worker_filter(self):
        """Test get_recent_tasks with worker filter."""
        TaskResultFactory.create_batch(4, worker="worker1@host")
        TaskResultFactory.create_batch(2, worker="worker2@host")

        mixin = CeleryResultsMixin()
        result = mixin.get_recent_tasks(worker="worker1@host")

        assert len(result.recent_tasks) == 4
        assert all(task.worker == "worker1@host" for task in result.recent_tasks)

    @pytest.mark.django_db(transaction=True)
    def test_get_recent_tasks_limit(self):
        """Test get_recent_tasks respects the limit parameter."""
        TaskResultFactory.create_batch(50, status="SUCCESS")

        mixin = CeleryResultsMixin()
        result = mixin.get_recent_tasks()

        # Default limit should be 50
        assert len(result.recent_tasks) <= 50

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

        mixin = CeleryResultsMixin()
        result = mixin.get_task_execution_stats(hours=1)

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

        mixin = CeleryResultsMixin()
        result = mixin.get_task_execution_stats(hours=1)

        assert len(result) == 1
        assert result[0].task_name == "tasks.add"
        assert result[0].total_count == 5  # Only recent tasks counted

    @pytest.mark.django_db(transaction=True)
    def test_get_task_execution_stats_exception_handling(self):
        """Test get_task_execution_stats handles exceptions."""
        mixin = CeleryResultsMixin()

        with patch.object(TaskResult.objects, "all") as mock_all:
            mock_all.side_effect = Exception("Database error")

            result = mixin.get_task_execution_stats()

            assert result == []
