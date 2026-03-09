from unittest.mock import Mock, patch

from celery_monitor.models import (
    ReservedTask,
)
from celery_monitor.results_monitor.base import CeleryResultsMonitor
from celery_monitor.results_monitor.workers_results import WorkersCeleryResultsMonitor


class TestCeleryResultsMonitor:
    """Test the base CeleryResultsMonitor class."""

    def test_init(self):
        """Test instantiation of WorkersCeleryResultsMonitor."""
        monitor = WorkersCeleryResultsMonitor()
        assert isinstance(monitor, CeleryResultsMonitor)

    def test_get_overall_status_counts_empty(self):
        """Test get_overall_status_counts returns empty list by default."""
        monitor = WorkersCeleryResultsMonitor()
        result = monitor.get_overall_status_counts()
        assert result == []

    def test_get_last_hour_status_counts_empty(self):
        """Test get_last_hour_status_counts returns empty list by default."""
        monitor = WorkersCeleryResultsMonitor()
        result = monitor.get_last_hour_status_counts()
        assert result == []

    def test_get_task_execution_stats_empty(self):
        """Test get_task_execution_stats returns empty list by default."""
        monitor = WorkersCeleryResultsMonitor()
        result = monitor.get_task_execution_stats()
        assert result == []

    def test_get_recent_tasks_empty(self):
        """Test get_recent_tasks returns empty list by default."""
        monitor = WorkersCeleryResultsMonitor()
        result = monitor.get_recent_tasks()
        assert isinstance(result, list)
        assert result == []

    @patch("celery_monitor.results_monitor.workers_results.current_app")
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
        mock_inspect.reserved.return_value = {
            "worker1@host": [
                {
                    "id": "reserved-task-1",
                    "name": "tasks.email",
                    "args": [],
                    "kwargs": {},
                },
                {
                    "id": "reserved-task-2",
                    "name": "tasks.report",
                    "args": [42],
                    "kwargs": {},
                },
            ],
            "worker2@host": [],
        }
        mock_inspect.active_queues.return_value = {
            "worker1@host": [{"name": "celery"}, {"name": "high_priority"}],
            "worker2@host": [{"name": "celery"}],
        }
        mock_app.control.inspect.return_value = mock_inspect

        monitor = WorkersCeleryResultsMonitor()
        result = monitor.get_worker_stats()

        assert len(result) == 2
        assert result[0].name == "worker1@host"
        assert result[0].status == "online"
        assert result[0].active_tasks == 2
        assert result[0].reserved_tasks == 2
        assert result[0].queues == ["celery", "high_priority"]
        assert result[1].name == "worker2@host"
        assert result[1].status == "online"
        assert result[1].active_tasks == 0
        assert result[1].reserved_tasks == 0
        assert result[1].queues == ["celery"]

    @patch("celery_monitor.results_monitor.workers_results.current_app")
    def test_get_worker_stats_no_workers(self, mock_app):
        """Test get_worker_stats with no workers."""
        mock_inspect = Mock()
        mock_inspect.ping.return_value = None
        mock_inspect.active.return_value = None
        mock_inspect.reserved.return_value = None
        mock_inspect.active_queues.return_value = None
        mock_app.control.inspect.return_value = mock_inspect

        monitor = WorkersCeleryResultsMonitor()
        result = monitor.get_worker_stats()

        assert result == []

    @patch("celery_monitor.results_monitor.workers_results.current_app")
    def test_get_worker_stats_exception_handling(self, mock_app):
        """Test get_worker_stats handles exceptions gracefully."""
        mock_app.control.inspect.side_effect = Exception("Connection error")

        monitor = WorkersCeleryResultsMonitor()
        result = monitor.get_worker_stats()

        assert result == []

    @patch("celery_monitor.results_monitor.workers_results.current_app")
    def test_get_reserved_tasks_returns_typed_list(self, mock_app):
        """Test get_reserved_tasks returns a list of ReservedTask dataclasses."""
        mock_inspect = Mock()
        mock_inspect.reserved.return_value = {
            "worker1@host": [
                {
                    "id": "abc-123",
                    "name": "tasks.send_email",
                    "hostname": "worker1@host",
                    "args": ["user@example.com"],
                    "kwargs": {"subject": "Hello"},
                },
                {
                    "id": "def-456",
                    "name": "tasks.generate_report",
                    "hostname": "worker1@host",
                    "args": [],
                    "kwargs": {"report_id": 99},
                },
            ],
            "worker2@host": [
                {
                    "id": "ghi-789",
                    "name": "tasks.process_payment",
                    "hostname": "worker2@host",
                    "args": [500],
                    "kwargs": {"currency": "USD"},
                },
            ],
        }
        mock_app.control.inspect.return_value = mock_inspect

        monitor = WorkersCeleryResultsMonitor()
        result = monitor.get_reserved_tasks()

        assert len(result) == 3
        assert all(isinstance(t, ReservedTask) for t in result)
        # Results sorted by worker name
        assert result[0].worker == "worker1@host"
        assert result[0].id == "abc-123"
        assert result[0].name == "tasks.send_email"
        assert result[0].hostname == "worker1@host"
        assert result[0].args == ["user@example.com"]
        assert result[0].kwargs == {"subject": "Hello"}
        assert result[1].id == "def-456"
        assert result[2].worker == "worker2@host"
        assert result[2].id == "ghi-789"

    @patch("celery_monitor.results_monitor.workers_results.current_app")
    def test_get_reserved_tasks_empty(self, mock_app):
        """Test get_reserved_tasks returns empty list when no reserved tasks."""
        mock_inspect = Mock()
        mock_inspect.reserved.return_value = {}
        mock_app.control.inspect.return_value = mock_inspect

        monitor = WorkersCeleryResultsMonitor()
        result = monitor.get_reserved_tasks()

        assert result == []

    @patch("celery_monitor.results_monitor.workers_results.current_app")
    def test_get_reserved_tasks_exception_handling(self, mock_app):
        """Test get_reserved_tasks returns empty list on error."""
        mock_app.control.inspect.side_effect = Exception("Broker unreachable")

        monitor = WorkersCeleryResultsMonitor()
        result = monitor.get_reserved_tasks()

        assert result == []
