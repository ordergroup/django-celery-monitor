import sys
from unittest.mock import MagicMock, patch

import pytest
from django.conf import settings as django_settings

from celery_monitor.enums import BackendType

HAS_CELERY_RESULTS = "django_celery_results" in django_settings.INSTALLED_APPS


class TestBackendType:
    def test_from_str_redis(self):
        assert BackendType.from_str("redis") == BackendType.REDIS

    def test_from_str_celery_results(self):
        assert BackendType.from_str("celery_results") == BackendType.CELERY_RESULTS

    def test_from_str_unknown(self):
        assert BackendType.from_str("unknown") == BackendType.UNKNOWN

    def test_from_str_uppercase(self):
        assert BackendType.from_str("REDIS") == BackendType.REDIS

    def test_from_str_mixed_case(self):
        assert BackendType.from_str("Celery_Results") == BackendType.CELERY_RESULTS

    def test_from_str_invalid(self):
        assert BackendType.from_str("invalid") == BackendType.UNKNOWN

    def test_from_str_empty(self):
        assert BackendType.from_str("") == BackendType.UNKNOWN


class TestHasDjangoCeleryResult:
    def test_returns_false_when_not_importable(self):
        from celery_monitor.utils import has_django_celery_result

        with patch.dict(sys.modules, {"django_celery_results": None}):
            result = has_django_celery_result()
        assert result is False

    def test_returns_true_when_importable(self):
        from celery_monitor.utils import has_django_celery_result

        mock_module = MagicMock()
        with patch.dict(sys.modules, {"django_celery_results": mock_module}):
            result = has_django_celery_result()
        assert result is True


class TestHasRedis:
    @patch("celery_monitor.utils.current_app")
    def test_returns_true_for_redis_url(self, mock_app):
        mock_app.conf.broker_url = "redis://localhost:6379/0"
        from celery_monitor.utils import has_redis

        assert has_redis() is True

    @patch("celery_monitor.utils.current_app")
    def test_returns_true_for_rediss_url(self, mock_app):
        mock_app.conf.broker_url = "rediss://localhost:6379/0"
        from celery_monitor.utils import has_redis

        assert has_redis() is True

    @patch("celery_monitor.utils.current_app")
    def test_returns_false_for_amqp_url(self, mock_app):
        mock_app.conf.broker_url = "amqp://localhost"
        from celery_monitor.utils import has_redis

        assert has_redis() is False

    @patch("celery_monitor.utils.current_app")
    def test_returns_false_for_none_url(self, mock_app):
        mock_app.conf.broker_url = None
        from celery_monitor.utils import has_redis

        assert has_redis() is False

    @patch("celery_monitor.utils.current_app")
    def test_returns_false_for_empty_url(self, mock_app):
        mock_app.conf.broker_url = ""
        from celery_monitor.utils import has_redis

        assert has_redis() is False


class TestIsRedisBackend:
    @patch("celery_monitor.utils.has_redis", return_value=True)
    @patch("celery_monitor.utils.has_django_celery_result", return_value=False)
    def test_true_when_redis_setting_and_redis_available(self, _dcr, _redis, settings):
        settings.CELERY_MONITOR_RESULTS_BACKEND = "redis"
        from celery_monitor.utils import is_redis_backend

        assert is_redis_backend() is True

    @patch("celery_monitor.utils.has_redis", return_value=True)
    @patch("celery_monitor.utils.has_django_celery_result", return_value=True)
    def test_false_when_celery_results_takes_priority(self, _dcr, _redis, settings):
        settings.CELERY_MONITOR_RESULTS_BACKEND = "celery_results"
        from celery_monitor.utils import is_redis_backend

        assert is_redis_backend() is False

    @patch("celery_monitor.utils.has_redis", return_value=False)
    @patch("celery_monitor.utils.has_django_celery_result", return_value=False)
    def test_false_when_redis_not_available(self, _dcr, _redis, settings):
        settings.CELERY_MONITOR_RESULTS_BACKEND = "redis"
        from celery_monitor.utils import is_redis_backend

        assert is_redis_backend() is False

    @patch("celery_monitor.utils.has_redis", return_value=True)
    @patch("celery_monitor.utils.has_django_celery_result", return_value=True)
    def test_false_when_unknown_and_celery_results_installed(
        self, _dcr, _redis, settings
    ):
        settings.CELERY_MONITOR_RESULTS_BACKEND = "unknown"
        from celery_monitor.utils import is_redis_backend

        assert is_redis_backend() is False

    @patch("celery_monitor.utils.has_redis", return_value=True)
    @patch("celery_monitor.utils.has_django_celery_result", return_value=False)
    def test_true_when_unknown_and_redis_available(self, _dcr, _redis, settings):
        settings.CELERY_MONITOR_RESULTS_BACKEND = "unknown"
        from celery_monitor.utils import is_redis_backend

        assert is_redis_backend() is True


class TestGetResultsMonitor:
    @pytest.mark.skipif(
        not HAS_CELERY_RESULTS, reason="django_celery_results not in INSTALLED_APPS"
    )
    @patch("celery_monitor.results_monitor.has_redis", return_value=False)
    @patch("celery_monitor.results_monitor.has_django_celery_result", return_value=True)
    def test_returns_django_celery_results_monitor(self, _dcr, _redis, settings):
        settings.CELERY_MONITOR_RESULTS_BACKEND = "celery_results"
        from celery_monitor.results_monitor import get_results_monitor
        from celery_monitor.results_monitor.django_celery_results import (
            DjangoCeleryResultsMonitor,
        )

        result = get_results_monitor()
        assert isinstance(result, DjangoCeleryResultsMonitor)

    @patch("celery_monitor.results_monitor.has_redis", return_value=True)
    @patch(
        "celery_monitor.results_monitor.has_django_celery_result", return_value=False
    )
    def test_returns_redis_monitor_when_redis_configured(self, _dcr, _redis, settings):
        settings.CELERY_MONITOR_RESULTS_BACKEND = "redis"

        try:
            from unittest.mock import MagicMock

            from celery_monitor.results_monitor import get_results_monitor
            from celery_monitor.results_monitor.redis_results import RedisResultsMonitor

            mock_redis_client = MagicMock()
            mock_redis_client.get.return_value = None
            with patch(
                "celery_monitor.redis.client.get_results_client",
                return_value=mock_redis_client,
            ):
                result = get_results_monitor()
            assert isinstance(result, RedisResultsMonitor)
        except ImportError:
            pytest.skip("redis not installed")

    @patch("celery_monitor.results_monitor.has_redis", return_value=False)
    @patch(
        "celery_monitor.results_monitor.has_django_celery_result", return_value=False
    )
    def test_returns_workers_monitor_as_fallback(self, _dcr, _redis, settings):
        settings.CELERY_MONITOR_RESULTS_BACKEND = "unknown"
        from celery_monitor.results_monitor import get_results_monitor
        from celery_monitor.results_monitor.workers_results import (
            WorkersCeleryResultsMonitor,
        )

        result = get_results_monitor()
        assert isinstance(result, WorkersCeleryResultsMonitor)


class TestGetSignalsBackend:
    @patch("celery_monitor.signals_backend.has_redis", return_value=False)
    @patch("celery_monitor.signals_backend.has_django_celery_result", return_value=True)
    def test_returns_noop_when_celery_results_installed(self, _dcr, _redis, settings):
        settings.CELERY_MONITOR_RESULTS_BACKEND = "celery_results"
        from unittest.mock import Mock

        from celery import Celery

        from celery_monitor.signals_backend import get_signals_backend
        from celery_monitor.signals_backend.noop import NoopSignalsResultBackend

        app = Mock(spec=Celery)
        result = get_signals_backend(app)
        assert isinstance(result, NoopSignalsResultBackend)

    @patch("celery_monitor.signals_backend.has_redis", return_value=True)
    @patch(
        "celery_monitor.signals_backend.has_django_celery_result", return_value=False
    )
    def test_returns_redis_backend_when_redis_configured(self, _dcr, _redis, settings):
        settings.CELERY_MONITOR_RESULTS_BACKEND = "redis"
        from unittest.mock import Mock

        from celery import Celery

        from celery_monitor.signals_backend import get_signals_backend
        from celery_monitor.signals_backend.redis import RedisSignalsResultBackend

        app = Mock(spec=Celery)
        with patch.object(
            RedisSignalsResultBackend, "_get_redis_client", return_value=None
        ):
            result = get_signals_backend(app)
        assert isinstance(result, RedisSignalsResultBackend)

    @patch("celery_monitor.signals_backend.has_redis", return_value=False)
    @patch(
        "celery_monitor.signals_backend.has_django_celery_result", return_value=False
    )
    def test_returns_noop_as_fallback(self, _dcr, _redis, settings):
        settings.CELERY_MONITOR_RESULTS_BACKEND = "unknown"
        from unittest.mock import Mock

        from celery import Celery

        from celery_monitor.signals_backend import get_signals_backend
        from celery_monitor.signals_backend.noop import NoopSignalsResultBackend

        app = Mock(spec=Celery)
        result = get_signals_backend(app)
        assert isinstance(result, NoopSignalsResultBackend)
