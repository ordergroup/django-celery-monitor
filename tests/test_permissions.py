"""Tests for Celery Monitor view permissions."""

from unittest import mock

import pytest
from django.contrib import admin
from django.contrib.auth.models import Permission
from django.test import RequestFactory
from django.urls import reverse

from celery_monitor.models import QueueStats

URL_KWARGS = {"task_id": "abc123", "queue_name": "celery"}

MONITOR_URL_NAMES = [
    (pattern.name, list(pattern.pattern.converters))
    for pattern in admin.site.get_urls()
    if (getattr(pattern, "name", None) or "").startswith("celery_monitor_")
]


def _url(name, params=()):
    return reverse(f"admin:{name}", kwargs={p: URL_KWARGS[p] for p in params})


def _staff(django_user_model, username, *codenames):
    user = django_user_model.objects.create_user(
        username=username, password="pass", is_staff=True
    )
    user.user_permissions.add(
        *Permission.objects.filter(
            content_type__app_label="celery_monitor", codename__in=codenames
        )
    )
    return user


@pytest.fixture
def staff(django_user_model):
    return _staff(django_user_model, "staff")


@pytest.fixture
def viewer(django_user_model):
    return _staff(django_user_model, "viewer", "view_celery_monitor")


@pytest.fixture
def manager(django_user_model):
    return _staff(
        django_user_model, "manager", "view_celery_monitor", "manage_celery_monitor"
    )


def _queue_monitor_stub():
    monitor = mock.Mock()
    monitor.get_queue_stats.return_value = [
        QueueStats(queue_name="total", count=1),
        QueueStats(queue_name="celery", count=1),
    ]
    return monitor


@pytest.mark.django_db
class TestPermissions:
    def test_monitor_routes_are_registered(self):
        assert len(MONITOR_URL_NAMES) > 20

    @pytest.mark.parametrize("name,params", MONITOR_URL_NAMES)
    def test_staff_without_permissions_gets_403(self, client, staff, name, params):
        client.force_login(staff)
        url = _url(name, params)
        assert client.get(url).status_code == 403
        assert client.post(url).status_code == 403

    def test_anonymous_is_redirected_to_login(self, client):
        response = client.get(_url("celery_monitor_dashboard"))
        assert response.status_code == 302
        assert reverse("admin:login") in response["Location"]

    @pytest.mark.parametrize(
        "user_fixture,visible", [("viewer", False), ("manager", True)]
    )
    def test_dashboard_action_buttons_require_manage(
        self, request, client, user_fixture, visible
    ):
        client.force_login(request.getfixturevalue(user_fixture))
        with mock.patch(
            "celery_monitor.templatetags.celery_monitor_tags.is_redis_backend",
            return_value=True,
        ):
            response = client.get(_url("celery_monitor_dashboard"))
        assert response.status_code == 200
        assert (
            _url("celery_monitor_clear_all") in response.content.decode()
        ) is visible

    @pytest.mark.parametrize(
        "name,params",
        [
            ("celery_monitor_task_revoke", ["task_id"]),
            ("celery_monitor_task_kill", ["task_id"]),
            ("celery_monitor_clear_queue", ["queue_name"]),
            ("celery_monitor_clear_all", []),
        ],
    )
    def test_viewer_cannot_run_actions(self, client, viewer, name, params):
        client.force_login(viewer)
        assert client.post(_url(name, params)).status_code == 403

    def test_manager_can_revoke_task(self, client, manager):
        client.force_login(manager)
        with mock.patch("celery_monitor.views.current_app") as current_app:
            response = client.post(_url("celery_monitor_task_revoke", ["task_id"]))
        assert response.status_code == 204
        current_app.control.revoke.assert_called_once_with("abc123")

    def test_manager_can_clear_results(self, client, manager):
        client.force_login(manager)
        with mock.patch(
            "celery_monitor.redis.tasks.clear_celery_results.delay"
        ) as delay:
            response = client.post(_url("celery_monitor_clear_results"))
        assert response.status_code == 204
        delay.assert_called_once_with()

    def test_superuser_can_view_dashboard(self, admin_client):
        assert admin_client.get(_url("celery_monitor_dashboard")).status_code == 200

    @pytest.mark.parametrize(
        "user_fixture,visible", [("viewer", False), ("manager", True)]
    )
    def test_queue_clear_buttons_require_manage(
        self, request, client, user_fixture, visible
    ):
        client.force_login(request.getfixturevalue(user_fixture))
        with mock.patch(
            "celery_monitor.views.get_queue_monitor", return_value=_queue_monitor_stub()
        ):
            response = client.get(_url("celery_monitor_redis_queue_stats"))
        assert response.status_code == 200
        assert ("clear-queue-btn" in response.content.decode()) is visible

    @pytest.mark.parametrize(
        "user_fixture,visible",
        [("staff", False), ("viewer", True), ("admin_user", True)],
    )
    def test_app_list_entry_requires_view(self, request, user_fixture, visible):
        http_request = RequestFactory().get("/admin/")
        http_request.user = request.getfixturevalue(user_fixture)
        app_labels = [app["app_label"] for app in admin.site.get_app_list(http_request)]
        assert ("celery_monitor" in app_labels) is visible
