from django.contrib.admin import AdminSite
from django.http import HttpRequest
from django.db.models import Count
from django.template.response import TemplateResponse
from django.urls import path, reverse
from django.utils import timezone
from celery_monitor import queries

from django_celery_results.models import TaskResult


def status_counts_overall_view(request: HttpRequest):
    status_counts = queries.get_overall_status_counts()
    context = {"status_counts": {row.status: row.count for row in status_counts}}
    return TemplateResponse(
        request,
        "celery_monitor/partials/status_counts.html",
        context,
    )


def status_counts_last_hour_view(request: HttpRequest):
    status_counts = queries.get_last_hour_status_counts()
    context = {"status_counts": {row.status: row.count for row in status_counts}}
    return TemplateResponse(
        request,
        "celery_monitor/partials/status_counts.html",
        context,
    )


def dashboard_view(request: HttpRequest, site: AdminSite):
    status_filter = request.GET.get("status", "")
    task_name_filter = request.GET.get("task_name", "")
    worker_filter = request.GET.get("worker", "")

    qs = TaskResult.objects.all()
    if status_filter:
        qs = qs.filter(status=status_filter)
    if task_name_filter:
        qs = qs.filter(task_name=task_name_filter)
    if worker_filter:
        qs = qs.filter(worker=worker_filter)

    recent_tasks = qs.order_by("-date_done")[:50]

    task_names = (
        TaskResult.objects.values_list("task_name", flat=True)
        .distinct()
        .order_by("task_name")
    )
    workers = (
        TaskResult.objects.exclude(worker__isnull=True)
        .values_list("worker", flat=True)
        .distinct()
        .order_by("worker")
    )

    now = timezone.now()
    last_hour = now - timezone.timedelta(hours=1)
    last_hour_counts = (
        TaskResult.objects.filter(date_done__gte=last_hour)
        .values("status")
        .annotate(count=Count("id"))
        .order_by("status")
    )

    status_counts = queries.get_overall_status_counts()

    context = {
        **site.each_context(request),
        "title": "Celery Monitor",
        "status_counts": {row.status: row.count for row in status_counts},
        "last_hour_counts": {row["status"]: row["count"] for row in last_hour_counts},
        "total_tasks": TaskResult.objects.count(),
        "recent_tasks": recent_tasks,
        "task_names": task_names,
        "workers": workers,
        "current_status": status_filter,
        "current_task_name": task_name_filter,
        "current_worker": worker_filter,
    }
    return TemplateResponse(request, "celery_monitor/dashboard.html", context)


def task_detail_view(request: HttpRequest, site: AdminSite, task_id: int):
    task = TaskResult.objects.get(task_id=task_id)
    context = {
        **site.each_context(request),
        "title": f"Task {task_id}",
        "task": task,
    }
    return TemplateResponse(request, "celery_monitor/task_detail.html", context)


def patch_admin_site(site):
    _orig_get_urls = site.get_urls
    _orig_get_app_list = site.get_app_list

    def new_get_urls():
        custom_urls = [
            path(
                "celery-monitor/",
                site.admin_view(lambda req: dashboard_view(req, site)),
                name="celery_monitor_dashboard",
            ),
            path(
                "celery-monitor/status-counts-overall",
                site.admin_view(status_counts_overall_view),
                name="celery_monitor_statuses_overall_count",
            ),
            path(
                "celery-monitor/status-counts-last-hr",
                site.admin_view(status_counts_last_hour_view),
                name="celery_monitor_statuses_last_hour_count",
            ),
            path(
                "celery-monitor/task/<str:task_id>/",
                site.admin_view(
                    lambda req, task_id: task_detail_view(req, site, task_id)
                ),
                name="celery_monitor_task_detail",
            ),
        ]
        return custom_urls + _orig_get_urls()

    site.get_urls = new_get_urls

    def new_get_app_list(request, app_label=None):
        app_list = _orig_get_app_list(request, app_label=app_label)
        if app_label is None or app_label == "celery_monitor":
            dashboard_url = reverse(f"{site.name}:celery_monitor_dashboard")
            app_list.append(
                {
                    "name": "Celery Monitor",
                    "app_label": "celery_monitor",
                    "app_url": dashboard_url,
                    "has_module_perms": True,
                    "models": [
                        {
                            "name": "Dashboard",
                            "object_name": "CeleryMonitorDashboard",
                            "admin_url": dashboard_url,
                            "view_only": True,
                        }
                    ],
                }
            )
        return app_list

    site.get_app_list = new_get_app_list
