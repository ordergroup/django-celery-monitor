from django.contrib.admin import AdminSite
from django.db.models import Count
from django.http import HttpRequest
from django.template.response import TemplateResponse
from django.urls import path, reverse
from django.utils import timezone
from django_celery_results.models import TaskResult

from celery_monitor import queries


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


def redis_queue_stats_view(request: HttpRequest):
    queue_stats = queries.get_redis_queue_stats()
    context = {"queue_stats": queue_stats}
    return TemplateResponse(
        request,
        "celery_monitor/partials/queue_stats.html",
        context,
    )


def redis_queue_task_types_view(request: HttpRequest):
    task_type_stats = queries.get_redis_queue_task_types()
    context = {"task_type_stats": task_type_stats}
    return TemplateResponse(
        request,
        "celery_monitor/partials/queue_task_types.html",
        context,
    )


def worker_stats_view(request: HttpRequest):
    worker_stats = queries.get_worker_stats()
    context = {"worker_stats": worker_stats}
    return TemplateResponse(
        request,
        "celery_monitor/partials/worker_stats.html",
        context,
    )


def task_execution_stats_view(request: HttpRequest, site: AdminSite):
    hours_param = request.GET.get("hours", "1")

    if hours_param == "all":
        hours = None
        current_hours = "all"
    else:
        try:
            hours = int(hours_param)
            if hours <= 0:
                hours = 1
            current_hours = hours
        except (ValueError, TypeError):
            hours = 1
            current_hours = 1

    sort_by = request.GET.get("sort", "total_count")
    sort_order = request.GET.get("order", "desc")

    valid_sorts = [
        "task_name",
        "total_count",
        "success_count",
        "failure_count",
        "avg_runtime",
    ]
    if sort_by not in valid_sorts:
        sort_by = "total_count"
    if sort_order not in ["asc", "desc"]:
        sort_order = "desc"

    execution_stats = queries.get_task_execution_stats(
        hours=hours, sort_by=sort_by, sort_order=sort_order
    )
    context = {
        **site.each_context(request),
        "title": "Task Execution Stats",
        "execution_stats": execution_stats,
        "current_hours": current_hours,
        "current_sort": sort_by,
        "current_order": sort_order,
    }
    return TemplateResponse(
        request,
        "celery_monitor/task_execution_stats.html",
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
                "celery-monitor/redis-queue-stats",
                site.admin_view(redis_queue_stats_view),
                name="celery_monitor_redis_queue_stats",
            ),
            path(
                "celery-monitor/worker-stats",
                site.admin_view(worker_stats_view),
                name="celery_monitor_worker_stats",
            ),
            path(
                "celery-monitor/redis-queue-task-types",
                site.admin_view(redis_queue_task_types_view),
                name="celery_monitor_redis_queue_task_types",
            ),
            path(
                "celery-monitor/task-execution-stats",
                site.admin_view(lambda req: task_execution_stats_view(req, site)),
                name="celery_monitor_task_execution_stats",
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
            execution_stats_url = reverse(
                f"{site.name}:celery_monitor_task_execution_stats"
            )

            models = [
                {
                    "name": "Dashboard",
                    "object_name": "CeleryMonitorDashboard",
                    "admin_url": dashboard_url,
                    "view_only": True,
                }
            ]

            # Add execution stats if django-celery-results is installed
            from celery_monitor.utils import has_django_celery_result

            if has_django_celery_result():
                models.append(
                    {
                        "name": "Task Execution Stats",
                        "object_name": "TaskExecutionStats",
                        "admin_url": execution_stats_url,
                        "view_only": True,
                    }
                )

            app_list.append(
                {
                    "name": "Celery Monitor",
                    "app_label": "celery_monitor",
                    "app_url": dashboard_url,
                    "has_module_perms": True,
                    "models": models,
                }
            )
        return app_list

    site.get_app_list = new_get_app_list
