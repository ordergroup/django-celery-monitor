from django.contrib.admin import AdminSite
from django.http import HttpRequest
from django.template.response import TemplateResponse
from django.urls import path, reverse

from celery_monitor.queue_monitor import get_queue_monitor
from celery_monitor.results_monitor import get_results_monitor
from celery_monitor.utils import has_django_celery_result


def status_counts_overall_view(request: HttpRequest):
    results_monitor = get_results_monitor()
    status_counts = results_monitor.get_overall_status_counts()
    context = {"status_counts": {row.status: row.count for row in status_counts}}
    return TemplateResponse(
        request,
        "celery_monitor/partials/status_counts.html",
        context,
    )


def status_counts_last_hour_view(request: HttpRequest):
    results_monitor = get_results_monitor()
    status_counts = results_monitor.get_last_hour_status_counts()
    context = {"status_counts": {row.status: row.count for row in status_counts}}
    return TemplateResponse(
        request,
        "celery_monitor/partials/status_counts.html",
        context,
    )


def redis_queue_stats_view(request: HttpRequest):
    queue_monitor = get_queue_monitor()
    context = {"queue_stats": queue_monitor.get_queue_stats()}
    return TemplateResponse(
        request,
        "celery_monitor/partials/queue_stats.html",
        context,
    )


def redis_queue_task_types_view(request: HttpRequest):
    queue_monitor = get_queue_monitor()
    task_type_stats = queue_monitor.get_queue_task_types()
    context = {"task_type_stats": task_type_stats}
    return TemplateResponse(
        request,
        "celery_monitor/partials/queue_task_types.html",
        context,
    )


def worker_stats_view(request: HttpRequest):
    results_monitor = get_results_monitor()
    worker_stats = results_monitor.get_worker_stats()
    context = {"worker_stats": worker_stats}
    return TemplateResponse(
        request,
        "celery_monitor/partials/worker_stats.html",
        context,
    )


def recent_tasks_view(request: HttpRequest):
    status_filter = request.GET.get("status") or None
    task_name_filter = request.GET.get("task_name") or None
    worker_filter = request.GET.get("worker") or None

    results_monitor = get_results_monitor()
    data = results_monitor.get_recent_tasks(
        status=status_filter,
        task_name=task_name_filter,
        worker=worker_filter,
    )

    context = {
        "recent_tasks": data.recent_tasks,
        "current_status": status_filter or "",
        "current_task_name": task_name_filter or "",
        "current_worker": worker_filter or "",
        "task_names": data.task_names,
        "workers": data.workers,
    }
    return TemplateResponse(
        request,
        "celery_monitor/partials/recent_tasks.html",
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

    results_monitor = get_results_monitor()
    execution_stats = results_monitor.get_task_execution_stats(
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
    context = {
        **site.each_context(request),
        "title": "Celery Monitor",
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
                "celery-monitor/recent-tasks",
                site.admin_view(recent_tasks_view),
                name="celery_monitor_recent_tasks",
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
