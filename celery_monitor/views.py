from django.contrib.admin import AdminSite
from django.http import HttpRequest, HttpResponseNotFound
from django.template.response import TemplateResponse

from celery_monitor.queue_monitor import get_queue_monitor
from celery_monitor.results_monitor import get_results_monitor


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
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")
    hours_param = request.GET.get("hours", "1")

    if date_from and date_to:
        hours = None
        current_hours = "custom"
    elif hours_param == "all":
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
        "min_runtime",
        "max_runtime",
    ]
    if sort_by not in valid_sorts:
        sort_by = "total_count"
    if sort_order not in ["asc", "desc"]:
        sort_order = "desc"

    results_monitor = get_results_monitor()
    execution_stats = results_monitor.get_task_execution_stats(
        hours=hours,
        sort_by=sort_by,
        sort_order=sort_order,
        date_from=date_from,
        date_to=date_to,
    )
    context = {
        **site.each_context(request),
        "title": "Task Execution Stats",
        "execution_stats": execution_stats,
        "current_hours": current_hours,
        "current_sort": sort_by,
        "current_order": sort_order,
        "date_from": date_from,
        "date_to": date_to,
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


def task_detail_view(request: HttpRequest, site: AdminSite, task_id: str):
    results_monitor = get_results_monitor()
    task = results_monitor.get_task_detail(task_id)

    if not task:
        return HttpResponseNotFound(f"Task with ID {task_id} not found")

    context = {
        **site.each_context(request),
        "title": f"Task {task_id}",
        "task": task,
    }
    return TemplateResponse(request, "celery_monitor/task_detail.html", context)
