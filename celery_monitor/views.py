import math

from django.contrib.admin import AdminSite
from django.http import HttpRequest, HttpResponse, HttpResponseNotFound
from django.template.response import TemplateResponse
from django.views.decorators.http import require_POST

from celery_monitor.filters import (
    RecentTasksFilters,
    TaskExecutionStatsFilters,
    TaskResultsFilters,
    WorkerStatsFilters,
)
from celery_monitor.queue_monitor import get_queue_monitor
from celery_monitor.results_monitor import get_results_monitor
from celery_monitor.utils import is_redis_backend


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
    filters = WorkerStatsFilters.from_request(request)
    results_monitor = get_results_monitor()
    worker_stats = results_monitor.get_worker_stats(
        include_offline=filters.include_offline
    )
    context = {"worker_stats": worker_stats}
    return TemplateResponse(
        request,
        "celery_monitor/partials/worker_stats.html",
        context,
    )


def recent_tasks_view(request: HttpRequest):
    filters = RecentTasksFilters.from_request(request)
    results_monitor = get_results_monitor()
    data = results_monitor.get_recent_tasks(
        status=filters.status,
        task_name=filters.task_name,
        worker=filters.worker,
        limit=filters.limit,
    )

    context = {
        "recent_tasks": data.recent_tasks,
        "filters": filters,
        "task_names": data.task_names,
        "workers": data.workers,
    }
    return TemplateResponse(
        request,
        "celery_monitor/partials/recent_tasks.html",
        context,
    )


def task_execution_stats_view(request: HttpRequest, site: AdminSite):
    filters = TaskExecutionStatsFilters.from_request(request)
    results_monitor = get_results_monitor()
    execution_stats = results_monitor.get_task_execution_stats(
        sort_by=filters.sort_by,
        sort_order=filters.sort_order,
        date_from=filters.date_from,
        date_to=filters.date_to,
    )
    context = {
        **site.each_context(request),
        "title": "Task Execution Stats",
        "execution_stats": execution_stats,
        "filters": filters,
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


def task_results(request: HttpRequest, site: AdminSite):
    filters = TaskResultsFilters.from_request(request)
    results_monitor = get_results_monitor()
    result = results_monitor.get_tasks(
        status=filters.status,
        task_name=filters.task_name,
        worker=filters.worker,
        date_from=filters.date_from,
        date_to=filters.date_to,
        page=filters.page,
        page_size=filters.page_size,
    )
    total_pages = max(1, math.ceil(result.total / filters.page_size))

    context = {
        **site.each_context(request),
        "title": "Task Results",
        "tasks": result.tasks,
        "total": result.total,
        "task_names": result.task_names,
        "workers": result.workers,
        "filters": filters,
        "total_pages": total_pages,
        "is_redis": is_redis_backend(),
    }
    return TemplateResponse(request, "celery_monitor/task_results.html", context)


@require_POST
def clear_queue(request: HttpRequest, site: AdminSite, queue_name: str):
    queue_monitor = get_queue_monitor()
    queue_monitor.clear_queue(queue_name)
    return HttpResponse(status=204)


@require_POST
def clear_all_queues(request: HttpRequest, site: AdminSite):
    queue_monitor = get_queue_monitor()
    for queue_name in queue_monitor.get_queue_names():
        queue_monitor.clear_queue(queue_name)
    return HttpResponse(status=204)
