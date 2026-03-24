import json
import math

from celery import current_app
from django.conf import settings
from django.contrib.admin import AdminSite
from django.http import HttpRequest, HttpResponse, HttpResponseNotFound, JsonResponse
from django.template.response import TemplateResponse
from django.views.decorators.http import require_POST

from celery_monitor.filters import (
    QueueDetailFilters,
    RecentTasksFilters,
    TaskExecutionStatsFilters,
    TaskResultsFilters,
    TaskTypeDetailFilters,
    WorkerStatsFilters,
)
from celery_monitor.queue_monitor import get_queue_monitor
from celery_monitor.results_monitor import get_results_monitor
from celery_monitor.results_monitor.workers_results import WorkersCeleryResultsMonitor
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


def redis_queue_history_view(request: HttpRequest):
    queue_monitor = get_queue_monitor()
    queues = queue_monitor.queue_length_history()
    return JsonResponse({"queues": queues})


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


def reserved_tasks_view(request: HttpRequest):
    workers_monitor = WorkersCeleryResultsMonitor()
    reserved_tasks = workers_monitor.get_reserved_tasks()
    context = {"reserved_tasks": reserved_tasks}
    return TemplateResponse(
        request,
        "celery_monitor/partials/reserved_tasks.html",
        context,
    )


def recent_tasks_view(request: HttpRequest):
    filters = RecentTasksFilters.from_request(request)
    results_monitor = get_results_monitor()
    queue_monitor = get_queue_monitor()
    data = results_monitor.get_recent_tasks(
        status=filters.status,
        task_name=filters.task_name,
        queue_name=filters.queue_name,
        worker=filters.worker,
        limit=filters.limit,
    )
    task_names = results_monitor.get_tasks_names()
    queue_names = queue_monitor.get_queue_names()
    workers = results_monitor.get_workers_names()

    context = {
        "recent_tasks": data,
        "filters": filters,
        "task_names": task_names,
        "queue_names": queue_names,
        "workers": workers,
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
        "hours_param": request.GET.get("hours")
        or ("" if request.GET.get("date_from") else "1"),
        "filters": filters,
    }
    return TemplateResponse(
        request,
        "celery_monitor/task_execution_stats.html",
        context,
    )


def dashboard_view(request: HttpRequest, site: AdminSite):
    refresh_interval = getattr(
        settings, "DJANGO_CELERY_MONITOR_DASHBOARD_REFRESH_INTERVAL", 60
    )
    context = {
        **site.each_context(request),
        "title": "Celery Monitor",
        "refresh_interval": refresh_interval,
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
    queue_monitor = get_queue_monitor()
    result = results_monitor.get_tasks(
        status=filters.status,
        task_name=filters.task_name,
        queue_name=filters.queue_name,
        worker=filters.worker,
        date_from=filters.date_from,
        date_to=filters.date_to,
        page=filters.page,
        page_size=filters.page_size,
    )
    total_pages = max(1, math.ceil(result.total / filters.page_size))
    task_names = results_monitor.get_tasks_names()
    queue_names = queue_monitor.get_queue_names()
    workers = results_monitor.get_workers_names()

    context = {
        **site.each_context(request),
        "title": "Task Results",
        "tasks": result.tasks,
        "total": result.total,
        "task_names": task_names,
        "queue_names": queue_names,
        "workers": workers,
        "filters": filters,
        "total_pages": total_pages,
        "is_redis": is_redis_backend(),
        "filter_skipped": result.filter_skipped,
    }
    return TemplateResponse(request, "celery_monitor/task_results.html", context)


def task_type_detail_view(request: HttpRequest, site: AdminSite):
    filters = TaskTypeDetailFilters.from_request(request)
    results_monitor = get_results_monitor()
    task_names = sorted(results_monitor.get_tasks_names())

    if not filters.task_name:
        context = {
            **site.each_context(request),
            "title": "Task Type Detail",
            "task_names": task_names,
            "task_name": None,
        }
        return TemplateResponse(
            request, "celery_monitor/task_type_detail.html", context
        )

    time_series = results_monitor.get_task_type_time_series(
        task_name=filters.task_name,
        date_from=filters.date_from,
        date_to=filters.date_to,
    )
    result = results_monitor.get_tasks(
        task_name=filters.task_name,
        status=filters.status,
        worker=filters.worker,
        date_from=filters.date_from,
        date_to=filters.date_to,
        page=filters.page,
        page_size=filters.page_size,
    )
    total_pages = max(1, math.ceil(result.total / filters.page_size))
    workers = results_monitor.get_workers_names()

    throughput_buckets = results_monitor.get_throughput_time_series(
        task_name=filters.task_name,
        date_from=filters.date_from,
        date_to=filters.date_to,
    )
    # Build cumulative step series: each point is {x: iso_timestamp, y: cumulative_count}
    queued_total = 0
    started_total = 0
    queued_cumulative = []
    started_cumulative = []
    for b in throughput_buckets:
        queued_total += b.queued_count
        queued_cumulative.append({"x": b.bucket.isoformat(), "y": queued_total})
        started_total += b.started_count
        started_cumulative.append({"x": b.bucket.isoformat(), "y": started_total})

    chart_data = {
        "labels": [ts.bucket.isoformat() for ts in time_series],
        "counts": [ts.count for ts in time_series],
        "success_counts": [ts.success_count for ts in time_series],
        "failure_counts": [ts.failure_count for ts in time_series],
        "runtime": {
            "avg": [ts.avg_runtime for ts in time_series],
            "min": [ts.min_runtime for ts in time_series],
            "max": [ts.max_runtime for ts in time_series],
        },
        "wait": {
            "avg": [ts.avg_wait for ts in time_series],
            "min": [ts.min_wait for ts in time_series],
            "max": [ts.max_wait for ts in time_series],
        },
        "throughput": {
            "queued": queued_cumulative,
            "started": started_cumulative,
        },
    }

    context = {
        **site.each_context(request),
        "title": filters.task_name,
        "task_name": filters.task_name,
        "task_names": task_names,
        "tasks": result.tasks,
        "total": result.total,
        "workers": workers,
        "filters": filters,
        "total_pages": total_pages,
        "is_redis": is_redis_backend(),
        "chart_data_json": json.dumps(chart_data),
        "filter_skipped": result.filter_skipped,
    }
    return TemplateResponse(request, "celery_monitor/task_type_detail.html", context)


def queue_detail_view(request: HttpRequest, site: AdminSite):
    filters = QueueDetailFilters.from_request(request)
    queue_monitor = get_queue_monitor()
    queue_names = sorted(queue_monitor.get_queue_names())

    if not filters.queue_name:
        context = {
            **site.each_context(request),
            "title": "Queue Detail",
            "queue_names": queue_names,
            "queue_name": None,
        }
        return TemplateResponse(request, "celery_monitor/queue_detail.html", context)

    results_monitor = get_results_monitor()
    time_series = results_monitor.get_queue_time_series(
        queue_name=filters.queue_name,
        date_from=filters.date_from,
        date_to=filters.date_to,
    )
    result = results_monitor.get_tasks(
        queue_name=filters.queue_name,
        status=filters.status,
        worker=filters.worker,
        date_from=filters.date_from,
        date_to=filters.date_to,
        page=filters.page,
        page_size=filters.page_size,
    )
    total_pages = max(1, math.ceil(result.total / filters.page_size))
    workers = results_monitor.get_workers_names()

    throughput_buckets = results_monitor.get_queue_throughput_time_series(
        queue_name=filters.queue_name,
        date_from=filters.date_from,
        date_to=filters.date_to,
    )
    queued_total = 0
    started_total = 0
    queued_cumulative = []
    started_cumulative = []
    for b in throughput_buckets:
        queued_total += b.queued_count
        queued_cumulative.append({"x": b.bucket.isoformat(), "y": queued_total})
        started_total += b.started_count
        started_cumulative.append({"x": b.bucket.isoformat(), "y": started_total})

    chart_data = {
        "labels": [ts.bucket.isoformat() for ts in time_series],
        "counts": [ts.count for ts in time_series],
        "success_counts": [ts.success_count for ts in time_series],
        "failure_counts": [ts.failure_count for ts in time_series],
        "runtime": {
            "avg": [ts.avg_runtime for ts in time_series],
            "min": [ts.min_runtime for ts in time_series],
            "max": [ts.max_runtime for ts in time_series],
        },
        "wait": {
            "avg": [ts.avg_wait for ts in time_series],
            "min": [ts.min_wait for ts in time_series],
            "max": [ts.max_wait for ts in time_series],
        },
        "throughput": {
            "queued": queued_cumulative,
            "started": started_cumulative,
        },
    }

    context = {
        **site.each_context(request),
        "title": filters.queue_name,
        "queue_name": filters.queue_name,
        "queue_names": queue_names,
        "tasks": result.tasks,
        "total": result.total,
        "workers": workers,
        "filters": filters,
        "total_pages": total_pages,
        "is_redis": is_redis_backend(),
        "chart_data_json": json.dumps(chart_data),
        "filter_skipped": result.filter_skipped,
    }
    return TemplateResponse(request, "celery_monitor/queue_detail.html", context)


_REFRESH_TRIGGER = {"HX-Trigger": "refreshMemoryInfo"}


@require_POST
def clear_computed_stats_view(request: HttpRequest, site: AdminSite):
    from celery_monitor.redis.tasks import clear_celery_stats

    clear_celery_stats.delay()
    return HttpResponse(status=204, headers=_REFRESH_TRIGGER)


@require_POST
def compute_stats_view(request: HttpRequest, site: AdminSite):
    from celery_monitor.redis.tasks import calculate_celery_stats

    calculate_celery_stats.delay(overwrite=True)
    return HttpResponse(status=204, headers=_REFRESH_TRIGGER)


@require_POST
def clear_results_view(request: HttpRequest, site: AdminSite):
    from celery_monitor.redis.tasks import clear_celery_results

    clear_celery_results.delay()
    return HttpResponse(status=204, headers=_REFRESH_TRIGGER)


@require_POST
def clear_all_view(request: HttpRequest, site: AdminSite):
    from celery_monitor.redis.tasks import clear_all_celery_data

    clear_all_celery_data.delay()
    return HttpResponse(status=204, headers=_REFRESH_TRIGGER)


@require_POST
def prune_stale_recent_tasks_view(request: HttpRequest, site: AdminSite):
    from celery_monitor.redis.tasks import prune_stale_recent_tasks

    prune_stale_recent_tasks.delay()
    return HttpResponse(status=204, headers=_REFRESH_TRIGGER)


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


@require_POST
def kill_task(request: HttpRequest, site: AdminSite, task_id: str):
    sigkill = request.GET.get("sigkill") is not None
    signal = "SIGKILL" if sigkill else "SIGTERM"
    current_app.control.revoke(task_id, terminate=True, signal=signal)
    return HttpResponse(status=204)


@require_POST
def revoke_task(request: HttpRequest, site: AdminSite, task_id: str):
    current_app.control.revoke(task_id)
    return HttpResponse(status=204)
