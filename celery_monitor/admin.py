import inspect

from django.urls import path, reverse

from celery_monitor import views


def patch_admin_site(site):
    _orig_get_urls = site.get_urls
    _orig_get_app_list = site.get_app_list

    def new_get_urls():
        custom_urls = [
            path(
                "celery-monitor/",
                site.admin_view(lambda req: views.dashboard_view(req, site)),
                name="celery_monitor_dashboard",
            ),
            path(
                "celery-monitor/recent-tasks",
                site.admin_view(views.recent_tasks_view),
                name="celery_monitor_recent_tasks",
            ),
            path(
                "celery-monitor/status-counts-overall",
                site.admin_view(views.status_counts_overall_view),
                name="celery_monitor_statuses_overall_count",
            ),
            path(
                "celery-monitor/status-counts-last-hr",
                site.admin_view(views.status_counts_last_hour_view),
                name="celery_monitor_statuses_last_hour_count",
            ),
            path(
                "celery-monitor/redis-queue-stats",
                site.admin_view(views.redis_queue_stats_view),
                name="celery_monitor_redis_queue_stats",
            ),
            path(
                "celery-monitor/redis-queue-history",
                site.admin_view(views.redis_queue_history_view),
                name="celery_monitor_redis_queue_history",
            ),
            path(
                "celery-monitor/worker-stats",
                site.admin_view(views.worker_stats_view),
                name="celery_monitor_worker_stats",
            ),
            path(
                "celery-monitor/reserved-tasks",
                site.admin_view(views.reserved_tasks_view),
                name="celery_monitor_reserved_tasks",
            ),
            path(
                "celery-monitor/redis-queue-task-types",
                site.admin_view(views.redis_queue_task_types_view),
                name="celery_monitor_redis_queue_task_types",
            ),
            path(
                "celery-monitor/task-execution-stats",
                site.admin_view(lambda req: views.task_execution_stats_view(req, site)),
                name="celery_monitor_task_execution_stats",
            ),
            path(
                "celery-monitor/task-results",
                site.admin_view(lambda req: views.task_results(req, site)),
                name="celery_monitor_task_results",
            ),
            path(
                "celery-monitor/task/<str:task_id>/",
                site.admin_view(
                    lambda req, task_id: views.task_detail_view(req, site, task_id)
                ),
                name="celery_monitor_task_detail",
            ),
            path(
                "celery-monitor/task/<str:task_id>/kill",
                site.admin_view(
                    lambda req, task_id: views.kill_task(req, site, task_id)
                ),
                name="celery_monitor_task_kill",
            ),
            path(
                "celery-monitor/task/<str:task_id>/revoke",
                site.admin_view(
                    lambda req, task_id: views.revoke_task(req, site, task_id)
                ),
                name="celery_monitor_task_revoke",
            ),
            path(
                "celery-monitor/queues/clear-all/",
                site.admin_view(lambda req: views.clear_all_queues(req, site)),
                name="celery_monitor_clear_all_queues",
            ),
            path(
                "celery-monitor/queues/<str:queue_name>/clear/",
                site.admin_view(
                    lambda req, queue_name: views.clear_queue(req, site, queue_name)
                ),
                name="celery_monitor_clear_queue",
            ),
        ]
        return custom_urls + _orig_get_urls()

    _orig_supports_app_label = (
        "app_label" in inspect.signature(_orig_get_app_list).parameters
    )

    site.get_urls = new_get_urls

    def new_get_app_list(request, app_label=None):
        if _orig_supports_app_label:
            app_list = _orig_get_app_list(request, app_label=app_label)
        else:
            app_list = _orig_get_app_list(request)
        if app_label is None or app_label == "celery_monitor":
            dashboard_url = reverse(f"{site.name}:celery_monitor_dashboard")
            execution_stats_url = reverse(
                f"{site.name}:celery_monitor_task_execution_stats"
            )
            task_results_url = reverse(f"{site.name}:celery_monitor_task_results")

            models = [
                {
                    "name": "Dashboard",
                    "object_name": "CeleryMonitorDashboard",
                    "admin_url": dashboard_url,
                    "view_only": True,
                },
                {
                    "name": "Task Results",
                    "object_name": "CeleryMonitorTaskResults",
                    "admin_url": task_results_url,
                    "view_only": True,
                },
                {
                    "name": "Task Execution Stats",
                    "object_name": "CeleryMonitorTaskExecutionStats",
                    "admin_url": execution_stats_url,
                    "view_only": True,
                },
            ]
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
