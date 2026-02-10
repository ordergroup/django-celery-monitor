from django.apps import AppConfig


class CeleryMonitorConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "celery_monitor"
    verbose_name = "Celery Monitor"

    def ready(self):
        from django.contrib import admin

        from .admin import patch_admin_site

        patch_admin_site(admin.site)
