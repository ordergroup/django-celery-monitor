from django.db import models


class CeleryStatusCount(models.Model):
    """Read-only model for the celery_status_counts materialized view."""

    status = models.CharField(max_length=50, primary_key=True)
    count = models.IntegerField()

    class Meta:
        managed = False
        db_table = "celery_status_counts"
