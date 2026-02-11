"""Factory definitions for tests using factory_boy."""

from datetime import timedelta

import factory
from django.utils import timezone


class TaskResultFactory(factory.django.DjangoModelFactory):
    """Factory for django_celery_results.models.TaskResult."""

    class Meta:
        model = "django_celery_results.TaskResult"
        skip_postgeneration_save = True

    task_id = factory.Sequence(lambda n: f"test-task-{n}")
    task_name = factory.Faker(
        "random_element", elements=["tasks.add", "tasks.process", "tasks.send_email"]
    )
    status = factory.Faker(
        "random_element", elements=["SUCCESS", "FAILURE", "PENDING", "STARTED"]
    )
    worker = factory.Faker(
        "random_element", elements=["worker1@host", "worker2@host", "worker3@host"]
    )
    content_type = "application/json"
    content_encoding = "utf-8"
    result = factory.LazyAttribute(lambda obj: '{"result": "ok"}' if obj.status == "SUCCESS" else None)
    
    # Set dates with consistent execution time (10 seconds by default)
    # These will use timezone.now() which can be frozen with time-machine
    date_created = factory.LazyFunction(lambda: timezone.now() - timedelta(minutes=30, seconds=11))
    date_started = factory.LazyFunction(lambda: timezone.now() - timedelta(minutes=30, seconds=10))
    date_done = factory.LazyFunction(lambda: timezone.now() - timedelta(minutes=30))
    
    traceback = factory.LazyAttribute(
        lambda obj: "Traceback..." if obj.status == "FAILURE" else None
    )
    meta = "{}"


class CeleryStatusCountFactory(factory.django.DjangoModelFactory):
    """Factory for CeleryStatusCount materialized view."""

    class Meta:
        model = "celery_monitor.CeleryStatusCount"

    status = factory.Sequence(lambda n: f"STATUS_{n}")
    count = factory.Faker("random_int", min=0, max=1000)
