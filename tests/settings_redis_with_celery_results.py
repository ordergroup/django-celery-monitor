"""Django settings for tests with Redis broker and django-celery-results."""

SECRET_KEY = "test-secret-key"

DEBUG = True

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django.contrib.admin",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django_celery_results",
    "celery_monitor",
]

MIDDLEWARE = []

ROOT_URLCONF = ""

USE_TZ = True

# Celery settings for testing with Redis broker and django-celery-results
CELERY_BROKER_URL = "redis://localhost:6379/0"
CELERY_RESULT_BACKEND = "django-db"
