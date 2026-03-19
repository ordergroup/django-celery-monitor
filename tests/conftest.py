import pytest


@pytest.fixture(autouse=True)
def reset_results_monitor_cache():
    """Reset the module-level monitor class cache between tests."""
    import celery_monitor.results_monitor as rm

    rm._cached_monitor_class = None
    rm._cache_expires_at = 0.0
    yield
    rm._cached_monitor_class = None
    rm._cache_expires_at = 0.0
