# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Reusable Django app (`celery_monitor`, packaged as `django-celery-monitor`) that adds a Celery monitoring dashboard inside Django admin. Supports Python 3.10+, Django 3.2–5.2, Celery 5. Version lives in `pyproject.toml`.

## Commands

```bash
uv sync --all-extras --dev                       # install dev deps

uv run pytest tests/                             # default settings (tests.settings)
uv run pytest tests/test_filters.py::TestX::test_y   # single test
uv run pytest tests/ --ds=tests.settings_with_celery_results
uv run pytest tests/ --ds=tests.settings_redis
uv run pytest tests/ --ds=tests.settings_redis_with_celery_results

uv run tox                                       # full matrix (python × django × backend config)
uv run tox -e py312-django52-redis               # one env

uv run ruff check celery_monitor tests
uv run ruff format celery_monitor tests
uv run djlint --check celery_monitor/templates   # CI also checks template formatting
```

CI (`.github/workflows/ci.yml`) runs all four settings modules per Python version, a coverage job across settings, and ruff + djlint checks. Behaviour differs per settings module, so a change touching backend selection should be tested with all four `--ds` variants.

## Architecture

### Admin integration (no urls.py)
`CeleryMonitorConfig.ready()` (`apps.py`) calls `patch_admin_site(admin.site)` in `admin.py`, which monkey-patches `site.get_urls` (all monitor routes, named `admin:celery_monitor_*`, wrapped in `site.admin_view`) and `site.get_app_list` (injects fake "models" like Dashboard / Task Results into the admin sidebar). New views must be registered there. Every view in `views.py` must be decorated with `@permission_required(VIEW_PERMISSION, raise_exception=True)`, or `MANAGE_PERMISSION` for state-changing actions (`permissions.py`). Action buttons in templates are wrapped in `{% if perms.celery_monitor.manage_celery_monitor %}`. `ready()` also imports `signals` and `redis.tasks` so Celery signal handlers and shared tasks are registered.

### Three pluggable backend layers, each chosen by a factory
All selection is driven by `CELERY_MONITOR_RESULTS_BACKEND` (`"celery_results"`, `"redis"`, or unset → auto; parsed via `enums.BackendType`) plus `utils.has_django_celery_result()` / `utils.has_redis()`:

- **Results monitor** — `results_monitor.get_results_monitor()` returns a `CeleryResultsMonitor` (ABC in `results_monitor/base.py`) implementation:
  `DjangoCeleryResultsMonitor` (queries `django_celery_results` ORM) → `RedisComputedResultsMonitor` if the Redis key `last_calculation_timestamp` exists, else `RedisResultsMonitor` (raw Redis data) → `WorkersCeleryResultsMonitor` (live `celery inspect` only). The chosen class is cached at module level for 30s (`_cached_monitor_class`); `tests/conftest.py` resets that cache between tests.
- **Signals backend** — `signals_backend.get_signals_backend()`; `signals.py` forwards Celery task signals (publish/prerun/postrun/failure/retry/revoked) to it. `RedisSignalsResultBackend` writes the monitor's own Redis schema; `Noop` is used when django-celery-results stores results itself.
- **Queue monitor** — `queue_monitor.get_queue_monitor()`: `RedisMonitor` (broker queue lengths, task types in queues, length history streams) or base `QueueMonitor`.

When adding a feature to the results monitor, add the abstract method to `CeleryResultsMonitor` and implement it in every subclass. Shared return types are dataclasses in `models.py` (the only real model there is the unmanaged `CeleryStatusCount`).

### Redis schema
`celery_monitor/redis/`: `client.get_results_client()` connects to `result_backend` or falls back to `broker_url`. All keys are defined in `redis/keys.py` under the `celery:monitor:` prefix (task hashes, recent-task index, per-task/queue indexes, bucketed stats, rollups, throughput). `redis/tasks.py` holds shared tasks: `calculate_celery_stats` (incremental pre-aggregation into stat buckets, meant for Celery Beat; `overwrite=True` recomputes), `prune_stale_recent_tasks`, `clear_celery_stats`, etc. TTLs come from `DJANGO_CELERY_MONITOR_TASK_DATA_TTL` / `DJANGO_CELERY_MONITOR_STATS_TTL`; payloads are lz4-compressed.

### Frontend
Server-rendered admin templates + HTMX (loaded from CDN) + small static JS files for charts/date pickers. Top-level pages (`dashboard.html`, `task_detail.html`, `queue_detail.html`, ...) extend `admin/base_site.html` — keep `{{ block.super }}` when overriding blocks like `extrahead`/`extrastyle` so host-project admin customizations still load; each dashboard widget is a partial in `templates/celery_monitor/partials/` fetched by its own view via `hx-get` and auto-refreshed (`DJANGO_CELERY_MONITOR_DASHBOARD_REFRESH_INTERVAL`). Partial-returning views live alongside page views in `views.py`.

### Migrations
`0001` creates, only on PostgreSQL with django-celery-results, a `celery_status_counts` materialized view plus refresh function/trigger backing `CeleryStatusCount`; it's a no-op elsewhere.

## Testing notes
- Tests use in-memory SQLite and `fakeredis`; Redis-backed monitors are tested by patching `get_results_client` in the module under test (e.g. `celery_monitor.results_monitor.redis_results.get_results_client`) to return a `FakeRedis(decode_responses=True)`.
- `factory-boy` factories are in `tests/factories.py`; `time-machine` is used for time-dependent stats.
- Tests depending on optional backends are guarded with `pytest.mark.skipif` (e.g. `HAS_REDIS`, django-celery-results availability) so the whole suite runs under every settings module.

## Conventions

- Write code and comments in English.
- Keep indentation depth to 3 levels or less; ruff's complexity check (`C90`) is not enabled in `pyproject.toml`, so this isn't enforced by tooling.
- Don't put business logic in views: keep it in the backend layers (results/queue monitors, signals backends, `redis/` modules). Views only call a factory, pick a template and build the context.
- Celery tasks should return a meaningful value and raise exceptions on error conditions rather than swallowing them.
- If unsure about a preferred solution or project-specific context (e.g. conflicting conventions, ambiguous requirements, a design choice with no clear precedent in the codebase), ask the user via the question-asking tool instead of guessing.
- Don't write comments that explain the change being made or the agent's decisions (e.g. "changed to use X", "removed per review") — comments must document the code for its next reader. Explain your reasoning in the conversation/summary instead, not in the code.
- Keep docstrings terse: state what the function returns/does and, in short clauses, the one non-obvious fact a reader needs (a unit, an invariant, why a naive alternative would be wrong) — don't spell out full justification paragraphs. If the reasoning doesn't fit in a clause or two, it belongs in the PR description/conversation, not the docstring.
- `git add` a newly created file that belongs in the repo (module, test, migration, template, static file, doc) as soon as it's written, so it shows up in `git diff` and can't be missed at commit time. This is staging only — still don't commit unless asked. Skip it for scratch/throwaway files (put those in the scratchpad directory instead) and for anything `.gitignore` already covers.
