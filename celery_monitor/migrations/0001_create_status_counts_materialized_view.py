from django.db import migrations


def is_postgres(schema_editor):
    return schema_editor.connection.vendor == "postgresql"


def create_materialized_view(apps, schema_editor):
    if not is_postgres(schema_editor):
        return

    schema_editor.execute(
        """
        CREATE MATERIALIZED VIEW celery_status_counts AS
        SELECT 
            status,
            COUNT(id) as count
        FROM django_celery_results_taskresult
        GROUP BY status
        ORDER BY status;
        
        CREATE UNIQUE INDEX celery_status_counts_status_idx 
        ON celery_status_counts(status);
        """
    )


def drop_materialized_view(apps, schema_editor):
    """Drop materialized view only for PostgreSQL."""
    if not is_postgres(schema_editor):
        return

    schema_editor.execute("DROP MATERIALIZED VIEW IF EXISTS celery_status_counts;")


def create_refresh_function(apps, schema_editor):
    """Create trigger function only for PostgreSQL."""
    if not is_postgres(schema_editor):
        return

    schema_editor.execute(
        """
        CREATE OR REPLACE FUNCTION refresh_celery_status_counts()
        RETURNS TRIGGER AS $$
        BEGIN
            REFRESH MATERIALIZED VIEW CONCURRENTLY celery_status_counts;
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
        """
    )


def drop_refresh_function(apps, schema_editor):
    """Drop trigger function only for PostgreSQL."""
    if not is_postgres(schema_editor):
        return

    schema_editor.execute("DROP FUNCTION IF EXISTS refresh_celery_status_counts();")


def create_trigger(apps, schema_editor):
    """Create trigger only for PostgreSQL."""
    if not is_postgres(schema_editor):
        return

    schema_editor.execute(
        """
        CREATE TRIGGER refresh_celery_status_counts_trigger
        AFTER INSERT OR UPDATE OR DELETE ON django_celery_results_taskresult
        FOR EACH STATEMENT
        EXECUTE FUNCTION refresh_celery_status_counts();
        """
    )


def drop_trigger(apps, schema_editor):
    """Drop trigger only for PostgreSQL."""
    if not is_postgres(schema_editor):
        return

    schema_editor.execute(
        """
        DROP TRIGGER IF EXISTS refresh_celery_status_counts_trigger 
        ON django_celery_results_taskresult;
        """
    )


class Migration(migrations.Migration):
    dependencies = [
        ("django_celery_results", "__latest__"),
    ]

    operations = [
        # Create the materialized view (PostgreSQL only)
        migrations.RunPython(
            code=create_materialized_view,
            reverse_code=drop_materialized_view,
        ),
        # Create function to refresh the materialized view (PostgreSQL only)
        migrations.RunPython(
            code=create_refresh_function,
            reverse_code=drop_refresh_function,
        ),
        # Create trigger to auto-refresh on INSERT/UPDATE/DELETE (PostgreSQL only)
        migrations.RunPython(
            code=create_trigger,
            reverse_code=drop_trigger,
        ),
    ]

