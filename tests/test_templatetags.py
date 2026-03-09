from celery_monitor.templatetags.celery_monitor_tags import format_duration


class TestFormatDuration:
    def test_none_returns_dash(self):
        assert format_duration(None) == "-"

    def test_zero(self):
        assert format_duration(0.0) == "0.000s"

    def test_subsecond(self):
        assert format_duration(0.5) == "0.500s"

    def test_exactly_one_second(self):
        assert format_duration(1.0) == "1.00s"

    def test_seconds(self):
        assert format_duration(5.5) == "5.50s"

    def test_just_under_one_minute(self):
        assert format_duration(59.99) == "59.99s"

    def test_exactly_one_minute(self):
        assert format_duration(60.0) == "1m 0s"

    def test_minutes_and_seconds(self):
        assert format_duration(150.0) == "2m 30s"

    def test_just_under_one_hour(self):
        result = format_duration(3599.0)
        assert result == "59m 59s"

    def test_exactly_one_hour(self):
        assert format_duration(3600.0) == "1h 0m"

    def test_hours_and_minutes(self):
        assert format_duration(5400.0) == "1h 30m"

    def test_multiple_hours(self):
        assert format_duration(7200.0) == "2h 0m"
