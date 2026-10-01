from airline_warehouse.config import Settings
from airline_warehouse.sql import render_sql, split_sql_statements, template_context


def _settings() -> Settings:
    return Settings(account="a", user="u", password="p")


def test_setup_template_has_no_unresolved_tokens() -> None:
    rendered = render_sql("setup/01_objects.sql", template_context(_settings()))
    assert "{{" not in rendered
    assert "AIRLINE_PERFORMANCE.ANALYTICS" not in rendered  # schemas use active database here
    assert len(list(split_sql_statements(rendered))) >= 10


def test_fact_load_uses_merge_and_on_time_rule_is_explicit() -> None:
    context = template_context(_settings(), source_month="2024-01")
    fact_sql = render_sql("facts/merge_fact_flights.sql", context)
    staging_sql = render_sql("staging/transform_month.sql", context)
    assert "MERGE INTO AIRLINE_PERFORMANCE.ANALYTICS.FACT_FLIGHTS" in fact_sql
    assert "ARRIVAL_DELAY_MINUTES <= 15" in staging_sql
    assert "ROW_NUMBER() OVER" in staging_sql


def test_raw_loader_replaces_only_the_requested_month() -> None:
    context = template_context(
        _settings(),
        source_month="2024-01",
        source_file="flights_2024_01.csv",
        checksum="abc",
        run_id="run-id",
        stage_prefix="2024-01/abc",
    )
    sql = render_sql("raw/load_month.sql", context)
    assert sql.startswith("USE SCHEMA AIRLINE_PERFORMANCE.RAW")
    assert "WHERE SOURCE_MONTH = TO_DATE('2024-01-01', 'YYYY-MM-DD')" in sql
    assert "FORCE = TRUE" in sql


def test_statement_splitter_ignores_trailing_comments() -> None:
    statements = list(split_sql_statements("SELECT 1;\n-- instructions only\n"))
    assert statements == ["SELECT 1"]
