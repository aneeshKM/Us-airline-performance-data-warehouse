from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterator

from airline_warehouse.config import Settings

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SQL_ROOT = PROJECT_ROOT / "sql"
TOKEN = re.compile(r"{{\s*([a-z_]+)\s*}}")


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    return str(value).replace("'", "''")


def template_context(settings: Settings, **values: Any) -> dict[str, str]:
    context = {
        "database": settings.database,
        "raw_schema": settings.raw_schema,
        "staging_schema": settings.staging_schema,
        "analytics_schema": settings.analytics_schema,
        "metadata_schema": settings.metadata_schema,
        "stage": settings.stage,
    }
    context.update({key: sql_literal(value) for key, value in values.items()})
    return context


def render_sql(relative_path: str, context: dict[str, str]) -> str:
    path = SQL_ROOT / relative_path
    source = path.read_text(encoding="utf-8")

    def substitute(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in context:
            raise KeyError(f"No SQL template value supplied for {name!r} in {relative_path}")
        return context[name]

    rendered = TOKEN.sub(substitute, source)
    remaining = TOKEN.findall(rendered)
    if remaining:
        raise ValueError(f"Unresolved SQL tokens in {relative_path}: {remaining}")
    return rendered


def split_sql_statements(script: str) -> Iterator[str]:
    """Split project SQL files, which intentionally contain no semicolons in literals."""
    for statement in script.split(";"):
        cleaned = statement.strip()
        if cleaned:
            yield cleaned

