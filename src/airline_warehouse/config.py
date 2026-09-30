from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from airline_warehouse.month import YearMonth

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_$]*$")


def validate_identifier(value: str, setting: str) -> str:
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"{setting} must be an unquoted Snowflake identifier")
    return value.upper()


@dataclass(frozen=True)
class Settings:
    account: str
    user: str
    password: str
    database: str = "AIRLINE_PERFORMANCE"
    warehouse: str = "AIRLINE_ETL_WH"
    role: str = "AIRLINE_ETL_ROLE"
    raw_schema: str = "RAW"
    staging_schema: str = "STAGING"
    analytics_schema: str = "ANALYTICS"
    metadata_schema: str = "METADATA"
    stage: str = "FLIGHT_FILES"
    raw_data_dir: Path = Path("data/raw")
    bts_base_url: str = "https://transtats.bts.gov/PREZIP"
    request_timeout_seconds: int = 120
    start_month: YearMonth = YearMonth(2023, 1)
    end_month: YearMonth = YearMonth(2024, 12)

    def __post_init__(self) -> None:
        for field_name in (
            "database",
            "warehouse",
            "role",
            "raw_schema",
            "staging_schema",
            "analytics_schema",
            "metadata_schema",
            "stage",
        ):
            normalized = validate_identifier(getattr(self, field_name), field_name)
            object.__setattr__(self, field_name, normalized)
        object.__setattr__(self, "raw_data_dir", Path(self.raw_data_dir).expanduser().resolve())
        if self.request_timeout_seconds <= 0:
            raise ValueError("REQUEST_TIMEOUT_SECONDS must be positive")
        if self.start_month > self.end_month:
            raise ValueError("PIPELINE_START_MONTH must not be after PIPELINE_END_MONTH")

    @classmethod
    def from_env(cls, *, require_credentials: bool = True) -> "Settings":
        load_dotenv()
        account = os.getenv("SNOWFLAKE_ACCOUNT", "")
        user = os.getenv("SNOWFLAKE_USER", "")
        password = os.getenv("SNOWFLAKE_PASSWORD", "")
        if require_credentials:
            missing = [
                name
                for name, value in (
                    ("SNOWFLAKE_ACCOUNT", account),
                    ("SNOWFLAKE_USER", user),
                    ("SNOWFLAKE_PASSWORD", password),
                )
                if not value
            ]
            if missing:
                raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

        return cls(
            account=account,
            user=user,
            password=password,
            database=os.getenv("SNOWFLAKE_DATABASE", "AIRLINE_PERFORMANCE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "AIRLINE_ETL_WH"),
            role=os.getenv("SNOWFLAKE_ROLE", "AIRLINE_ETL_ROLE"),
            raw_schema=os.getenv("SNOWFLAKE_RAW_SCHEMA", "RAW"),
            staging_schema=os.getenv("SNOWFLAKE_STAGING_SCHEMA", "STAGING"),
            analytics_schema=os.getenv("SNOWFLAKE_ANALYTICS_SCHEMA", "ANALYTICS"),
            metadata_schema=os.getenv("SNOWFLAKE_METADATA_SCHEMA", "METADATA"),
            stage=os.getenv("SNOWFLAKE_STAGE", "FLIGHT_FILES"),
            raw_data_dir=Path(os.getenv("RAW_DATA_DIR", "data/raw")),
            bts_base_url=os.getenv("BTS_BASE_URL", "https://transtats.bts.gov/PREZIP").rstrip(
                "/"
            ),
            request_timeout_seconds=int(os.getenv("REQUEST_TIMEOUT_SECONDS", "120")),
            start_month=YearMonth.parse(os.getenv("PIPELINE_START_MONTH", "2023-01")),
            end_month=YearMonth.parse(os.getenv("PIPELINE_END_MONTH", "2024-12")),
        )

    def connection_parameters(self) -> dict[str, str]:
        return {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "warehouse": self.warehouse,
            "database": self.database,
            "role": self.role,
            "application": "us_airline_performance_warehouse",
        }

