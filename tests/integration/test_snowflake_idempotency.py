import os

import pytest

from airline_warehouse.config import Settings
from airline_warehouse.month import YearMonth
from airline_warehouse.pipeline import MonthlyPipeline
from airline_warehouse.warehouse import SnowflakeWarehouse


@pytest.mark.integration
def test_forced_second_load_does_not_change_fact_count() -> None:
    if os.getenv("RUN_SNOWFLAKE_INTEGRATION") != "1":
        pytest.skip("Set RUN_SNOWFLAKE_INTEGRATION=1 to run the Snowflake idempotency test")
    settings = Settings.from_env()
    warehouse = SnowflakeWarehouse(settings)
    month = YearMonth.parse(os.getenv("IDEMPOTENCY_MONTH", "2024-01"))
    pipeline = MonthlyPipeline(settings, warehouse=warehouse)

    pipeline.run(month, force=True, run_type="IDEMPOTENCY")
    first_count = warehouse.month_counts(month)["fact_record_count"]
    pipeline.run(month, force=True, run_type="IDEMPOTENCY")
    second_count = warehouse.month_counts(month)["fact_record_count"]

    assert first_count > 0
    assert second_count == first_count

