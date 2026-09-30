import pytest

from airline_warehouse.month import YearMonth, month_range


def test_year_month_parsing_and_range_cross_year() -> None:
    assert [str(item) for item in month_range(YearMonth.parse("2023-11"), YearMonth(2024, 2))] == [
        "2023-11",
        "2023-12",
        "2024-01",
        "2024-02",
    ]


@pytest.mark.parametrize("value", ["2024", "2024-00", "2024-13", "not-a-month"])
def test_invalid_year_month(value: str) -> None:
    with pytest.raises(ValueError):
        YearMonth.parse(value)


def test_range_rejects_reverse_order() -> None:
    with pytest.raises(ValueError, match="Start month"):
        month_range(YearMonth(2024, 2), YearMonth(2024, 1))

