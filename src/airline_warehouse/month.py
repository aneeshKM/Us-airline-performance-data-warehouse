from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True, order=True)
class YearMonth:
    year: int
    month: int

    def __post_init__(self) -> None:
        if self.year < 1987 or not 1 <= self.month <= 12:
            raise ValueError(f"Invalid BTS source month: {self.year:04d}-{self.month:02d}")

    @classmethod
    def parse(cls, value: str) -> "YearMonth":
        try:
            year_text, month_text = value.strip().split("-", maxsplit=1)
            return cls(int(year_text), int(month_text))
        except (AttributeError, TypeError, ValueError) as exc:
            raise ValueError("Month must use YYYY-MM format") from exc

    @property
    def first_day(self) -> date:
        return date(self.year, self.month, 1)

    @property
    def compact(self) -> str:
        return f"{self.year:04d}{self.month:02d}"

    def next(self) -> "YearMonth":
        if self.month == 12:
            return YearMonth(self.year + 1, 1)
        return YearMonth(self.year, self.month + 1)

    def __str__(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


def month_range(start: YearMonth, end: YearMonth) -> list[YearMonth]:
    if start > end:
        raise ValueError("Start month must not be after end month")
    result: list[YearMonth] = []
    current = start
    while current <= end:
        result.append(current)
        current = current.next()
    return result

