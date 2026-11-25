import os
import re
from datetime import date

MONTH_PATTERN = r"^[0-9]{4}-(0[1-9]|1[0-2])$"


def configured_months() -> tuple[str, str]:
    """Read range overrides and accept the original single-year variables."""
    year: str = os.environ.get("TLC_ANALYSIS_YEAR", "2026")
    last_month: str = os.environ.get("TLC_LAST_MONTH", "9").zfill(2)
    return (
        os.environ.get("TLC_START_MONTH", f"{year}-01"),
        os.environ.get("TLC_END_MONTH", f"{year}-{last_month}"),
    )


def month_range(start_month: str, end_month: str) -> tuple[date, date, tuple[str, ...]]:
    """Return inclusive source months and the exclusive end of the pickup period."""
    if not all(re.fullmatch(MONTH_PATTERN, month) for month in (start_month, end_month)):
        raise ValueError("Start and end months must use YYYY-MM")

    start: date = date.fromisoformat(f"{start_month}-01")
    end: date = date.fromisoformat(f"{end_month}-01")
    if end < start:
        raise ValueError("End month must be on or after the start month")

    months: list[str] = []
    current: date = start
    while current <= end:
        months.append(current.isoformat()[:7])
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)

    return start, current, tuple(months)
