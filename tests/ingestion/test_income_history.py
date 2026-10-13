import io
from datetime import date

import pytest
from openpyxl import Workbook

from ingestion.worldbank.income_history import (
    SHEET,
    Classification,
    IncomeHistoryError,
    fiscal_year,
    parse,
    read_workbook,
    write_seed,
)

HEADER = (None, "Bank's fiscal year:", "FY99", "FY00", "FY27")
ROWS = [
    (None, "World Bank Analytical Classifications"),
    HEADER,
    (None, "Data for calendar year :", 1997, 1998, 2025),
    (None, "Low income (L)", "<= 785", "<= 760", "<= 1,135"),
    ("NLD", "Netherlands", "H", "H", "H"),
    ("VEN", "Venezuela, RB", "UM", None, "LM"),
    ("YEM", "Yemen, Rep.", "LM*", "L", ".."),
    ("YUGf", "Yugoslavia, Fed. Rep.", "LM", "", None),
    (None, "Note: Income classifications are set each year on July 1"),
]


def test_fiscal_year_labels():
    assert [fiscal_year(x) for x in ("FY89", "FY99", "FY00", "FY27")] == [1989, 1999, 2000, 2027]
    with pytest.raises(IncomeHistoryError):
        fiscal_year("1999")


def test_valid_from_is_july_first_of_previous_calendar_year():
    assert Classification("NLD", "Netherlands", 2027, "HIC").valid_from == date(2026, 7, 1)


def test_parse_maps_codes_and_skips_unclassified_years():
    got = {(c.country_code, c.fiscal_year): c.income_code for c in parse(ROWS)}
    assert got == {
        ("NLD", 1999): "HIC", ("NLD", 2000): "HIC", ("NLD", 2027): "HIC",
        ("VEN", 1999): "UMC", ("VEN", 2027): "LMC",          # FY00 not classified: no row
        ("YEM", 1999): "LMC", ("YEM", 2000): "LIC",           # LM* is a footnoted LM; ".." no data
        ("YUGf", 1999): "LMC",                                # former economy, lower-case suffix
    }


def test_parse_rejects_unknown_code():
    with pytest.raises(IncomeHistoryError, match="unknown income code 'X'"):
        list(parse([HEADER, ("NLD", "Netherlands", "X", "H", "H")]))


def test_parse_requires_header():
    with pytest.raises(IncomeHistoryError, match="Header row"):
        list(parse([("NLD", "Netherlands", "H")]))


def _xlsx(rows, sheet=SHEET) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = sheet
    for row in rows:
        ws.append(list(row))
    buffer = io.BytesIO()
    wb.save(buffer)
    return buffer.getvalue()


def test_read_workbook_and_write_seed(tmp_path):
    rows = read_workbook(_xlsx(ROWS))
    out = tmp_path / "seed.csv"
    write_seed(rows, out)
    lines = out.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "country_code,country_name,fiscal_year,valid_from,income_code"
    assert lines[1] == "NLD,Netherlands,1999,1998-07-01,HIC"
    assert len(lines) == 1 + 8


def test_read_workbook_requires_sheet():
    with pytest.raises(IncomeHistoryError, match="missing"):
        read_workbook(_xlsx(ROWS, sheet="Other"))


def test_read_workbook_rejects_duplicate_rows():
    with pytest.raises(IncomeHistoryError, match="Duplicate"):
        read_workbook(_xlsx(ROWS + [("NLD", "Netherlands", "H", "H", "H")]))
