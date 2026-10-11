import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "notebooks"))

from country_codes import (  # noqa: E402
    EXCLUDED,
    EXPECTED_NOT_IN_WDI,
    WDI_OVERRIDES,
    country_code,
    resolve,
)


def props(adm0, iso="-99", iso_eh="-99", name=None):
    return {"ADM0_A3": adm0, "ISO_A3": iso, "ISO_A3_EH": iso_eh, "NAME": name or adm0}


def feature(*args, **kwargs):
    return {"properties": props(*args, **kwargs)}


def test_iso_a3_is_used_when_present():
    assert country_code(props("NLD", "NLD", "NLD")) == ("NLD", "iso")


def test_iso_a3_eh_fills_missing_iso_a3():
    # France and Norway have ISO_A3 = -99 in Natural Earth
    assert country_code(props("FRA", "-99", "FRA")) == ("FRA", "iso")
    assert country_code(props("NOR", "-99", "NOR")) == ("NOR", "iso")


def test_dependency_with_parent_iso_a3_eh_maps_to_parent():
    assert country_code(props("KAB", "-99", "KAZ", "Baikonur")) == ("KAZ", "iso")


def test_overrides_win_over_iso_codes():
    assert country_code(props("KOS")) == ("XKX", "override")
    assert country_code(props("JEY", "JEY", "JEY")) == ("CHI", "override")
    assert country_code(props("ALD", "ALA", "ALA")) == ("FIN", "override")


def test_unit_without_iso_code_keeps_ne_code():
    assert country_code(props("SOL", name="Somaliland")) == ("SOL", "ne_adm0")


def test_resolve_rows():
    rows = resolve([feature("NLD", "NLD", "NLD"), feature("SOL"), feature("ATA", "ATA", "ATA")])
    assert [(r["country_code"], r["code_source"], r["excluded"]) for r in rows] == [
        ("NLD", "iso", False), ("SOL", "ne_adm0", False), ("ATA", "iso", True)]


def test_resolve_rejects_duplicate_feature_codes():
    with pytest.raises(ValueError, match="Duplicate ADM0_A3 NLD"):
        resolve([feature("NLD", "NLD", "NLD"), feature("NLD", "NLD", "NLD")])


def test_resolve_rejects_fallback_code_colliding_with_iso_code():
    # A unit without ISO code whose ADM0_A3 equals another unit's ISO code would merge them
    with pytest.raises(ValueError, match="collide"):
        resolve([feature("ABC", "BEL", "BEL"), feature("BEL")])


def test_constants_are_consistent():
    assert not set(WDI_OVERRIDES.values()) & EXPECTED_NOT_IN_WDI
    assert not set(EXCLUDED) & EXPECTED_NOT_IN_WDI
