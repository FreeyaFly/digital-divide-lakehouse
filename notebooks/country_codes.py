"""Natural Earth admin-0 units → the country codes used by the World Bank (WDI).

A plain Python file (not a notebook): Notebook 34 imports it from the same workspace folder,
and tests/notebooks/test_country_codes.py tests it locally.

Natural Earth 1:10m Admin 0 (v5.1.2) has 258 units. Its codes need three corrections:
- `ISO_A3` is "-99" for 22 units, including France and Norway; `ISO_A3_EH` fills most of them.
- `WB_A3` holds old World Bank codes (ROM, ZAR, TMP) and is not used.
- A few units are coded differently by WDI (Kosovo XKX, Channel Islands CHI) or reported as
  part of another country (Åland in Finland): WDI_OVERRIDES.

Units without any ISO code (de facto or disputed areas such as Somaliland, N. Cyprus, Siachen)
keep Natural Earth's own ADM0_A3 code. They are not attributed to another country: the
boundaries follow Natural Earth's de facto lines, and Gold decides how to treat them.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

MISSING = "-99"

# Natural Earth ADM0_A3 → WDI country code.
WDI_OVERRIDES = {
    "KOS": "XKX",  # Kosovo: WDI uses the user-assigned code XKX
    "JEY": "CHI",  # Jersey: WDI reports the Channel Islands together
    "GGY": "CHI",  # Guernsey
    "ALD": "FIN",  # Åland: part of Finland in WDI
}

# Units left out of the H3 tessellation, with the reason.
EXCLUDED = {
    "ATA": "Antarctica spans all longitudes and contains the South Pole, which H3 tessellation "
           "of a lon/lat polygon cannot represent; it has no WDI data either",
}

# Country codes expected to have a boundary but no WDI country (checked against v5.1.2 and the
# 217 WDI countries on 2026-10-11). Notebook 34 fails if the actual set differs.
EXPECTED_NOT_IN_WDI = frozenset({
    # Territories and states WDI does not report separately
    "AIA", "ATF", "BLM", "COK", "ESH", "FLK", "HMD", "IOT", "MSR", "NFK", "NIU", "PCN", "SGS",
    "SHN", "SPM", "TWN", "UMI", "VAT", "WLF",
    # De facto or disputed units without an ISO code (Natural Earth ADM0_A3)
    "BJN", "BRT", "CNM", "CYN", "ESB", "KAS", "PGA", "SCR", "SER", "SOL", "SPI", "USG", "WSB",
})


def country_code(props: Mapping[str, str]) -> tuple[str, str]:
    """(country code, where it came from) for one Natural Earth feature's properties.

    The origin is "override" (WDI_OVERRIDES), "iso" (ISO_A3, else ISO_A3_EH) or "ne_adm0"
    (no ISO code: Natural Earth's own ADM0_A3).
    """
    adm0 = props["ADM0_A3"]
    if adm0 in WDI_OVERRIDES:
        return WDI_OVERRIDES[adm0], "override"
    for field in ("ISO_A3", "ISO_A3_EH"):
        code = props.get(field)
        if code and code != MISSING:
            return code, "iso"
    return adm0, "ne_adm0"


def resolve(features: Iterable[Mapping]) -> list[dict]:
    """One row per Natural Earth feature, with its country code and origin of the code.

    Raises ValueError if a feature code is duplicated, or if a fallback ADM0_A3 code collides
    with an ISO code of another unit (two different territories would merge silently).
    """
    rows, adm0_seen = [], set()
    for feature in features:
        props = feature["properties"]
        adm0 = props["ADM0_A3"]
        if adm0 in adm0_seen:
            raise ValueError(f"Duplicate ADM0_A3 {adm0}")
        adm0_seen.add(adm0)
        code, source = country_code(props)
        rows.append({"ne_adm0_a3": adm0, "country_code": code, "code_source": source,
                     "name": props["NAME"], "excluded": adm0 in EXCLUDED})

    iso_codes = {r["country_code"] for r in rows if r["code_source"] != "ne_adm0"}
    collisions = sorted(r["ne_adm0_a3"] for r in rows
                        if r["code_source"] == "ne_adm0" and r["country_code"] in iso_codes)
    if collisions:
        raise ValueError(f"Fallback ADM0_A3 codes collide with ISO codes: {collisions}")
    return rows
