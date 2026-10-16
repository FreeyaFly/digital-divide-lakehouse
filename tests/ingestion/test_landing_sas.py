from datetime import UTC, datetime, timedelta

import pytest
import responses

from ingestion import landing_sas
from ingestion.landing_sas import MAX_HOURS, SasError, check_sas, rotate, sas_window

NOW = datetime(2026, 10, 14, 7, 0, tzinfo=UTC)
FAKE_SAS = "sv=2025-01-01&sr=c&sp=rl&sig=not-a-real-signature"


def test_window_starts_before_now_and_lasts_the_requested_hours():
    window = sas_window(NOW, 144)
    assert window.start == NOW - timedelta(minutes=15)
    assert window.expiry == NOW + timedelta(hours=144)


def test_window_fits_in_a_user_delegation_key():
    window = sas_window(NOW, MAX_HOURS)
    assert window.expiry - window.start <= timedelta(days=7)


@pytest.mark.parametrize("hours", [0, MAX_HOURS + 1])
def test_window_rejects_out_of_range_hours(hours):
    with pytest.raises(SasError):
        sas_window(NOW, hours)


def test_window_rejects_naive_time():
    with pytest.raises(SasError):
        sas_window(datetime(2026, 10, 14, 7, 0), 24)


def test_rotate_signs_checks_then_stores():
    calls = []
    expiry = rotate(
        "acct",
        "landing",
        "ddlake",
        "adls-landing-sas",
        24,
        NOW,
        sign=lambda a, c, w: calls.append(("sign", a, c)) or FAKE_SAS,
        check=lambda a, c, s: calls.append(("check", s)),
        store=lambda sc, k, v: calls.append(("store", sc, k, v)),
    )
    assert calls == [
        ("sign", "acct", "landing"),
        ("check", FAKE_SAS),
        ("store", "ddlake", "adls-landing-sas", FAKE_SAS),
    ]
    assert expiry == NOW + timedelta(hours=24)


def test_rotate_keeps_the_old_secret_when_the_new_sas_fails():
    stored = []

    def failing_check(account, container, sas):
        raise SasError("HTTP 403")

    with pytest.raises(SasError):
        rotate(
            "acct",
            "landing",
            "ddlake",
            "adls-landing-sas",
            24,
            NOW,
            sign=lambda a, c, w: FAKE_SAS,
            check=failing_check,
            store=lambda sc, k, v: stored.append(v),
        )
    assert stored == []


@responses.activate
def test_check_sas_accepts_a_listable_container():
    responses.get("https://acct.dfs.core.windows.net/landing", json={"paths": []})
    check_sas("acct", "landing", FAKE_SAS)
    assert "sig=not-a-real-signature" in responses.calls[0].request.url


@responses.activate
def test_check_sas_reports_the_status_but_not_the_sas():
    responses.get("https://acct.dfs.core.windows.net/landing", status=403)
    with pytest.raises(SasError) as error:
        check_sas("acct", "landing", FAKE_SAS)
    assert "403" in str(error.value)
    assert "sig=" not in str(error.value)


def test_main_never_prints_the_sas(monkeypatch, capsys):
    monkeypatch.setattr(landing_sas, "sign_sas", lambda a, c, w: FAKE_SAS)
    monkeypatch.setattr(landing_sas, "check_sas", lambda a, c, s: None)
    monkeypatch.setattr(landing_sas, "store_secret", lambda sc, k, v: None)
    assert landing_sas.main(["--storage-account", "acct", "--hours", "24"]) == 0
    out = capsys.readouterr()
    assert "rotated secret ddlake/adls-landing-sas" in out.out
    assert "sig=" not in out.out + out.err


def test_main_requires_a_storage_account(monkeypatch):
    monkeypatch.delenv("DDLAKE_STORAGE_ACCOUNT", raising=False)
    with pytest.raises(SystemExit):
        landing_sas.main([])
