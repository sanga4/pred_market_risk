import pytest

import institutional_staging_repair as repair
import institutional_staging_validate as validate


def test_staging_repair_rejects_unsafe_database_aliases():
    assert repair._safe_name("event_risk_institutional") == "event_risk_institutional"
    with pytest.raises(ValueError):
        repair._safe_name("event_risk; DROP DATABASE x")


def test_staging_repair_refuses_retail_source(monkeypatch):
    monkeypatch.setattr(repair, "STAGING_DB", "event_risk")
    monkeypatch.setattr(repair, "LEGACY_SOURCE_DB", "event_risk")
    with pytest.raises(RuntimeError, match="refusing to run staging repair"):
        repair.main()


def test_staging_validator_qualifies_only_safe_aliases():
    assert validate._qualified("event_risk", "ticks") == '"event_risk".main."ticks"'
    with pytest.raises(ValueError):
        validate._qualified("event_risk;bad", "ticks")
