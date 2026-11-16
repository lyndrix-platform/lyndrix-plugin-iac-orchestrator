"""A deliberately empty value is a value (1.8.0 staging fix).

``service_reconcile_exclude_services`` saved as "" means "exclude nothing" (IaCConfig
reads it so). The settings API used to report it as the default ``aac-hermes-agent``
(a) and to skip saving ``aac-hermes-agent`` back as "unchanged" (b) — the protection
stayed off while the dialog claimed it was on. Staging 2026-11-16.
"""
import json
from types import SimpleNamespace

import pytest

from iac_plugin.app.controller.config import IaCConfig
from iac_plugin.app.controller.settings_schema import SETTINGS_FIELDS, OrchestratorSettings

KEY = "service_reconcile_exclude_services"
VAULT_KEY = "iac_service_reconcile_exclude_services"
ENV = "PLUGIN_IAC_ORCHESTRATOR_SERVICE_RECONCILE_EXCLUDE_SERVICES"
HERMES = "aac-hermes-agent"


class FakeCtx:
    def __init__(self, **vault):
        self.vault = {"iac_token_registry": json.dumps([]), **vault}

    def get_secret(self, key):
        return self.vault.get(key)

    def set_secret(self, key, value):
        self.vault[key] = value
        return True


@pytest.fixture(autouse=True)
def _no_env_override(monkeypatch):
    monkeypatch.delenv(ENV, raising=False)


def _mgr(**vault):
    ctx = FakeCtx(**vault)
    return ctx, OrchestratorSettings(ctx, SimpleNamespace(state={})), IaCConfig(ctx)


def test_never_set_shows_the_default_and_the_default_applies():
    _ctx, mgr, config = _mgr()
    assert mgr.get_values()[KEY] == HERMES
    assert config.service_reconcile_exclude_services == {HERMES}
    # Saving the default over "never set" changes nothing effective → skipped.
    assert mgr.save_values({KEY: HERMES}) == {"saved": [], "locked": [], "unchanged": [KEY]}


def test_a_saved_empty_value_is_shown_empty_like_it_applies():  # (a)
    ctx, mgr, config = _mgr()
    assert mgr.save_values({KEY: ""})["saved"] == [KEY]
    assert ctx.vault[VAULT_KEY] == ""
    assert config.service_reconcile_exclude_services == set()
    assert mgr.get_values()[KEY] == ""                       # was: "aac-hermes-agent"
    # Saving "" again is unchanged now.
    assert mgr.save_values({KEY: "  "})["unchanged"] == [KEY]


def test_the_default_can_be_written_back_over_an_empty_value():  # (b)
    ctx, mgr, config = _mgr(**{VAULT_KEY: ""})
    result = mgr.save_values({KEY: HERMES})
    assert result["saved"] == [KEY] and result["unchanged"] == []   # was: saved []
    assert ctx.vault[VAULT_KEY] == HERMES
    assert config.service_reconcile_exclude_services == {HERMES}
    assert mgr.get_values()[KEY] == HERMES


def test_env_override_still_locks_the_field(monkeypatch):
    ctx, mgr, config = _mgr(**{VAULT_KEY: HERMES})
    monkeypatch.setenv(ENV, "")
    assert config.service_reconcile_exclude_services == set()         # env wins
    result = mgr.save_values({KEY: ""})
    assert result["locked"] == [KEY] and result["saved"] == []
    assert mgr.get_values()[f"{KEY}__locked"] is True
    assert ctx.vault[VAULT_KEY] == HERMES


def test_other_str_fields_keep_blank_means_default():
    """Only the flagged field changes: a blank Lyndrix base URL is still shown
    (and compared) as its default."""
    assert [f.key for f in SETTINGS_FIELDS if f.empty_is_value] == [KEY]
    _ctx, mgr, _config = _mgr(iac_lyndrix_base_url="")
    assert mgr.get_values()["lyndrix_base_url"] == "http://10.1.10.31:8081"
    assert mgr.save_values({"lyndrix_base_url": "http://10.1.10.31:8081"})["unchanged"] == ["lyndrix_base_url"]
