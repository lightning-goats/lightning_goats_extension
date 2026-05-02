import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from lnbits.extensions.lightning_goats.services.url_validation import (
    OutboundURLPolicyError,
    validate_outbound_url,
)
from lnbits.extensions.lightning_goats.services.openhab import OpenHABService
from lnbits.extensions.lightning_goats import tasks, views_api


def run(coro):
    return asyncio.run(coro)


def test_outbound_url_policy_allows_public_and_wireguard_urls():
    assert validate_outbound_url("https://example.com/api") == "https://example.com/api"
    assert validate_outbound_url("http://10.8.0.42:8080/rest") == "http://10.8.0.42:8080/rest"


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com",
        "http://127.0.0.1:8080",
        "http://localhost:8080",
        "http://169.254.169.254/latest/meta-data",
        "http://10.0.0.12:8080",
        "http://172.16.0.12:8080",
        "http://192.168.1.12:8080",
    ],
)
def test_outbound_url_policy_rejects_unsafe_urls(url):
    with pytest.raises(OutboundURLPolicyError):
        validate_outbound_url(url)


def test_operational_configuration_requires_openhab_url():
    assert not views_api.is_operationally_configured(SimpleNamespace(openhab_url=""))
    assert views_api.is_operationally_configured(SimpleNamespace(openhab_url="http://10.8.0.2:8080"))


def test_herd_wallet_must_belong_to_authenticated_user(monkeypatch):
    wallet = SimpleNamespace(id="wallet-b", user="other-user")

    async def fake_get_wallet(wallet_id):
        assert wallet_id == "wallet-b"
        return wallet

    monkeypatch.setattr(views_api, "get_wallet", fake_get_wallet)

    with pytest.raises(HTTPException) as exc:
        run(views_api.ensure_user_wallet("user-a", "wallet-b"))

    assert exc.value.status_code == 400


def test_openhab_override_state_unknown_fails_closed(monkeypatch):
    service = OpenHABService("http://10.8.0.2:8080", "token")

    async def fake_get_item_state(item_name):
        return None

    monkeypatch.setattr(service, "get_item_state", fake_get_item_state)

    assert run(service.get_feeder_override_state()) is None
    assert run(service.is_feeder_override_enabled()) is True


def test_failed_feeder_trigger_marks_payment_failed_not_processed(monkeypatch):
    processed = []
    failed = []
    claimed = []

    async def fake_get_wallet(wallet_id):
        return SimpleNamespace(id=wallet_id, user="user-a", balance_msat=2_000_000)

    async def fake_get_settings(user_id):
        return SimpleNamespace(
            openhab_url="http://10.8.0.2:8080",
            openhab_auth="token",
            openhab_feeder_rule_id="rule-1",
            herd_wallet_id="wallet-a",
            feeder_trigger_sats=1_000,
            minimum_sats=10,
            interface_messages_enabled=True,
        )

    async def fake_try_claim_payment(**kwargs):
        claimed.append(kwargs["payment_hash"])
        return True

    async def fake_mark_processed(payment_hash):
        processed.append(payment_hash)

    async def fake_mark_failed(payment_hash, error):
        failed.append((payment_hash, error))

    class FakeOpenHABService:
        def __init__(self, base_url, auth_token):
            pass

        async def is_feeder_override_enabled(self):
            return False

        async def trigger_feeder(self, rule_id):
            return False

        async def close(self):
            pass

    monkeypatch.setattr(tasks, "get_wallet", fake_get_wallet)
    monkeypatch.setattr(tasks, "get_settings", fake_get_settings)
    monkeypatch.setattr(tasks, "try_claim_payment", fake_try_claim_payment)
    monkeypatch.setattr(tasks, "mark_payment_processed", fake_mark_processed)
    monkeypatch.setattr(tasks, "mark_payment_failed", fake_mark_failed)
    monkeypatch.setattr(tasks, "OpenHABService", FakeOpenHABService)
    monkeypatch.setattr(tasks, "_LG_STARTED_AT", None)
    monkeypatch.setattr(tasks, "_LG_TODAY_START", None)

    payment = SimpleNamespace(
        wallet_id="wallet-a",
        amount=1_000_000,
        checking_id="checking-1",
        payment_hash=None,
        preimage=None,
        extra={},
    )

    run(tasks._handle_payment(payment))

    assert claimed == ["checking-1"]
    assert not processed
    assert failed
    assert failed[0][0] == "checking-1"
    assert "feeder trigger failed" in failed[0][1].lower()


def test_unconfigured_payment_is_not_claimed(monkeypatch):
    claimed = []

    async def fake_get_wallet(wallet_id):
        return SimpleNamespace(id=wallet_id, user="user-a", balance_msat=2_000_000)

    async def fake_get_settings(user_id):
        return SimpleNamespace(openhab_url="", herd_wallet_id="wallet-a")

    async def fake_try_claim_payment(**kwargs):
        claimed.append(kwargs["payment_hash"])
        return True

    monkeypatch.setattr(tasks, "get_wallet", fake_get_wallet)
    monkeypatch.setattr(tasks, "get_settings", fake_get_settings)
    monkeypatch.setattr(tasks, "try_claim_payment", fake_try_claim_payment)

    payment = SimpleNamespace(
        wallet_id="wallet-a",
        amount=1_000_000,
        checking_id="checking-2",
        payment_hash=None,
        preimage=None,
        extra={},
    )

    run(tasks._handle_payment(payment))

    assert not claimed
