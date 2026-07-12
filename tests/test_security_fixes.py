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


@pytest.fixture(autouse=True)
def _reset_feeder_cooldown():
    tasks._last_feed_monotonic.clear()
    yield
    tasks._last_feed_monotonic.clear()


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


def test_outbound_url_policy_rejects_hostname_resolving_to_private(monkeypatch):
    import socket as _socket

    from lnbits.extensions.lightning_goats.services import url_validation

    def fake_getaddrinfo(host, *args, **kwargs):
        # A public-looking name that an attacker points at loopback (SSRF via DNS).
        return [(_socket.AF_INET, None, None, "", ("127.0.0.1", 0))]

    monkeypatch.setattr(url_validation.socket, "getaddrinfo", fake_getaddrinfo)

    with pytest.raises(OutboundURLPolicyError):
        url_validation.validate_outbound_url("http://sneaky.example.com/rest")


def test_outbound_url_policy_allows_hostname_resolving_to_public(monkeypatch):
    import socket as _socket

    from lnbits.extensions.lightning_goats.services import url_validation

    def fake_getaddrinfo(host, *args, **kwargs):
        return [(_socket.AF_INET, None, None, "", ("93.184.216.34", 0))]

    monkeypatch.setattr(url_validation.socket, "getaddrinfo", fake_getaddrinfo)

    assert (
        url_validation.validate_outbound_url("http://public.example.com/rest")
        == "http://public.example.com/rest"
    )


def test_stop_unregisters_invoice_listener_and_cancels_tasks():
    from lnbits.tasks import invoice_listeners

    invoice_listeners[tasks.INVOICE_LISTENER_NAME] = object()

    class FakeTask:
        def __init__(self):
            self.cancelled = False

        def done(self):
            return False

        def cancel(self):
            self.cancelled = True

        def __await__(self):
            async def _raise():
                raise asyncio.CancelledError

            return _raise().__await__()

    fake = FakeTask()
    tasks.scheduled_tasks.clear()
    tasks.scheduled_tasks.append(fake)

    run(tasks.stop_background_tasks())

    assert tasks.INVOICE_LISTENER_NAME not in invoice_listeners
    assert fake.cancelled
    assert tasks.scheduled_tasks == []


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

    async def fake_get_settings(user_id, auto_populate=True):
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

    async def fake_get_settings(user_id, auto_populate=True):
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


# ---------------------------------------------------------------------------
# Distribution overlap with cyberherd: check whether cyberherd is distributing
# (send_splits_enabled) and act accordingly.
# ---------------------------------------------------------------------------


def _lg_settings(**overrides):
    base = dict(
        openhab_url="http://10.8.0.2:8080",
        openhab_auth="token",
        openhab_feeder_rule_id="rule-1",
        herd_wallet_id="wallet-a",
        feeder_trigger_sats=1_000,
        minimum_sats=10,
        interface_messages_enabled=True,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class _FakeOpenHABTriggers:
    def __init__(self, *args, **kwargs):
        pass

    async def is_feeder_override_enabled(self):
        return False

    async def trigger_feeder(self, rule_id):
        return True

    async def close(self):
        pass


def _patch_feeder_path(monkeypatch, state):
    async def fake_get_wallet(wallet_id):
        return SimpleNamespace(id=wallet_id, user="user-a", balance_msat=2_000_000)

    async def fake_get_settings(user_id, auto_populate=True):
        return _lg_settings()

    async def fake_claim(**kwargs):
        return True

    async def fake_processed(payment_hash):
        state["processed"].append(payment_hash)

    async def fake_failed(payment_hash, error):
        state["failed"].append((payment_hash, error))

    async def fake_send_feeder_message(**kwargs):
        return True

    monkeypatch.setattr(tasks, "get_wallet", fake_get_wallet)
    monkeypatch.setattr(tasks, "get_settings", fake_get_settings)
    monkeypatch.setattr(tasks, "try_claim_payment", fake_claim)
    monkeypatch.setattr(tasks, "mark_payment_processed", fake_processed)
    monkeypatch.setattr(tasks, "mark_payment_failed", fake_failed)
    monkeypatch.setattr(tasks, "send_feeder_message", fake_send_feeder_message)
    monkeypatch.setattr(tasks, "OpenHABService", _FakeOpenHABTriggers)
    monkeypatch.setattr(tasks, "_LG_STARTED_AT", None)
    monkeypatch.setattr(tasks, "_LG_TODAY_START", None)


def _payment(checking_id):
    return SimpleNamespace(
        wallet_id="wallet-a", amount=2_000_000, checking_id=checking_id,
        payment_hash=None, preimage=None, extra={},
    )


def test_delegates_distribution_when_cyberherd_autosplits_enabled(monkeypatch):
    state = {"processed": [], "failed": []}
    transfer_calls = []
    _patch_feeder_path(monkeypatch, state)

    async def ch_get_settings(user_id):
        return SimpleNamespace(send_splits_enabled=True, herd_wallet="wallet-a")

    async def ch_transfer(settings):
        transfer_calls.append(settings)
        return {"ok": True, "sats": 0}

    monkeypatch.setattr("lnbits.extensions.cyberherd.crud.get_settings", ch_get_settings)
    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.services.send_splits.transfer_herd_balance_to_source",
        ch_transfer,
    )

    run(tasks._handle_payment(_payment("c1")))

    # Delegated to cyberherd: LG must NOT distribute, and the feed is a success.
    assert transfer_calls == []
    assert state["processed"] == ["c1"]
    assert not state["failed"]


def test_zero_balance_distribution_is_not_a_failure(monkeypatch):
    state = {"processed": [], "failed": []}
    _patch_feeder_path(monkeypatch, state)

    async def ch_get_settings(user_id):
        return SimpleNamespace(send_splits_enabled=False, herd_wallet="wallet-a")

    async def ch_transfer(settings):
        return {"ok": False, "error": "Herd wallet balance is zero"}

    monkeypatch.setattr("lnbits.extensions.cyberherd.crud.get_settings", ch_get_settings)
    monkeypatch.setattr(
        "lnbits.extensions.cyberherd.services.send_splits.transfer_herd_balance_to_source",
        ch_transfer,
    )

    run(tasks._handle_payment(_payment("c2")))

    # An already-empty herd wallet is "already distributed", not a failure.
    assert state["processed"] == ["c2"]
    assert not state["failed"]


def test_no_herd_wallet_configured_skips_payment(monkeypatch):
    """Fail closed: with no herd wallet configured, do not process (feeder must
    not fire on an unrelated wallet)."""
    triggered = []

    async def fake_get_wallet(wallet_id):
        return SimpleNamespace(id=wallet_id, user="user-a", balance_msat=2_000_000)

    async def fake_get_settings(user_id, auto_populate=True):
        return _lg_settings(herd_wallet_id=None)

    async def fake_claim(**kwargs):
        triggered.append("claimed")
        return True

    monkeypatch.setattr(tasks, "get_wallet", fake_get_wallet)
    monkeypatch.setattr(tasks, "get_settings", fake_get_settings)
    monkeypatch.setattr(tasks, "try_claim_payment", fake_claim)

    run(tasks._handle_payment(_payment("c3")))

    # Returned before claiming/processing anything.
    assert triggered == []


def test_feeder_does_not_refire_within_cooldown(monkeypatch):
    """Feeder cooldown: a second payment arriving right after a feed (within the
    cooldown window) must not feed the goats again, even though its balance is
    still above the trigger."""
    state = {"processed": [], "failed": []}
    feeds = []
    _patch_feeder_path(monkeypatch, state)

    class CountingOpenHAB(_FakeOpenHABTriggers):
        async def trigger_feeder(self, rule_id):
            feeds.append(rule_id)
            return True

    monkeypatch.setattr(tasks, "OpenHABService", CountingOpenHAB)

    async def ch_get_settings(user_id):
        # auto-splits on -> LG delegates; the fake balance never drops.
        return SimpleNamespace(send_splits_enabled=True, herd_wallet="wallet-a")

    monkeypatch.setattr("lnbits.extensions.cyberherd.crud.get_settings", ch_get_settings)

    run(tasks._handle_payment(_payment("p1")))
    run(tasks._handle_payment(_payment("p2")))

    assert feeds == ["rule-1"]                    # fed exactly once
    assert state["processed"] == ["p1", "p2"]     # both handled successfully
    assert not state["failed"]


def test_extra_string_is_not_used_as_preimage(monkeypatch):
    """A bare hex-like string in payment.extra must not be treated as a preimage
    (it would fail proof verification and wrongly reject the payment)."""
    payment = SimpleNamespace(
        wallet_id="w", amount=1000, checking_id="chk", payment_hash=None,
        preimage=None, extra="deadbeef" * 8,  # 64 hex chars, but NOT a preimage
    )
    ph, chk, preimage = tasks._extract_payment_hash_checking_id_preimage(payment)
    assert preimage is None
    assert ph is None
    assert chk == "chk"


def test_parse_ts_handles_datetime_objects():
    """LNbits Payment.time / created_at are datetime objects. The parser must
    accept them; otherwise a datetime falls through to None and every live
    payment is dropped as an 'unknown timestamp' during the startup-backlog
    window (the feeder never auto-triggers for ~180s after each restart)."""
    import datetime as _dt

    aware = _dt.datetime(2026, 7, 12, 13, 0, tzinfo=_dt.timezone.utc)
    parsed = tasks._parse_ts_to_dt(aware)
    assert parsed is not None
    assert parsed.timestamp() == aware.timestamp()

    # naive datetimes are assumed UTC
    naive = _dt.datetime(2026, 7, 12, 13, 0)
    assert tasks._parse_ts_to_dt(naive) is not None

    # existing formats still parse
    assert tasks._parse_ts_to_dt(1783861245) is not None
    assert tasks._parse_ts_to_dt("2026-07-12T13:00:00Z") is not None
    assert tasks._parse_ts_to_dt(None) is None


def test_extract_payment_dt_reads_payment_time():
    """A live payment's datetime is extracted, so the startup guard no longer
    treats it as unknown-timestamp."""
    import datetime as _dt

    now = _dt.datetime.now(_dt.timezone.utc)
    payment = SimpleNamespace(time=now, created_at=now, extra={})
    dt = tasks._extract_payment_dt(payment)
    assert dt is not None
    assert dt.timestamp() == pytest.approx(now.timestamp())
