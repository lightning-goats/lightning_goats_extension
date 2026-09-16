import socket

import pytest

from lnbits.extensions.lightning_goats.services import url_validation
from lnbits.extensions.lightning_goats.services.url_validation import (
    OutboundURLPolicyError,
    ensure_outbound_url_allowed,
    validate_outbound_url,
)


def test_openhab_allows_ipv4_loopback():
    assert (
        ensure_outbound_url_allowed("http://127.0.0.1:8080", "OpenHAB URL")
        == "http://127.0.0.1:8080"
    )


def test_openhab_allows_localhost_and_ipv6_loopback():
    assert (
        ensure_outbound_url_allowed("http://localhost:8080", "OpenHAB URL")
        == "http://localhost:8080"
    )
    assert (
        ensure_outbound_url_allowed("http://[::1]:8080", "OpenHAB URL")
        == "http://[::1]:8080"
    )


def test_low_level_generic_policy_still_rejects_loopback_by_default():
    with pytest.raises(OutboundURLPolicyError):
        validate_outbound_url("http://127.0.0.1:8080")


def test_weather_integration_allows_exact_ipv4_localhost():
    assert (
        ensure_outbound_url_allowed(
            "http://127.0.0.1:5000/get_received_data",
            "weather station URL",
        )
        == "http://127.0.0.1:5000/get_received_data"
    )


def test_non_openhab_integrations_do_not_get_broad_loopback_access():
    with pytest.raises(OutboundURLPolicyError):
        ensure_outbound_url_allowed(
            "http://127.0.0.2:5000/get_received_data",
            "weather station URL",
        )

    with pytest.raises(OutboundURLPolicyError):
        ensure_outbound_url_allowed(
            "http://localhost:5000/get_received_data",
            "weather station URL",
        )


def test_openhab_does_not_allow_dns_name_that_resolves_to_loopback(monkeypatch):
    def fake_getaddrinfo(host, *args, **kwargs):
        return [(socket.AF_INET, None, None, "", ("127.0.0.1", 0))]

    monkeypatch.setattr(url_validation.socket, "getaddrinfo", fake_getaddrinfo)

    with pytest.raises(OutboundURLPolicyError):
        ensure_outbound_url_allowed("http://sneaky.example.com:8080", "OpenHAB URL")


def test_weather_does_not_allow_dns_name_that_resolves_to_loopback(monkeypatch):
    def fake_getaddrinfo(host, *args, **kwargs):
        return [(socket.AF_INET, None, None, "", ("127.0.0.1", 0))]

    monkeypatch.setattr(url_validation.socket, "getaddrinfo", fake_getaddrinfo)

    with pytest.raises(OutboundURLPolicyError):
        ensure_outbound_url_allowed(
            "http://sneaky.example.com:5000/get_received_data",
            "weather station URL",
        )
