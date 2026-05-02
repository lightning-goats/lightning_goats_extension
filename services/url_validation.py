"""Outbound URL validation for Lightning Goats integrations."""

from ipaddress import ip_address, ip_network
from urllib.parse import urlparse


ALLOWED_PRIVATE_NETWORKS = (ip_network("10.8.0.0/24"),)


class OutboundURLPolicyError(ValueError):
    """Raised when a configured outbound URL violates the extension policy."""


def _is_allowed_ip(host: str) -> bool:
    parsed_ip = ip_address(host)
    if any(parsed_ip in network for network in ALLOWED_PRIVATE_NETWORKS):
        return True
    return not (
        parsed_ip.is_loopback
        or parsed_ip.is_link_local
        or parsed_ip.is_multicast
        or parsed_ip.is_private
        or parsed_ip.is_reserved
        or parsed_ip.is_unspecified
    )


def validate_outbound_url(url: str) -> str:
    """Validate a user-configured outbound URL and return its stripped value.

    Public HTTP(S) URLs are allowed. The WireGuard subnet 10.8.0.0/24 is the
    only allowed private range.
    """

    value = (url or "").strip()
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise OutboundURLPolicyError("URL must be a valid http:// or https:// URL")

    hostname = parsed.hostname.strip().lower()
    if hostname == "localhost" or hostname.endswith(".localhost"):
        raise OutboundURLPolicyError("localhost URLs are not allowed")

    try:
        allowed = _is_allowed_ip(hostname)
    except ValueError:
        allowed = True

    if not allowed:
        raise OutboundURLPolicyError(
            "URL host is not allowed. Public hosts and 10.8.0.0/24 are allowed."
        )

    return value


def ensure_outbound_url_allowed(url: str, field_name: str = "URL") -> str:
    """Validate a URL and raise ValueError with a field-specific message."""

    try:
        return validate_outbound_url(url)
    except OutboundURLPolicyError as exc:
        raise OutboundURLPolicyError(f"Invalid {field_name}: {exc}") from exc
