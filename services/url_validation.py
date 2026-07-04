"""Outbound URL validation for Lightning Goats integrations."""

import socket
from ipaddress import ip_address, ip_network
from urllib.parse import urlparse

from loguru import logger


ALLOWED_PRIVATE_NETWORKS = (ip_network("10.8.0.0/24"),)


class OutboundURLPolicyError(ValueError):
    """Raised when a configured outbound URL violates the extension policy."""


def _is_allowed_ip(ip) -> bool:
    """Return True if a parsed ip_address object is permitted by policy."""
    if any(ip in network for network in ALLOWED_PRIVATE_NETWORKS):
        return True
    return not (
        ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_private
        or ip.is_reserved
        or ip.is_unspecified
    )


def _resolve_host_ips(host: str) -> list:
    """Resolve a hostname to the set of ip_address objects it maps to.

    Returns an empty list when resolution fails (e.g. offline/CI). Callers
    treat an empty result as "cannot prove the host is unsafe" and allow it,
    since a name that does not resolve cannot be reached anyway.
    """
    try:
        infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        logger.debug(f"Lightning Goats: could not resolve outbound host '{host}': {exc}")
        return []
    resolved = []
    for info in infos:
        sockaddr = info[4]
        if not sockaddr:
            continue
        try:
            resolved.append(ip_address(sockaddr[0]))
        except ValueError:
            continue
    return resolved


def validate_outbound_url(url: str) -> str:
    """Validate a user-configured outbound URL and return its stripped value.

    Public HTTP(S) URLs are allowed. The WireGuard subnet 10.8.0.0/24 is the
    only allowed private range. Hostnames are resolved and every resulting IP
    is checked, so a public name that resolves to a private/loopback/metadata
    address (SSRF via DNS) is rejected rather than trusted blindly.
    """

    value = (url or "").strip()
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise OutboundURLPolicyError("URL must be a valid http:// or https:// URL")

    hostname = parsed.hostname.strip().lower()
    if hostname == "localhost" or hostname.endswith(".localhost"):
        raise OutboundURLPolicyError("localhost URLs are not allowed")

    # Collect every IP we must vet: the literal (if the host is an IP) or all
    # addresses the name resolves to.
    candidates = []
    try:
        candidates.append(ip_address(hostname))
    except ValueError:
        candidates.extend(_resolve_host_ips(hostname))

    # If the host neither is an IP literal nor resolves, it is unreachable; we
    # keep the historically permissive behaviour and let the request attempt
    # fail naturally rather than block a possibly-valid public name.
    for ip in candidates:
        if not _is_allowed_ip(ip):
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
