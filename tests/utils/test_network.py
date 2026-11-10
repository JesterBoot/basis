import ipaddress

import pytest

from utils.network import is_private_address, is_private_host


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.1.2.3",
        "172.16.0.1",
        "192.168.0.1",
        "169.254.169.254",
        "100.64.0.1",
        "0.0.0.0",
        "255.255.255.255",
        "224.0.0.1",
        "::1",
        "::",
        "fe80::1",
        "fc00::1",
        "ff02::1",
        "::ffff:127.0.0.1",
        "::ffff:10.0.0.1",
        "2002:7f00:1::",
        "64:ff9b::7f00:1",
        "64:ff9b::a00:1",
    ],
)
def test_non_public_addresses_are_private(address: str) -> None:
    assert is_private_address(ipaddress.ip_address(address)) is True


@pytest.mark.parametrize(
    "address",
    [
        "8.8.8.8",
        "1.1.1.1",
        "2606:4700:4700::1111",
        "::ffff:8.8.8.8",
        "64:ff9b::808:808",
    ],
)
def test_public_addresses_are_not_private(address: str) -> None:
    assert is_private_address(ipaddress.ip_address(address)) is False


@pytest.mark.parametrize(
    "host",
    ["localhost", "LOCALHOST", "localhost.", "api.localhost", "[::1]", "127.0.0.1"],
)
def test_private_hosts(host: str) -> None:
    assert is_private_host(host) is True


@pytest.mark.parametrize(
    "host", ["merchant.example", "webhook-sink", "localhost.example.com", "8.8.8.8"]
)
def test_public_or_unresolved_hosts(host: str) -> None:
    assert is_private_host(host) is False
