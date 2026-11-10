import asyncio
import ipaddress
import socket

NAT64_PREFIX = ipaddress.IPv6Network("64:ff9b::/96")

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address


def is_private_address(address: IPAddress) -> bool:
    if isinstance(address, ipaddress.IPv6Address):
        if address.ipv4_mapped is not None:
            return is_private_address(address.ipv4_mapped)
        if address.sixtofour is not None:
            return is_private_address(address.sixtofour)
        if address in NAT64_PREFIX:
            return is_private_address(ipaddress.IPv4Address(int(address) & 0xFFFFFFFF))
    return address.is_multicast or not address.is_global


def is_private_host(host: str) -> bool:
    normalized = host.strip("[]").rstrip(".").lower()
    if normalized == "localhost" or normalized.endswith(".localhost"):
        return True
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return False
    return is_private_address(address)


async def resolve_host(host: str, port: int) -> list[IPAddress]:
    infos = await asyncio.get_running_loop().getaddrinfo(
        host.strip("[]"), port, type=socket.SOCK_STREAM
    )
    return list({ipaddress.ip_address(info[4][0]) for info in infos})
