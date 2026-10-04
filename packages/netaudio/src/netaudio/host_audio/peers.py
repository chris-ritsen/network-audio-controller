from __future__ import annotations

import asyncio
import hashlib
import http.client
import json
import logging
import socket
import ssl
from dataclasses import dataclass
from typing import Any

from zeroconf import ServiceStateChange
from zeroconf.asyncio import AsyncServiceBrowser, AsyncServiceInfo

logger = logging.getLogger("netaudio")

DAEMON_SERVICE_TYPE = "_netaudio-relay._tcp.local."
RESOLVE_TIMEOUT_MILLISECONDS = 3000
REQUEST_TIMEOUT_SECONDS = 4.0


class PeerError(Exception):
    pass


@dataclass
class Peer:
    host: str
    address: str
    port: int
    scheme: str
    fingerprint: str | None
    version: str | None


def _text(properties: dict, key: str) -> str | None:
    value = properties.get(key.encode())
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else None


def _fingerprint(der: bytes) -> str:
    digest = hashlib.sha256(der).hexdigest().upper()
    return ":".join(digest[index : index + 2] for index in range(0, len(digest), 2))


def _request(peer: Peer, method: str, path: str, body: Any, timeout: float) -> tuple[int, Any]:
    payload = json.dumps(body).encode() if body is not None else None
    headers = {"accept": "application/json"}
    if payload is not None:
        headers["content-type"] = "application/json"
    if peer.scheme == "https":
        if not peer.fingerprint:
            raise PeerError(
                f"the netaudio server on {peer.host} ({peer.version or 'unknown version'}) is too old to share its audio"
            )
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        connection: http.client.HTTPConnection = http.client.HTTPSConnection(
            peer.address, peer.port, timeout=timeout, context=context
        )
        connection.connect()
        sock = connection.sock
        der = sock.getpeercert(binary_form=True) if isinstance(sock, ssl.SSLSocket) else None
        if der is None or _fingerprint(der) != peer.fingerprint:
            connection.close()
            raise PeerError(f"{peer.host} presented a TLS certificate that does not match its advertised fingerprint")
    else:
        connection = http.client.HTTPConnection(peer.address, peer.port, timeout=timeout)
    try:
        connection.request(method, path, body=payload, headers=headers)
        response = connection.getresponse()
        data = response.read()
    finally:
        connection.close()
    try:
        return response.status, json.loads(data) if data else None
    except json.JSONDecodeError as exception:
        raise PeerError(f"{peer.host} returned a reply that is not JSON") from exception


class HostAudioPeers:
    def __init__(self) -> None:
        self.peers: dict[str, Peer] = {}
        self._service_hosts: dict[str, str] = {}
        self._browser: AsyncServiceBrowser | None = None
        self._zeroconf: Any = None
        self._tasks: set[asyncio.Task] = set()
        self._local_host = socket.gethostname().removesuffix(".local").casefold()

    def start(self, zeroconf: Any) -> None:
        if self._browser is not None or zeroconf is None:
            return
        self._zeroconf = zeroconf
        self._browser = AsyncServiceBrowser(zeroconf.zeroconf, DAEMON_SERVICE_TYPE, handlers=[self._on_change])

    async def stop(self) -> None:
        browser = self._browser
        self._browser = None
        if browser is not None:
            await browser.async_cancel()
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.peers.clear()

    def _on_change(self, zeroconf, service_type, name, state_change) -> None:
        if state_change == ServiceStateChange.Removed:
            host = self._service_hosts.pop(name, None)
            if host is not None and self.peers.pop(host, None) is not None:
                logger.info("Host audio peer %s left", host)
            return
        task = asyncio.create_task(self._resolve(name), name=f"host-audio-peer:{name}")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _resolve(self, name: str) -> None:
        info = AsyncServiceInfo(DAEMON_SERVICE_TYPE, name)
        if not await info.async_request(self._zeroconf.zeroconf, RESOLVE_TIMEOUT_MILLISECONDS):
            return
        server = (info.server or "").removesuffix(".").removesuffix(".local")
        if not server or server.casefold() == self._local_host:
            return
        addresses = info.parsed_scoped_addresses()
        address = next((value for value in addresses if ":" not in value), None)
        if address is None or info.port is None:
            return
        properties = info.properties or {}
        peer = Peer(
            host=server,
            address=address,
            port=info.port,
            scheme=_text(properties, "scheme") or "http",
            fingerprint=_text(properties, "tls_sha256"),
            version=_text(properties, "server_version"),
        )
        if self.peers.get(server) != peer:
            logger.info("Host audio peer %s at %s:%s (netaudio %s)", server, address, info.port, peer.version)
        self.peers[server] = peer
        self._service_hosts[name] = server

    async def request(
        self, host: str, method: str, path: str, body: Any = None, timeout: float = REQUEST_TIMEOUT_SECONDS
    ) -> tuple[int, Any]:
        peer = self.find(host)
        if peer is None:
            known = ", ".join(sorted(self.peers)) or "none"
            raise PeerError(f"no netaudio server on {host!r} has been found on the network; servers found: {known}")
        loop = asyncio.get_running_loop()
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(None, _request, peer, method, path, body, timeout), timeout + 1
            )
        except (OSError, http.client.HTTPException, asyncio.TimeoutError) as exception:
            raise PeerError(f"cannot reach the netaudio server on {peer.host}: {exception}") from exception

    def find(self, host: str) -> Peer | None:
        wanted = host.casefold().removesuffix(".local")
        return next((peer for name, peer in self.peers.items() if name.casefold() == wanted), None)
