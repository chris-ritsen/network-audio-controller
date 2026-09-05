from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger("netaudio")

WEBAPP_ROOT = Path(__file__).parent / "webapp"

WEB_CONTENT_TYPES = {
    ".css": "text/css; charset=utf-8",
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".svg": "image/svg+xml",
}

WEB_INDEX_ROUTE = "/"
WEB_INDEX_RELATIVE_PATH = "index.html"


def resolve_web_asset(route: str) -> Path | None:
    relative_route = WEB_INDEX_RELATIVE_PATH if route == WEB_INDEX_ROUTE else route.lstrip("/")
    if not relative_route:
        return None
    root = WEBAPP_ROOT.resolve()
    candidate = (root / relative_route).resolve()
    if candidate != root and root not in candidate.parents:
        return None
    if candidate.suffix not in WEB_CONTENT_TYPES:
        return None
    if not candidate.is_file():
        return None
    return candidate


def prefers_web_page(headers: dict | None) -> bool:
    if not headers:
        return False
    return "text/html" in headers.get("accept", "")


def is_application_route(route: str) -> bool:
    final_segment = route.rstrip("/").rsplit("/", 1)[-1]
    return "." not in final_segment


class DaemonWebHandlers:
    async def _handle_web_asset(self, writer, route):
        asset_path = resolve_web_asset(route)
        if asset_path is None and is_application_route(route):
            asset_path = resolve_web_asset(WEB_INDEX_ROUTE)
        if asset_path is None:
            await self._send_json(writer, {"error": "not found"}, 404)
            return
        try:
            body = asset_path.read_bytes()
        except OSError as exception:
            logger.warning(f"Web asset {route} could not be read: {exception}")
            await self._send_json(writer, {"error": "web asset unavailable"}, 500)
            return
        header = (
            "HTTP/1.1 200 OK\r\n"
            f"Content-Type: {WEB_CONTENT_TYPES[asset_path.suffix]}\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Cache-Control: no-store\r\n"
            "\r\n"
        ).encode()
        writer.write(header + body)
        await writer.drain()
