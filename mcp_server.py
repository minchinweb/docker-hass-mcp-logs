import asyncio
import hmac
import json
import os

import httpx
from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.server.auth.settings import AuthSettings
from mcp.server.mcpserver import MCPServer
from pydantic import AnyHttpUrl
import websockets

HA = os.environ["HA_URL"].rstrip("/")
HA_WS = HA.replace("http", "ws", 1) + "/api/websocket"  # http -> ws, https -> wss
TOKEN = os.environ["HA_TOKEN"]
MCP_PATH = "/" + os.environ["MCP_PATH"].lstrip("/")
API_KEY = os.environ["MCP_AUTH_TOKEN"]
MCP_PUBLIC_URL = os.environ["MCP_PUBLIC_URL"].rstrip("/")


class StaticTokenVerifier(TokenVerifier):
    async def verify_token(self, token: str) -> AccessToken | None:
        if not hmac.compare_digest(token, API_KEY):
            return None
        return AccessToken(token=token, client_id="desktop", scopes=["user"])


mcp = MCPServer(
    "homeassistant-support",
    version="1.1.1",
    token_verifier=StaticTokenVerifier(),
    auth=AuthSettings(
        # issuer_url=AnyHttpUrl("http://127.0.0.1:8000"),
        # resource_server_url=AnyHttpUrl("http://127.0.0.1:8000"),
        issuer_url=AnyHttpUrl(MCP_PUBLIC_URL),
        resource_server_url=AnyHttpUrl(MCP_PUBLIC_URL),
        required_scopes=["user"],
    ),
)


async def ha_ws_call(payload: dict, timeout: float = 20) -> dict | list:
    """Open a short-lived HA WebSocket connection, authenticate, send one
    command, and return its `result`. Raises RuntimeError on any HA error."""
    async with asyncio.timeout(timeout):
        async with websockets.connect(HA_WS, max_size=16 * 1024 * 1024) as ws:
            hello = json.loads(await ws.recv())
            if hello.get("type") != "auth_required":
                raise RuntimeError(f"Unexpected handshake: {hello}")

            await ws.send(json.dumps({"type": "auth", "access_token": TOKEN}))
            auth = json.loads(await ws.recv())
            if auth.get("type") != "auth_ok":
                raise RuntimeError(f"HA auth failed: {auth}")

            await ws.send(json.dumps({"id": 1, **payload}))
            resp = json.loads(await ws.recv())
            if not resp.get("success"):
                raise RuntimeError(f"HA error: {resp.get('error')}")
            return resp["result"]


@mcp.tool()
async def get_error_log(
    min_level: str = "WARNING",
    tail_lines: int = 500,
    contains: str = "",
) -> str:
    """Home Assistant log, filtered by level, substring, last N lines."""

    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(
            f"{HA}/api/error_log", headers={"Authorization": f"Bearer {TOKEN}"}
        )
        r.raise_for_status()

    levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    ok = levels[levels.index(min_level.upper()) :]
    lines = [
        l
        for l in r.text.splitlines()
        if any(f" {x} " in l for x in ok) and contains.lower() in l.lower()
    ]
    return "\n".join(lines[-tail_lines:])


@mcp.tool()
async def list_dashboards() -> str:
    """List Lovelace dashboards (url_path, title, mode). The default dashboard
    is not included; use url_path=None with get_dashboard_config for it."""
    return json.dumps(await ha_ws_call({"type": "lovelace/dashboards/list"}), indent=2)


@mcp.tool()
async def get_dashboard_config(url_path: str | None = None) -> str:
    """Full Lovelace config for a dashboard. Omit url_path for the default one."""
    return json.dumps(
        await ha_ws_call({"type": "lovelace/config", "url_path": url_path}),
        indent=2,
    )


@mcp.tool()
async def list_lovelace_resources() -> str:
    """Custom card/module resources (e.g. HACS cards) registered with Lovelace."""
    return json.dumps(await ha_ws_call({"type": "lovelace/resources"}), indent=2)


if __name__ == "__main__":
    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=8000,
        streamable_http_path=MCP_PATH,
    )
