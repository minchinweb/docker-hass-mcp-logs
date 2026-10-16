import asyncio
import hmac
import json
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import httpx
from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.server.auth.settings import AuthSettings
from mcp.server.mcpserver import MCPServer
from pydantic import AnyHttpUrl
from starlette.requests import Request
from starlette.responses import JSONResponse
import websockets

HA = os.environ["HA_URL"].rstrip("/")
HA_WS = HA.replace("http", "ws", 1) + "/api/websocket"  # http -> ws, https -> wss
TOKEN = os.environ["HA_TOKEN"]
MCP_PATH = "/" + os.environ["MCP_PATH"].lstrip("/")
API_KEY = os.environ["MCP_AUTH_TOKEN"]
MCP_PUBLIC_URL = os.environ["MCP_PUBLIC_URL"].rstrip("/")

# Bump on every change to the tool set. Build metadata comes from the image
# (see Dockerfile) and falls back to "unknown" for non-Docker runs.
VERSION = "1.4.0"
BUILD_COMMIT = os.environ.get("BUILD_COMMIT", "unknown")
BUILD_DATE = os.environ.get("BUILD_DATE", "unknown")
STARTED_AT = datetime.now(timezone.utc).isoformat(timespec="seconds")


class StaticTokenVerifier(TokenVerifier):
    async def verify_token(self, token: str) -> AccessToken | None:
        if not hmac.compare_digest(token, API_KEY):
            return None
        return AccessToken(token=token, client_id="desktop", scopes=["user"])


mcp = MCPServer(
    "homeassistant-support",
    version=VERSION,
    token_verifier=StaticTokenVerifier(),
    auth=AuthSettings(
        # issuer_url=AnyHttpUrl("http://127.0.0.1:8000"),
        # resource_server_url=AnyHttpUrl("http://127.0.0.1:8000"),
        issuer_url=AnyHttpUrl(MCP_PUBLIC_URL),
        resource_server_url=AnyHttpUrl(MCP_PUBLIC_URL),
        required_scopes=["user"],
    ),
)


# Plain HTTP endpoint for quick checks (curl http://host:8000/version). It sits
# outside the MCP auth, so it only reveals version and build info, never the
# tool list (use get_server_info for that).
if hasattr(mcp, "custom_route"):

    @mcp.custom_route("/version", methods=["GET"])
    async def version_endpoint(request: Request) -> JSONResponse:
        return JSONResponse(
            {
                "name": "homeassistant-support",
                "version": VERSION,
                "build_commit": BUILD_COMMIT,
                "build_date": BUILD_DATE,
            }
        )

else:
    print("WARNING: MCPServer has no custom_route; /version endpoint disabled")


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


async def ha_rest_get(path: str, params: dict | None = None, timeout: float = 30):
    """GET a Home Assistant REST endpoint and return the decoded JSON."""
    async with httpx.AsyncClient(timeout=timeout) as c:
        r = await c.get(
            f"{HA}{path}",
            params=params,
            headers={"Authorization": f"Bearer {TOKEN}"},
        )
        r.raise_for_status()
        return r.json()


def _since(hours: float) -> str:
    """ISO timestamp `hours` ago (UTC), URL-safe for use in a path."""
    ts = datetime.now(timezone.utc) - timedelta(hours=hours)
    return quote(ts.isoformat(timespec="seconds"), safe="")


def _dump(obj, limit: int = 60000) -> str:
    """JSON-encode, truncating very large results so they stay readable."""
    text = json.dumps(obj, indent=2, default=str)
    if len(text) > limit:
        text = text[:limit] + f"\n... truncated ({len(text) - limit} more characters)"
    return text


@mcp.tool()
async def get_server_info() -> str:
    """Version and build info for this MCP server (version, build commit and
    date, start time) plus the names of every registered tool. Use it to check
    that a redeploy took effect and which tools this instance is serving."""
    tools = sorted(t.name for t in await mcp.list_tools())
    return _dump(
        {
            "name": "homeassistant-support",
            "version": VERSION,
            "build_commit": BUILD_COMMIT,
            "build_date": BUILD_DATE,
            "started_at": STARTED_AT,
            "tools": tools,
        }
    )


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


@mcp.tool()
async def list_automations(contains: str = "") -> str:
    """Automations with state (on/off), last_triggered and the config `id`
    needed by get_automation_config / list_traces / get_trace. `contains`
    filters on entity_id or friendly name (case-insensitive)."""
    needle = contains.lower()
    out = []
    for st in await ha_rest_get("/api/states"):
        eid = st["entity_id"]
        if not eid.startswith("automation."):
            continue
        attrs = st["attributes"]
        name = attrs.get("friendly_name") or ""
        if needle and needle not in f"{eid} {name}".lower():
            continue
        out.append(
            {
                "entity_id": eid,
                "name": name,
                "id": attrs.get("id"),
                "state": st["state"],
                "last_triggered": attrs.get("last_triggered"),
                "mode": attrs.get("mode"),
                "current": attrs.get("current"),
            }
        )
    return _dump(out)


@mcp.tool()
async def get_automation_config(automation_id: str) -> str:
    """Full config (triggers, conditions, actions) of a UI-defined automation.
    `automation_id` is the `id` from list_automations. YAML-defined
    automations without an id are not available here."""
    return _dump(await ha_rest_get(f"/api/config/automation/config/{automation_id}"))


@mcp.tool()
async def list_traces(automation_id: str) -> str:
    """Recent runs of an automation (HA keeps ~5 by default): run_id,
    timestamps, trigger, state and any error. `automation_id` is the `id`
    from list_automations. An empty list means it has not run recently."""
    return _dump(
        await ha_ws_call(
            {"type": "trace/list", "domain": "automation", "item_id": automation_id}
        )
    )


@mcp.tool()
async def get_trace(automation_id: str, run_id: str) -> str:
    """Step-by-step trace for one automation run (run_id from list_traces):
    which trigger fired, which conditions passed or failed, and each action's
    result or error."""
    return _dump(
        await ha_ws_call(
            {
                "type": "trace/get",
                "domain": "automation",
                "item_id": automation_id,
                "run_id": run_id,
            }
        )
    )


@mcp.tool()
async def get_logbook(entity_id: str = "", hours: float = 48) -> str:
    """Logbook entries for the last N hours, optionally for one entity. Shows
    automation triggers and state changes, so it can tell 'never triggered'
    from 'triggered but failed'."""
    params = {"entity": entity_id} if entity_id else None
    return _dump(await ha_rest_get(f"/api/logbook/{_since(hours)}", params))


@mcp.tool()
async def get_history(
    entity_id: str, hours: float = 24, include_attributes: bool = False
) -> str:
    """State history of one entity for the last N hours (state + timestamps).
    Set include_attributes=True for full states with attributes, e.g. the
    `event_type` of an event entity."""
    params = {"filter_entity_id": entity_id}
    if not include_attributes:
        params.update({"minimal_response": "1", "no_attributes": "1"})
    return _dump(await ha_rest_get(f"/api/history/period/{_since(hours)}", params))


@mcp.tool()
async def get_state(entity_id: str = "", contains: str = "") -> str:
    """Current state and attributes. Pass entity_id for one entity, or
    `contains` to list every entity whose id or friendly name matches
    (case-insensitive), e.g. to find all the Sleep as Android entities."""
    if entity_id:
        return _dump(await ha_rest_get(f"/api/states/{entity_id}"))
    needle = contains.lower()
    out = [
        st
        for st in await ha_rest_get("/api/states")
        if needle
        in f"{st['entity_id']} {st['attributes'].get('friendly_name') or ''}".lower()
    ]
    return _dump(out)


if __name__ == "__main__":
    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=8000,
        streamable_http_path=MCP_PATH,
    )
