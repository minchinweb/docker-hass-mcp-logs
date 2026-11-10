import asyncio
import functools
import hmac
import json
import os
import re
import socket
import threading
from collections import deque
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

# Optional: name of the Matter server's Docker container, so its log can be read
# through the Docker socket (see get_matter_server_log). Only this one container
# is ever read, and it comes from the environment, never from a tool argument.
MATTER_CONTAINER = os.environ.get("MATTER_CONTAINER", "").strip()
DOCKER_SOCKET = os.environ.get("DOCKER_SOCKET", "/var/run/docker.sock")

# Bump on every change to the tool set. Build metadata comes from the image
# (see Dockerfile) and falls back to "unknown" for non-Docker runs.
VERSION = "1.12.1"
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


async def ha_ws_batch(payloads: list[dict], timeout: float = 30) -> list:
    """Open one HA WebSocket connection, authenticate, send every command on it
    at once, and return one slot per command, in order: its `result`, or the
    exception for it (a RuntimeError for an HA error, TimeoutError for one that
    hadn't answered in time). Raises only if the connection or login fails."""
    results: list = [None] * len(payloads)
    done = [False] * len(payloads)
    if not payloads:
        return results
    try:
        async with asyncio.timeout(timeout):
            async with websockets.connect(HA_WS, max_size=16 * 1024 * 1024) as ws:
                hello = json.loads(await ws.recv())
                if hello.get("type") != "auth_required":
                    raise RuntimeError(f"Unexpected handshake: {hello}")

                await ws.send(json.dumps({"type": "auth", "access_token": TOKEN}))
                auth = json.loads(await ws.recv())
                if auth.get("type") != "auth_ok":
                    raise RuntimeError(f"HA auth failed: {auth}")

                for i, payload in enumerate(payloads, 1):
                    await ws.send(json.dumps({"id": i, **payload}))
                while not all(done):
                    resp = json.loads(await ws.recv())
                    if resp.get("type") != "result" or resp.get("id") not in range(
                        1, len(payloads) + 1
                    ):
                        continue
                    slot = resp["id"] - 1
                    done[slot] = True
                    results[slot] = (
                        resp["result"]
                        if resp.get("success")
                        else RuntimeError(f"HA error: {resp.get('error')}")
                    )
    except TimeoutError:
        for slot, finished in enumerate(done):
            if not finished:
                results[slot] = TimeoutError(f"no answer within {timeout:g}s")
        if not any(done):
            raise
    return results


async def ha_ws_call(payload: dict, timeout: float = 20) -> dict | list:
    """Open a short-lived HA WebSocket connection, authenticate, send one
    command, and return its `result`. Raises RuntimeError on any HA error."""
    result = (await ha_ws_batch([payload], timeout))[0]
    if isinstance(result, BaseException):
        raise result
    return result


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


# Live event buffer. HA only keeps past states in the recorder, so to see what
# actually arrived at a given moment (e.g. an overnight alarm event) we keep a
# rolling window of recent events from a background WebSocket subscription.
# It runs in its own thread/loop so it starts with the process regardless of
# MCP lifecycle hooks. Only state changes for entities matching
# EVENT_BUFFER_ENTITIES (regex on entity_id) and automation_triggered events
# are kept.
EVENT_BUFFER_ENTITIES = re.compile(
    os.environ.get(
        "EVENT_BUFFER_ENTITIES",
        r"sleep_as_android|nightlight|^person\.|^automation\.",
    )
)
EVENT_BUFFER_SIZE = int(os.environ.get("EVENT_BUFFER_SIZE", "2000"))
_events: deque = deque(maxlen=EVENT_BUFFER_SIZE)
_listener_status = {"connected": False, "since": None, "last_error": None}


def _compact_event(ev: dict) -> dict | None:
    etype = ev.get("event_type")
    data = ev.get("data", {})
    base = {"time_fired": ev.get("time_fired"), "event_type": etype}
    if etype == "state_changed":
        eid = data.get("entity_id", "")
        if not EVENT_BUFFER_ENTITIES.search(eid):
            return None
        old, new = data.get("old_state") or {}, data.get("new_state") or {}
        return {
            **base,
            "entity_id": eid,
            "old_state": old.get("state"),
            "new_state": new.get("state"),
            "new_event_type": (new.get("attributes") or {}).get("event_type"),
        }
    if etype == "automation_triggered":
        return {
            **base,
            "entity_id": data.get("entity_id"),
            "name": data.get("name"),
            "source": data.get("source"),
        }
    return None


async def _listen_events() -> None:
    backoff = 5
    while True:
        try:
            async with websockets.connect(HA_WS, max_size=16 * 1024 * 1024) as ws:
                await ws.recv()  # auth_required
                await ws.send(json.dumps({"type": "auth", "access_token": TOKEN}))
                if json.loads(await ws.recv()).get("type") != "auth_ok":
                    raise RuntimeError("HA auth failed")
                for i, etype in enumerate(("state_changed", "automation_triggered"), 1):
                    await ws.send(
                        json.dumps(
                            {"id": i, "type": "subscribe_events", "event_type": etype}
                        )
                    )
                _listener_status.update(
                    connected=True,
                    since=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                )
                backoff = 5
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("type") == "event":
                        rec = _compact_event(msg["event"])
                        if rec:
                            _events.append(rec)
        except Exception as e:  # reconnect on any failure
            _listener_status.update(connected=False, last_error=repr(e))
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


def _start_event_listener() -> None:
    threading.Thread(
        target=lambda: asyncio.run(_listen_events()),
        name="ha-event-listener",
        daemon=True,
    ).start()


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


def _friendly_errors(fn):
    """Return HA failures as readable text instead of raising.

    The MCP SDK reduces a raised exception to a bare 'Error executing tool X',
    which hides e.g. a 404 for an entity that no longer exists."""

    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        try:
            return await fn(*args, **kwargs)
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            hint = " (not found: the entity or item may not exist)" if status == 404 else ""
            body = e.response.text.strip()[:300]
            return f"Error: Home Assistant returned HTTP {status}{hint} for {e.request.url.path}. {body}"
        except httpx.HTTPError as e:
            return f"Error: could not reach Home Assistant: {type(e).__name__}: {e}"
        except RuntimeError as e:
            return f"Error: {e}"

    return wrapper


def tool(fn):
    """Register fn as an MCP tool with friendly error reporting."""
    return mcp.tool()(_friendly_errors(fn))


@tool
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


_LOG_HEADER = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")


async def _fetch_log_records() -> list[list[str]]:
    """The HA log as records: a timestamped line plus any continuation lines
    (traceback frames, wrapped messages) that follow it."""
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(
            f"{HA}/api/error_log", headers={"Authorization": f"Bearer {TOKEN}"}
        )
        r.raise_for_status()
    records: list[list[str]] = []
    for line in r.text.splitlines():
        if _LOG_HEADER.match(line):
            records.append([line])
        elif records:
            records[-1].append(line)
    return records


@tool
async def get_error_log(
    min_level: str = "WARNING",
    tail_lines: int = 500,
    contains: str = "",
) -> str:
    """Home Assistant log: one line per entry, filtered by level and substring
    (`contains` also searches tracebacks), last `tail_lines` entries. An entry
    with a traceback or continuation lines is marked `[+N lines]`; pass its
    timestamp to get_log_entry to read the rest."""
    levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    ok = levels[levels.index(min_level.upper()) :]
    needle = contains.lower()
    out = []
    for rec in await _fetch_log_records():
        if not any(f" {x} " in rec[0] for x in ok):
            continue
        if needle and needle not in "\n".join(rec).lower():
            continue
        extra = len(rec) - 1
        out.append(rec[0] + (f"  [+{extra} lines]" if extra else ""))
    return "\n".join(out[-tail_lines:])


@tool
async def get_log_entry(at: str, contains: str = "", max_lines: int = 200) -> str:
    """Full text of log entries, including traceback and continuation lines.
    `at` is the start of the entry's timestamp as shown by get_error_log, e.g.
    '2026-11-01 17:30:36.961' (or just '2026-11-01 17:30:36' for every entry in
    that second). `contains` narrows further; output is capped at `max_lines`."""
    at = at.strip()
    needle = contains.lower()
    found = [
        rec
        for rec in await _fetch_log_records()
        if rec[0].startswith(at) and needle in "\n".join(rec).lower()
    ]
    if not found:
        return f"No log entry found starting with {at!r}."
    lines = "\n\n".join("\n".join(rec) for rec in found).splitlines()
    text = "\n".join(lines[:max_lines])
    if len(lines) > max_lines:
        text += f"\n... truncated ({len(lines) - max_lines} more lines)"
    return text


@tool
async def list_dashboards() -> str:
    """List Lovelace dashboards (url_path, title, mode). The default dashboard
    is not included; use url_path=None with get_dashboard_config for it."""
    return json.dumps(await ha_ws_call({"type": "lovelace/dashboards/list"}), indent=2)


@tool
async def get_dashboard_config(url_path: str | None = None) -> str:
    """Full Lovelace config for a dashboard. Omit url_path for the default one."""
    return json.dumps(
        await ha_ws_call({"type": "lovelace/config", "url_path": url_path}),
        indent=2,
    )


@tool
async def list_lovelace_resources() -> str:
    """Custom card/module resources (e.g. HACS cards) registered with Lovelace."""
    return json.dumps(await ha_ws_call({"type": "lovelace/resources"}), indent=2)


@tool
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


@tool
async def get_automation_config(automation_id: str) -> str:
    """Full config (triggers, conditions, actions) of a UI-defined automation.
    `automation_id` is the `id` from list_automations. YAML-defined
    automations without an id are not available here."""
    return _dump(await ha_rest_get(f"/api/config/automation/config/{automation_id}"))


@tool
async def list_traces(automation_id: str) -> str:
    """Recent runs of an automation (HA keeps ~5 by default): run_id,
    timestamps, trigger, state and any error. `automation_id` is the `id`
    from list_automations. An empty list means it has not run recently."""
    return _dump(
        await ha_ws_call(
            {"type": "trace/list", "domain": "automation", "item_id": automation_id}
        )
    )


@tool
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


@tool
async def get_logbook(
    entity_id: str = "",
    hours: float = 48,
    contains: str = "",
    exclude: str = "",
    limit: int = 0,
) -> str:
    """Logbook entries for the last N hours, optionally for one entity. Shows
    automation triggers and state changes, so it can tell 'never triggered'
    from 'triggered but failed'.

    The logbook can be huge (every `last_seen` sensor logs each update), so
    narrow it: `contains` keeps entries whose entity, name, state or message
    contain the text; `exclude` drops entries matching any of its
    comma-separated texts (e.g. `last_seen,sun.sun`); `limit` keeps only the
    newest N (0 = all). The reply gives `matched` (before `limit`)."""
    params = {"entity": entity_id} if entity_id else None
    entries = await ha_rest_get(f"/api/logbook/{_since(hours)}", params)
    needle = contains.lower()
    drop = [x.strip().lower() for x in exclude.split(",") if x.strip()]
    if needle or drop:

        def text(e: dict) -> str:
            return json.dumps(e, default=str).lower()

        entries = [
            e
            for e in entries
            if (not needle or needle in text(e))
            and not any(x in text(e) for x in drop)
        ]
    matched = len(entries)
    if limit > 0:
        entries = entries[-limit:]
    return _dump({"matched": matched, "count": len(entries), "entries": entries})


@tool
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


@tool
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


async def _areas() -> dict[str, dict]:
    """Areas by id, each with its floor's name added as `floor`."""
    areas, floors = await asyncio.gather(
        ha_ws_call({"type": "config/area_registry/list"}),
        ha_ws_call({"type": "config/floor_registry/list"}),
    )
    floor_names = {f["floor_id"]: f["name"] for f in floors}
    return {
        a["area_id"]: {**a, "floor": floor_names.get(a.get("floor_id"))} for a in areas
    }


@tool
async def list_areas() -> str:
    """Floors (with their level) and areas (id, name, floor, aliases). Use it to
    see which rooms exist and on which floor."""
    areas, floors = await asyncio.gather(
        _areas(), ha_ws_call({"type": "config/floor_registry/list"})
    )
    return _dump(
        {
            "floors": [
                {"floor_id": f["floor_id"], "name": f["name"], "level": f.get("level")}
                for f in floors
            ],
            "areas": [
                {
                    "area_id": a["area_id"],
                    "name": a["name"],
                    "floor_id": a.get("floor_id"),
                    "floor": a["floor"],
                    "aliases": a.get("aliases") or [],
                }
                for a in areas.values()
            ],
        }
    )


@tool
async def list_integration_entities(
    domain: str,
    include_attributes: bool = True,
    state: str = "",
    contains: str = "",
    limit: int = 0,
) -> str:
    """Every entity an integration provides (its entity registry `platform`,
    e.g. `house_structure`), with its device, area (the entity's own, else its
    device's), floor, current state and, by default, attributes. Finds them
    even when renamed, unlike get_state's name search.

    Big integrations overflow the reply, so narrow it: `state` keeps entities
    in exactly that state (e.g. `unavailable`), `contains` keeps those whose
    entity id, device or area contains the text, `limit` keeps the first N
    (0 = all), and `include_attributes=False` drops the bulky attributes. The
    reply has `state_counts` for the whole integration (before filtering) and
    `matched` (before `limit`)."""
    entities, devices, areas, states = await asyncio.gather(
        ha_ws_call({"type": "config/entity_registry/list"}),
        ha_ws_call({"type": "config/device_registry/list"}),
        _areas(),
        ha_rest_get("/api/states"),
    )
    devices = {d["id"]: d for d in devices}
    states = {s["entity_id"]: s for s in states}
    out = []
    for e in entities:
        if e.get("platform") != domain:
            continue
        device = devices.get(e.get("device_id")) or {}
        area = areas.get(e.get("area_id") or device.get("area_id")) or {}
        current = states.get(e["entity_id"]) or {}
        item = {
            "entity_id": e["entity_id"],
            "unique_id": e.get("unique_id"),
            "disabled_by": e.get("disabled_by"),
            "device": device.get("name_by_user") or device.get("name"),
            "device_model": device.get("model"),
            "area_id": area.get("area_id"),
            "area": area.get("name"),
            "floor": area.get("floor"),
            "state": current.get("state"),
        }
        if include_attributes:
            item["attributes"] = current.get("attributes")
        out.append(item)
    state_counts: dict[str, int] = {}
    for item in out:
        key = str(item["state"])
        state_counts[key] = state_counts.get(key, 0) + 1
    needle = contains.lower()
    out = [
        item
        for item in out
        if (not state or item["state"] == state)
        and (
            not needle
            or needle
            in f"{item['entity_id']} {item['device']} {item['area']}".lower()
        )
    ]
    out.sort(key=lambda x: x["entity_id"])
    matched = len(out)
    if limit > 0:
        out = out[:limit]
    return _dump(
        {
            "total": sum(state_counts.values()),
            "state_counts": state_counts,
            "matched": matched,
            "count": len(out),
            "entities": out,
        }
    )


@tool
async def get_config_entries(domain: str) -> str:
    """An integration's config entries (title, state, any setup error) and
    their subentries (id, type, title). HA's API doesn't expose the saved
    settings; when the integration supports diagnostics they are under
    `diagnostics` (HA redacts what the integration marks secret), otherwise
    `diagnostics` says it isn't available."""
    entries = await ha_ws_call({"type": "config_entries/get", "domain": domain})
    out = []
    for entry in entries:
        entry_id = entry["entry_id"]
        item = {
            k: entry.get(k)
            for k in ("entry_id", "title", "state", "disabled_by", "reason")
        }
        item["subentries"] = await ha_ws_call(
            {"type": "config_entries/subentries/list", "entry_id": entry_id}
        )
        try:
            diagnostics = await ha_rest_get(
                f"/api/diagnostics/config_entry/{quote(entry_id)}"
            )
            item["diagnostics"] = diagnostics.get("data", diagnostics)
        except httpx.HTTPStatusError as e:
            item["diagnostics"] = (
                f"not available (HTTP {e.response.status_code}); "
                "the integration may not support diagnostics"
            )
        out.append(item)
    return _dump(out)


async def _house_structure_from_diagnostics() -> dict:
    """Fallback when house_structure/layout is not yet available on HA."""
    entries = await ha_ws_call(
        {"type": "config_entries/get", "domain": "house_structure"}
    )
    if not entries:
        return {"error": "House Structure is not set up"}
    entry_id = entries[0]["entry_id"]
    diagnostics = await ha_rest_get(
        f"/api/diagnostics/config_entry/{quote(entry_id)}"
    )
    data = diagnostics.get("data", diagnostics)
    # Drop live entity/sky noise so the layout stays readable.
    for part in data.get("parts") or []:
        part.pop("entities", None)
    data.pop("sky", None)
    data["source"] = "diagnostics"
    return data


@tool
async def get_house_structure(
    include_sheet: bool = True,
    detail: str = "full",
) -> str:
    """Current House Structure layout from Home Assistant: house settings
    (rotation, unit, shading), each part's saved settings and built attributes
    (rooms with size/position/neighbours, walls/windows/doors/internal walls/
    registers), every area with the parts in it and its neighbours, and by
    default the parts as a CSV `sheet` (same columns as the Reconfigure form).

    Prefer this over get_config_entries("house_structure") when reviewing how
    rooms are laid out or which faces are outside vs open.

    `include_sheet` (default true) asks for the CSV. `detail` is `full` (default:
    settings + parts + areas + sheet) or `sheet` (house settings + CSV only).
    Falls back to diagnostics when the integration is older than the layout
    websocket command."""
    detail = (detail or "full").strip().lower()
    if detail not in ("full", "sheet"):
        return "Error: detail must be 'full' or 'sheet'."
    want_sheet = include_sheet or detail == "sheet"
    try:
        data = await ha_ws_call(
            {"type": "house_structure/layout", "include_sheet": want_sheet}
        )
        if isinstance(data, dict):
            data["source"] = "house_structure/layout"
        else:
            data = {"error": f"unexpected layout reply: {data!r}"}
    except RuntimeError as ws_error:
        try:
            data = await _house_structure_from_diagnostics()
        except Exception:
            return f"Error: {ws_error}"
    if data.get("error"):
        return _dump(data)
    if detail == "sheet":
        slim = {
            "source": data.get("source"),
            "house": data.get("house"),
            "options": data.get("options"),
            "sheet": data.get("sheet"),
            "skipped": data.get("skipped"),
        }
        if slim["sheet"] is None and data.get("parts") is not None:
            slim["note"] = (
                "CSV sheet unavailable on this HA build; use detail='full' "
                "for parts and areas, or update House Structure."
            )
        return _dump(slim, limit=120000)
    if not include_sheet:
        data = {k: v for k, v in data.items() if k != "sheet"}
    return _dump(data, limit=120000)


async def _manifests() -> tuple[list[dict], str | None]:
    """Manifests of the integrations HA has loaded, or an error message if HA
    refuses the request (so the rest of the integration info still comes back)."""
    try:
        return await ha_ws_call({"type": "manifest/list"}), None
    except Exception as e:  # keep the other data even if this call fails
        return [], f"{type(e).__name__}: {e}"


@tool
async def list_integrations(contains: str = "", custom_only: bool = False) -> str:
    """Integrations installed in Home Assistant, each with whether it is
    activated and its version. `activated` is true when the integration is
    loaded (a loaded config entry, or loaded from YAML). `entries` gives the
    state of each config entry (loaded, setup_error, not_loaded, ...) and
    `disabled_by` when it has been disabled. `version` is the integration's own
    version for custom integrations (e.g. HACS); core integrations have no
    version of their own, so they report the Home Assistant core version.
    `contains` filters on domain or name; `custom_only` keeps only custom
    integrations."""
    config, entries, (manifests, manifest_error) = await asyncio.gather(
        ha_rest_get("/api/config"),
        ha_ws_call({"type": "config_entries/get"}),
        _manifests(),
    )
    loaded = {c.split(".", 1)[0] for c in config.get("components", [])}
    core_version = config.get("version")

    by_domain: dict[str, list[dict]] = {}
    for e in entries:
        by_domain.setdefault(e["domain"], []).append(
            {
                "title": e.get("title"),
                "state": e.get("state"),
                "disabled_by": e.get("disabled_by"),
            }
        )
    info = {m["domain"]: m for m in manifests if m.get("domain")}

    # manifest/list only covers loaded integrations. Entries of unloaded ones
    # (disabled, failed setup) need manifest/get, or a custom one would look core.
    missing = sorted(set(by_domain) - set(info))
    if missing:
        fetched = await asyncio.gather(
            *(
                ha_ws_call({"type": "manifest/get", "integration": d})
                for d in missing
            ),
            return_exceptions=True,
        )
        for d, res in zip(missing, fetched):
            if isinstance(res, dict):
                info[d] = res

    needle = contains.lower()
    out = []
    for domain in sorted(set(info) | set(by_domain) | loaded):
        m = info.get(domain)
        name = (m or {}).get("name") or domain
        if needle and needle not in f"{domain} {name}".lower():
            continue
        # Every manifest carries is_built_in; custom ones also declare a version.
        custom = m is not None and not m.get("is_built_in", True)
        if custom_only and not custom:
            continue
        domain_entries = by_domain.get(domain, [])
        # HA keeps a domain in `components` even when its setup failed, so with
        # entries, only a loaded entry counts; YAML-only ones use `components`.
        if domain_entries:
            activated = any(x["state"] == "loaded" for x in domain_entries)
        else:
            activated = domain in loaded
        out.append(
            {
                "domain": domain,
                "name": name,
                "custom": custom,
                "version": (m or {}).get("version") if custom else core_version,
                "activated": activated,
                "entries": domain_entries,
            }
        )
    return _dump(
        {
            "ha_version": core_version,
            "manifest_error": manifest_error,
            "count": len(out),
            "integrations": out,
        }
    )


async def _repairs() -> list[dict]:
    """Home Assistant's repairs (Settings > Repairs), compacted."""
    result = await ha_ws_call({"type": "repairs/list_issues"})
    keys = (
        "domain",
        "issue_id",
        "severity",
        "translation_key",
        "translation_placeholders",
        "is_fixable",
        "ignored",
        "created",
        "breaks_in_ha_version",
        "learn_more_url",
    )
    return [{k: i.get(k) for k in keys if k in i} for i in result.get("issues", [])]


@tool
async def list_repairs(domain: str = "", contains: str = "") -> str:
    """Home Assistant's repairs (Settings > Repairs): each issue's domain, id,
    severity, translation key and placeholders, and whether it is fixable or
    ignored. The Marketplace (HACS before Home Assistant 2026.11) raises one
    when an install or update needs a restart. `domain` keeps
    one integration's issues; `contains` searches the whole issue."""
    needle = contains.lower()
    out = [
        i
        for i in await _repairs()
        if (not domain or i.get("domain") == domain)
        and (not needle or needle in json.dumps(i, default=str).lower())
    ]
    return _dump({"count": len(out), "issues": out})


_STORE_KEYS = (
    "full_name",
    "name",
    "category",
    "domain",
    "custom",
    "installed",
    "installed_version",
    "available_version",
    "status",
    "pending_upgrade",
    "last_updated",
)
# Home Assistant 2026.11 replaced HACS with its built-in Marketplace, which
# answers in the same shape; HACS is tried for older versions.
_STORES = (
    ("The Marketplace", "marketplace/repositories/list"),
    ("HACS", "hacs/repositories/list"),
)


async def _store_repositories() -> tuple[list[dict], str | None, str | None]:
    """The repositories the Marketplace (or HACS) knows about.

    Returns (repositories, which store answered, error): the store is None and
    the error says why when neither answers.
    """
    errors = []
    for store, command in _STORES:
        try:
            result = await ha_ws_call({"type": command}, timeout=60)
        except Exception as e:  # not there (unknown_command), or refused
            errors.append(f"{store}: {type(e).__name__}: {e}")
            continue
        repos = [{k: r.get(k) for k in _STORE_KEYS if k in r} for r in result]
        return repos, store, None
    return [], None, "Neither the Marketplace nor HACS answered. " + "; ".join(errors)


@tool
async def list_marketplace_repositories(
    contains: str = "", installed_only: bool = True
) -> str:
    """Repositories the Marketplace knows about (Settings > Marketplace; HACS
    before Home Assistant 2026.11, which is used when there's no Marketplace):
    GitHub name, category, the integration's domain, whether it is a custom
    repository (`custom`, added with "Add from link"), whether it is installed,
    the installed and newest versions, and its `status` (new, installed,
    pending-upgrade, or pending-restart until Home Assistant restarts).
    `contains` filters on name or domain; `installed_only` (default) keeps the
    installed ones. `store` says which answered; `error` is set if neither."""
    repos, store, error = await _store_repositories()
    needle = contains.lower()
    out = []
    for r in repos:
        if installed_only and not r.get("installed"):
            continue
        names = f"{r.get('full_name')} {r.get('name')} {r.get('domain')}".lower()
        if needle and needle not in names:
            continue
        out.append(r)
    return _dump(
        {"store": store, "error": error, "count": len(out), "repositories": out}
    )


# Logged at every start for every custom integration: not a problem.
_UNTESTED = "has not been tested by Home Assistant"


def _failed(result) -> str | None:
    """A gathered call's error as text, or None if it succeeded."""
    if isinstance(result, BaseException):
        return f"{type(result).__name__}: {result}"
    return None


def _findings(
    domain: str,
    manifest: dict | None,
    offers_setup: bool | None,
    entries: list[dict],
    deps: list[dict],
    store: str | None,
    repos: list[dict],
    log: list[str],
) -> list[str]:
    """What diagnose_integration's results add up to, in plain words."""
    out: list[str] = []
    downloaded = any(r.get("installed") for r in repos)
    who = store or "HACS"
    if manifest is None:
        if downloaded:
            out.append(
                f"{who} has installed it, but Home Assistant hasn't found it: "
                "restart Home Assistant."
            )
        elif repos:
            out.append(f"{who} knows the repository but hasn't installed it.")
        else:
            out.append(
                "Home Assistant doesn't know this domain: it isn't installed "
                "(or its folder in custom_components has another name), or "
                "Home Assistant hasn't restarted since."
            )
            if store:
                out.append(
                    f"{store} has no record of it: add its repository there "
                    "(Add from link), or copy it into custom_components by "
                    "hand and restart."
                )
            else:
                out.append(
                    "Neither the Marketplace nor HACS is available, so it can "
                    "only be copied into custom_components by hand (then "
                    "restart)."
                )
    else:
        if manifest.get("config_flow") and offers_setup is False:
            out.append(
                "Its manifest says it has a config flow, but Home Assistant "
                "doesn't offer it: look in the log for errors loading it."
            )
        if not manifest.get("config_flow"):
            out.append("Its manifest has no config flow (set up from YAML only).")
        for d in deps:
            if not d["loaded"]:
                out.append(
                    f"Its dependency {d['domain']!r} isn't loaded, so it can't "
                    "be set up until that is."
                )
    for e in entries:
        if e["state"] not in ("loaded", "not_loaded"):
            reason = e.get("reason") or "no reason given"
            out.append(f"Entry {e['title']!r} is {e['state']}: {reason}.")
    for r in repos:
        if "restart" in str(r.get("status") or ""):
            out.append(f"{who} says a restart is pending for it.")
        running = (manifest or {}).get("version")
        have = r.get("installed_version")
        # a branch download's "version" is a commit hash: only compare versions
        is_version = re.match(r"v?\d+\.", str(have or ""))
        if r.get("installed") and running and is_version and str(running) not in have:
            out.append(
                f"{who} has {have} but Home Assistant is running {running}: "
                "restart Home Assistant."
            )
    if log:
        many = "s mention" if len(log) > 1 else " mentions"
        out.append(f"{len(log)} warning or error{many} {domain} in the log.")
    return out or ["Nothing wrong found."]


@tool
async def diagnose_integration(domain: str, log_lines: int = 30) -> str:
    """Why an integration can't be set up, or isn't working: everything about
    one domain in one call.

    - `findings`: what the rest adds up to, in plain words.
    - `known`: whether Home Assistant has found it, with its `manifest` (name,
      version, config_flow, dependencies, ...). A custom integration
      downloaded since the last restart isn't known until Home Assistant
      restarts.
    - `offers_setup_form`: whether it is among the integrations Home
      Assistant offers under "Add integration".
    - `entries`: its config entries' state and any setup error.
    - `dependencies`: each dependency in its manifest, and whether it is loaded.
    - `store`, `store_record`: which of the Marketplace (Home Assistant
      2026.11 on) or HACS answered, and its record of it (installed,
      versions, status); `store_error` when neither answered.
    - `repairs`: its repairs, and the Marketplace's (or HACS's) restart
      repairs.
    - `log`: the last `log_lines` warnings and errors that mention it (read
      one in full with get_log_entry).
    """
    results = await asyncio.gather(
        ha_ws_call({"type": "manifest/get", "integration": domain}),
        ha_rest_get("/api/config/config_entries/flow_handlers"),
        ha_ws_call({"type": "config_entries/get", "domain": domain}),
        ha_rest_get("/api/config"),
        _store_repositories(),
        _repairs(),
        _fetch_log_records(),
        return_exceptions=True,
    )
    manifest, handlers, entries, config, store_result, repairs, records = results

    manifest = manifest if isinstance(manifest, dict) else None
    offers_setup = domain in handlers if isinstance(handlers, list) else None
    loaded = set()
    if isinstance(config, dict):
        loaded = {c.split(".", 1)[0] for c in config.get("components", [])}
    entries = [
        {k: e.get(k) for k in ("entry_id", "title", "state", "reason", "disabled_by")}
        for e in (entries if isinstance(entries, list) else [])
    ]
    deps = [
        {"domain": d, "loaded": d in loaded}
        for d in (manifest or {}).get("dependencies", [])
    ]
    repos, store, store_error = (
        store_result
        if isinstance(store_result, tuple)
        else ([], None, _failed(store_result))
    )
    record = [r for r in repos if r.get("domain") == domain]
    own_repairs = []
    for issue in repairs if isinstance(repairs, list) else []:
        text = json.dumps(issue, default=str).lower()
        ours = issue.get("domain") == domain
        store_restart = issue.get("domain") in ("marketplace", "hacs") and (
            domain in text or "restart" in text
        )
        if ours or store_restart:
            own_repairs.append(issue)
    log: list[str] = []
    for rec in records if isinstance(records, list) else []:
        if domain not in "\n".join(rec) or _UNTESTED in rec[0]:
            continue
        if not any(f" {x} " in rec[0] for x in ("WARNING", "ERROR", "CRITICAL")):
            continue
        extra = len(rec) - 1
        log.append(rec[0] + (f"  [+{extra} lines]" if extra else ""))
    log = log[-log_lines:]

    calls = ("manifest", "flow_handlers", "config_entries", "config")
    errors = {
        name: _failed(r)
        for name, r in zip(
            (*calls, "store", "repairs", "log"), results, strict=True
        )
        if name != "manifest" and _failed(r)
    }
    return _dump(
        {
            "domain": domain,
            "findings": _findings(
                domain, manifest, offers_setup, entries, deps, store, record, log
            ),
            "known": manifest is not None,
            "manifest": manifest,
            "offers_setup_form": offers_setup,
            "entries": entries,
            "dependencies": deps,
            "store": store,
            "store_record": record,
            "store_error": store_error,
            "repairs": own_repairs,
            "log": log,
            "errors": errors,
        }
    )


@tool
async def get_recent_events(
    contains: str = "", event_type: str = "", minutes: float = 720
) -> str:
    """Events this server has seen live from HA's event bus since it started,
    independent of the recorder. Covers state changes of Sleep as Android,
    nightlight, person and automation entities, plus automation_triggered.
    Event entities show `new_event_type` (e.g. alert_start). Filter with
    `contains` (substring of entity_id/name), `event_type` (state_changed or
    automation_triggered) and `minutes` of lookback. The buffer is in memory,
    so it is empty after a server restart; `listener` shows its health."""
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
    needle = contains.lower()
    out = [
        e
        for e in list(_events)
        if (e["time_fired"] or "") >= cutoff
        and (not event_type or e["event_type"] == event_type)
        and (not needle or needle in json.dumps(e).lower())
    ]
    return _dump({"listener": _listener_status, "count": len(out), "events": out})


_ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


async def _docker_get(
    path: str,
    params: dict | None = None,
    timeout: float = 20,
    container: str | None = None,
):
    """GET the Docker Engine API over its socket. `container` is only used to
    word errors; it defaults to the Matter container, which must be set."""
    container = MATTER_CONTAINER if container is None else container
    if not container:
        raise RuntimeError(
            "MATTER_CONTAINER is not set: put the name of the Matter server's "
            "Docker container in that environment variable."
        )
    try:
        async with httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=DOCKER_SOCKET),
            base_url="http://docker",
            timeout=timeout,
        ) as c:
            r = await c.get(path, params=params)
    except (httpx.HTTPError, OSError) as e:
        raise RuntimeError(
            f"could not reach Docker through {DOCKER_SOCKET} "
            f"({type(e).__name__}: {e}). Mount the socket into this container, "
            "e.g. -v /var/run/docker.sock:/var/run/docker.sock:ro"
        ) from e
    if r.status_code == 404:
        raise RuntimeError(
            f"Docker has no container named {container!r} "
            "(for the Matter server, check MATTER_CONTAINER)"
        )
    if r.status_code >= 400:
        raise RuntimeError(f"Docker returned HTTP {r.status_code}: {r.text[:300]}")
    return r


def _demux_docker_logs(raw: bytes) -> str:
    """Docker logs for a container without a TTY come as 8-byte-framed stdout/
    stderr chunks; with a TTY they are plain text. Handle both."""
    if len(raw) < 8 or raw[0] not in (0, 1, 2) or raw[1:4] != b"\0\0\0":
        return raw.decode("utf-8", errors="replace")
    chunks, i = [], 0
    while i + 8 <= len(raw):
        size = int.from_bytes(raw[i + 4 : i + 8], "big")
        chunks.append(raw[i + 8 : i + 8 + size])
        i += 8 + size
    return b"".join(chunks).decode("utf-8", errors="replace")


@tool
async def get_matter_server_log(
    tail_lines: int = 200,
    contains: str = "",
    since_minutes: float = 0,
    fetch_lines: int = 5000,
) -> str:
    """Log of the Matter server's Docker container (named by the MATTER_CONTAINER
    environment variable): mDNS discovery, CASE/PASE sessions, commissioning
    and node availability, none of which reaches Home Assistant's own log.
    Needs the Docker socket mounted into this container.

    Lines are in Docker's UTC timestamps. `contains` keeps lines containing the
    text (case-insensitive, e.g. `error`, `commission`, `node 3`); `tail_lines`
    keeps the last N matches. `since_minutes` only looks that far back (0 = no
    limit), and `fetch_lines` is how many of the newest lines are read before
    filtering. `container` describes the container: status, when it started
    (a recent start explains offline nodes), restart count, network mode,
    addresses and its TZ setting."""
    params = {
        "stdout": 1,
        "stderr": 1,
        "timestamps": 1,
        "tail": max(1, min(fetch_lines, 50000)),
    }
    if since_minutes > 0:
        since = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)
        params["since"] = int(since.timestamp())
    inspect, logs = await asyncio.gather(
        _docker_get(f"/containers/{quote(MATTER_CONTAINER, safe='')}/json"),
        _docker_get(
            f"/containers/{quote(MATTER_CONTAINER, safe='')}/logs",
            params,
            timeout=60,
        ),
    )
    info = inspect.json()
    st, cfg = info.get("State") or {}, info.get("Config") or {}
    networks = (info.get("NetworkSettings") or {}).get("Networks") or {}
    container = {
        "name": (info.get("Name") or "").lstrip("/"),
        "image": cfg.get("Image"),
        "status": st.get("Status"),
        "started_at": st.get("StartedAt"),
        "finished_at": st.get("FinishedAt") if not st.get("Running") else None,
        "exit_code": st.get("ExitCode"),
        "oom_killed": st.get("OOMKilled"),
        "restart_count": info.get("RestartCount"),
        "health": (st.get("Health") or {}).get("Status"),
        "network_mode": (info.get("HostConfig") or {}).get("NetworkMode"),
        "networks": {
            n: {"ip": v.get("IPAddress"), "ipv6": v.get("GlobalIPv6Address")}
            for n, v in networks.items()
        },
        "tz": next((e for e in cfg.get("Env") or [] if e.startswith("TZ=")), None),
    }
    needle = contains.lower()
    lines = [
        _ANSI.sub("", ln)
        for ln in _demux_docker_logs(logs.content).splitlines()
        if not needle or needle in ln.lower()
    ]
    return _dump(
        {
            "container": container,
            "matched": len(lines),
            "lines": lines[-tail_lines:] if tail_lines > 0 else lines,
        },
        limit=120000,
    )


def _matter_findings(nodes: list[dict]) -> list[str]:
    """What the Matter nodes' availability and ping results add up to."""
    out = []
    for n in nodes:
        who = f"Node {n['node_id']} ({n['label']})"
        if n.get("available"):
            continue
        ping = n.get("ping")
        if not n["ip_addresses"]:
            out.append(
                f"{who} is unavailable and the server knows no address for it: "
                "mDNS discovery isn't reaching it (look at the Matter server's "
                "network mode and multicast/IPv6 on the host and router)."
            )
        elif isinstance(ping, dict) and ping and not any(ping.values()):
            out.append(
                f"{who} is unavailable and answers no ping at "
                f"{', '.join(ping)}: the Matter server can't reach it (device "
                "off or on another network, or the server's network can't "
                "route to it)."
            )
        elif isinstance(ping, dict) and any(ping.values()):
            out.append(
                f"{who} is unavailable but answers ping: the network path is "
                "fine, so the Matter session is failing. Read the Matter "
                "server log (get_matter_server_log) for CASE/mDNS errors, or "
                "reload the Matter integration."
            )
        else:
            out.append(f"{who} is unavailable (ping gave no answer: {ping}).")
    return out or ["Every Matter node is available."]


@tool
async def get_matter_nodes(ping: bool = True, include_topology: bool = False) -> str:
    """Live state of every Matter node, asked of the Matter server through Home
    Assistant: whether it is `available`, its network type (wifi/thread/
    ethernet), IP and MAC addresses, active fabrics, the devices behind it (a
    bridge such as an Aqara hub carries many), and, with `ping`, whether the
    server can ping each of its addresses right now. `findings` says what the
    results add up to. `include_topology` adds the Matter network topology
    graph (Thread/Wi-Fi links), if the server supports it. This is far smaller
    than get_config_entries("matter")."""
    registry = await ha_ws_call({"type": "config/device_registry/list"})
    devices = [
        d
        for d in registry
        if any(i and i[0] == "matter" for i in d.get("identifiers") or [])
    ]
    if not devices:
        return _dump({"count": 0, "nodes": [], "findings": ["No Matter devices."]})

    diags = await ha_ws_batch(
        [{"type": "matter/node_diagnostics", "device_id": d["id"]} for d in devices]
    )
    nodes: dict = {}
    errors = []
    for d, diag in zip(devices, diags, strict=True):
        name = d.get("name_by_user") or d.get("name")
        if isinstance(diag, BaseException):
            errors.append({"device": name, "error": str(diag)})
            continue
        node = nodes.setdefault(
            diag.get("node_id"),
            {"diag": diag, "device_id": d["id"], "devices": []},
        )
        node["devices"].append(
            {"name": name, "model": d.get("model"), "manufacturer": d.get("manufacturer")}
        )

    pings: dict = {}
    if ping and nodes:
        extra = [{"type": "matter/network_topology"}] if include_topology else []
        results = await ha_ws_batch(
            [{"type": "matter/ping_node", "device_id": n["device_id"]} for n in nodes.values()]
            + extra,
            timeout=60,
        )
        pings = dict(zip(nodes, results))
        topology = results[len(nodes)] if extra else None
    elif include_topology:
        (topology,) = await ha_ws_batch([{"type": "matter/network_topology"}])
    else:
        topology = None

    out = []
    for node_id, n in sorted(nodes.items(), key=lambda kv: kv[0] or 0):
        diag = n["diag"]
        result = pings.get(node_id)
        out.append(
            {
                "node_id": node_id,
                "label": n["devices"][0]["model"] or n["devices"][0]["name"],
                "available": diag.get("available"),
                "network_type": diag.get("network_type"),
                "node_type": diag.get("node_type"),
                "network_name": diag.get("network_name"),
                # the field is spelled "ip_adresses" in the Matter server's model
                "ip_addresses": diag.get("ip_adresses") or diag.get("ip_addresses") or [],
                "mac_address": diag.get("mac_address"),
                "active_fabrics": len(diag.get("active_fabrics") or []),
                "ping": str(result) if isinstance(result, BaseException) else result,
                "device_count": len(n["devices"]),
                "devices": n["devices"][:40],
            }
        )
    reply = {
        "count": len(out),
        "findings": _matter_findings(out),
        "nodes": out,
        "errors": errors,
    }
    if include_topology:
        reply["topology"] = (
            str(topology) if isinstance(topology, BaseException) else topology
        )
    return _dump(reply)


_MDNS_TYPES = {
    "_matter._tcp.local.": "operational: commissioned Matter nodes",
    "_matterc._udp.local.": "commissionable: devices with a commissioning window open",
    "_meshcop._udp.local.": "Thread border routers",
}
_VENDORS = {4476: "IKEA", 4447: "Aqara", 4874: "Eve", 4939: "Nanoleaf"}
_MATTER_ID = re.compile(r"deviceid_([0-9A-Fa-f]{16})-([0-9A-Fa-f]{16})")


def _local_addresses() -> dict[str, list[str]]:
    """This container's own interface addresses (loopback left out)."""
    import ifaddr

    out: dict[str, list[str]] = {}
    for adapter in ifaddr.get_adapters():
        ips = []
        for ip in adapter.ips:
            addr = str(ip.ip if ip.is_IPv4 else ip.ip[0])
            if not addr.startswith("127.") and addr != "::1":
                ips.append(addr)
        if ips:
            out[adapter.nice_name] = ips
    return out


async def _container_network(name: str) -> dict:
    """A container's network mode and networks, from the Docker socket."""
    try:
        r = await _docker_get(
            f"/containers/{quote(name, safe='')}/json", container=name
        )
    except RuntimeError as e:
        return {"error": str(e)}
    info = r.json()
    nets = (info.get("NetworkSettings") or {}).get("Networks") or {}
    return {
        "name": (info.get("Name") or "").lstrip("/"),
        "id": info.get("Id"),
        "network_mode": (info.get("HostConfig") or {}).get("NetworkMode"),
        "networks": {
            n: {
                "network_id": v.get("NetworkID"),
                "ip": v.get("IPAddress"),
                "ipv6": v.get("GlobalIPv6Address"),
            }
            for n, v in nets.items()
        },
    }


def _own_container_id() -> str | None:
    """This container's Docker ID, read from /proc (the hostname can't be used:
    a container that shares another's network stack shares its hostname too)."""
    for path in ("/proc/self/mountinfo", "/proc/self/cgroup"):
        try:
            text = open(path).read()
        except OSError:
            continue
        if m := re.search(r"/containers/([0-9a-f]{64})/", text) or re.search(
            r"docker[-/]([0-9a-f]{64})", text
        ):
            return m.group(1)
    return None


def _network_relation(
    me: dict, matter: dict, local: dict[str, list[str]] | None = None
) -> dict:
    """Whether this server hears the same mDNS as the Matter server, and why.
    `local` is this container's own addresses, used when it couldn't be
    inspected."""
    if "error" in me and "error" not in matter and local:
        theirs = {n["ip"] for n in matter["networks"].values() if n.get("ip")}
        mine = {ip for ips in local.values() for ip in ips}
        if theirs and theirs <= mine:
            return {
                "same_view": True,
                "explanation": "This container has all of the Matter container's "
                f"addresses ({', '.join(sorted(theirs))}), so it shares its "
                "network stack and hears the same mDNS.",
            }
    if "error" in me or "error" in matter:
        return {
            "same_view": None,
            "explanation": "Couldn't compare the two containers' networks: "
            + (me.get("error") or matter.get("error")),
        }
    mine, theirs = me["network_mode"] or "", matter["network_mode"] or ""

    def joins(mode: str, other: dict) -> bool:  # "container:<name or id>"
        ref = mode.split(":", 1)[1] if mode.startswith("container:") else ""
        oid = other.get("id") or ""
        return bool(ref) and (ref == other.get("name") or oid.startswith(ref))

    if mine == theirs == "host":
        return {"same_view": True, "explanation": "Both use the host's network."}
    if joins(mine, matter) or joins(theirs, me):
        return {
            "same_view": True,
            "explanation": "One container joins the other's network stack, "
            "so they see exactly the same network.",
        }
    if "host" in (mine, theirs):
        on_host = "this server" if mine == "host" else "the Matter server"
        return {
            "same_view": False,
            "explanation": f"Only {on_host} uses the host's network "
            f"(this server: {mine}; Matter server: {theirs}). What it hears "
            "on your LAN, the other doesn't, so these results do NOT show "
            "what the Matter server sees. Run this server with the same "
            "network mode (or network_mode: container:<matter container>).",
        }
    shared = {v["network_id"] for v in me["networks"].values() if v["network_id"]} & {
        v["network_id"] for v in matter["networks"].values() if v["network_id"]
    }
    if shared:
        return {
            "same_view": True,
            "explanation": "Both are on the same Docker network, so they see the "
            "same thing, but a Docker bridge does not carry your LAN's "
            "multicast: neither can hear LAN devices over mDNS. If the Matter "
            "server is on a bridge, that alone explains failed discovery; it "
            "needs host networking (or macvlan).",
        }
    return {
        "same_view": False,
        "explanation": "The two containers are on different networks "
        f"(this server: {mine}; Matter server: {theirs}).",
    }


async def _browse_mdns(types: list[str], seconds: float, ip_version) -> dict:
    """Browse mDNS service types for `seconds`; {type: {name: details}}."""
    from zeroconf import ServiceStateChange
    from zeroconf.asyncio import AsyncServiceBrowser, AsyncServiceInfo, AsyncZeroconf

    azc = AsyncZeroconf(ip_version=ip_version)
    found: dict[str, dict[str, dict]] = {t: {} for t in types}
    tasks: set[asyncio.Future] = set()

    async def resolve(service_type: str, name: str) -> None:
        info = AsyncServiceInfo(service_type, name)
        ok = await info.async_request(azc.zeroconf, 3000)
        txt = {
            k.decode(errors="replace"): (v or b"").decode(errors="replace")
            for k, v in (info.properties if ok else {}).items()
        }
        found[service_type][name].update(
            resolved=bool(ok),
            host=info.server,
            port=info.port,
            addresses=info.parsed_addresses() if ok else [],
            txt=txt,
        )

    def on_change(zeroconf, service_type, name, state_change) -> None:
        if state_change is ServiceStateChange.Removed:
            return
        found[service_type].setdefault(name, {"resolved": False})
        task = asyncio.ensure_future(resolve(service_type, name))
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    browser = AsyncServiceBrowser(azc.zeroconf, types, handlers=[on_change])
    try:
        await asyncio.sleep(seconds)
        if tasks:
            await asyncio.wait(tasks, timeout=5)
    finally:
        await browser.async_cancel()
        await azc.async_close()
    return found


@tool
async def test_mdns(seconds: float = 6, extra_types: str = "") -> str:
    """Listen for mDNS (multicast DNS) from this server's network for a few
    seconds and report what answers: commissioned Matter nodes
    (`_matter._tcp`, matched to Home Assistant's node IDs), devices with a
    commissioning window open (`_matterc._udp`; a device you are sharing to
    Home Assistant appears here only while the other app has it open, so run
    this during the share), and Thread border routers (`_meshcop._udp`).
    `extra_types` adds comma-separated service types, e.g. `_hap._tcp.local.`.

    This only says what the MATTER SERVER sees if the two share a network:
    `network` compares this container with MATTER_CONTAINER (needs the Docker
    socket) and says whether the results apply. Matter devices on other
    ecosystems' fabrics (IKEA, Google) are listed too, with `ours: false`."""
    seconds = max(2.0, min(seconds, 30.0))
    types = list(_MDNS_TYPES)
    for t in (x.strip() for x in extra_types.split(",")):
        if t:
            types.append(t if t.endswith(".") else t + ".")

    from zeroconf import IPVersion

    ip_note = None
    try:
        found = await _browse_mdns(types, seconds, IPVersion.All)
    except Exception as first:  # e.g. no IPv6 in this container
        try:
            found = await _browse_mdns(types, seconds, IPVersion.V4Only)
            ip_note = f"IPv6 mDNS wasn't available here ({first!r}); IPv4 only."
        except Exception as e:
            return (
                f"Error: could not listen for mDNS from this container: "
                f"{type(e).__name__}: {e}"
            )

    try:
        devices = await ha_ws_call({"type": "config/device_registry/list"})
    except Exception:
        devices = []
    fabrics, known_nodes = set(), set()
    for d in devices:
        for i in d.get("identifiers") or []:
            if i and i[0] == "matter" and (m := _MATTER_ID.search(str(i[1]))):
                fabrics.add(m.group(1).upper())
                known_nodes.add(int(m.group(2), 16))

    services: dict[str, list[dict]] = {}
    for stype, names in found.items():
        rows = []
        for name, d in sorted(names.items()):
            instance = name.removesuffix("." + stype).removesuffix(stype)
            row = {"instance": instance, **d}
            if stype == "_matter._tcp.local." and "-" in instance:
                fabric, _, node = instance.partition("-")
                row["fabric"] = fabric.upper()
                row["ours"] = fabric.upper() in fabrics
                try:
                    row["node_id"] = int(node, 16)
                except ValueError:
                    pass
            vp = (d.get("txt") or {}).get("VP", "")
            if vp.split("+")[0].isdigit():
                vendor = int(vp.split("+")[0])
                row["vendor"] = _VENDORS.get(vendor, vendor)
            rows.append(row)
        services[stype] = rows

    me_name = _own_container_id() or socket.gethostname()
    local = _local_addresses()
    if MATTER_CONTAINER:
        me, matter = await asyncio.gather(
            _container_network(me_name), _container_network(MATTER_CONTAINER)
        )
        relation = _network_relation(me, matter, local)
        network = {"this_server": me, "matter_server": matter, **relation}
    else:
        network = {
            "same_view": None,
            "explanation": "MATTER_CONTAINER isn't set, so I can't tell whether "
            "the Matter server shares this network.",
        }

    findings = []
    if network["same_view"] is False:
        findings.append("WARNING: " + network["explanation"])
    elif network["same_view"] is None:
        findings.append(network["explanation"])
    elif "bridge" in network["explanation"]:
        findings.append(network["explanation"])
    if not any(services.values()):
        findings.append(
            "Nothing answered mDNS at all: multicast isn't reaching this "
            "container (a Docker bridge network, or a switch/router/Wi-Fi "
            "setting dropping multicast)."
        )
    seen = {r.get("node_id") for r in services["_matter._tcp.local."] if r.get("ours")}
    for node in sorted(known_nodes - seen):
        findings.append(
            f"Matter node {node} isn't advertising _matter._tcp here: it can't "
            "be found by mDNS from this network (off, asleep, or mDNS blocked)."
        )
    routers = len(services["_meshcop._udp.local."])
    findings.append(
        f"{routers} Thread border router(s) seen."
        if routers
        else "No Thread border router seen: a Thread device (e.g. IKEA's "
        "plugs) can't be reached from here without one advertising _meshcop."
    )
    commissionable = services["_matterc._udp.local."]
    findings.append(
        f"{len(commissionable)} commissionable device(s) visible."
        if commissionable
        else "No device has a commissioning window open (or its advert isn't "
        "reaching here). Start the share in the other app, then run this."
    )
    return _dump(
        {
            "listened_seconds": seconds,
            "ip_note": ip_note,
            "findings": findings,
            "network": network,
            "this_server_addresses": local,
            "known_matter_nodes": sorted(known_nodes),
            "services": services,
            "service_meaning": _MDNS_TYPES,
        },
        limit=120000,
    )


@tool
async def list_config_flows(handler: str = "") -> str:
    """Config flows in progress right now (the "Add integration"/discovery
    dialogs left open, or stuck): flow id, handler (integration domain), the
    step it is on and its source (user, zeroconf, ...). `handler` keeps one
    integration's. Aborted or finished flows aren't kept by Home Assistant, so
    an empty list doesn't mean a flow never failed; the reason it failed is in
    the log (get_error_log)."""
    flows = await ha_rest_get("/api/config/config_entries/flow")
    out = [
        {
            "flow_id": f.get("flow_id"),
            "handler": f.get("handler"),
            "step_id": f.get("step_id"),
            "source": (f.get("context") or {}).get("source"),
            "title": (f.get("context") or {}).get("title_placeholders"),
        }
        for f in flows
        if not handler or f.get("handler") == handler
    ]
    return _dump({"count": len(out), "flows": out})


if __name__ == "__main__":
    _start_event_listener()
    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=8000,
        streamable_http_path=MCP_PATH,
    )
