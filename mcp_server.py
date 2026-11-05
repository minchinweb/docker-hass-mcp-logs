import asyncio
import functools
import hmac
import json
import os
import re
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

# Bump on every change to the tool set. Build metadata comes from the image
# (see Dockerfile) and falls back to "unknown" for non-Docker runs.
VERSION = "1.9.0"
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
async def get_logbook(entity_id: str = "", hours: float = 48) -> str:
    """Logbook entries for the last N hours, optionally for one entity. Shows
    automation triggers and state changes, so it can tell 'never triggered'
    from 'triggered but failed'."""
    params = {"entity": entity_id} if entity_id else None
    return _dump(await ha_rest_get(f"/api/logbook/{_since(hours)}", params))


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
    domain: str, include_attributes: bool = True
) -> str:
    """Every entity an integration provides (its entity registry `platform`,
    e.g. `house_structure`), with its device, area (the entity's own, else its
    device's), floor, current state and, by default, attributes. Finds them
    even when renamed, unlike get_state's name search."""
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
        state = states.get(e["entity_id"]) or {}
        item = {
            "entity_id": e["entity_id"],
            "unique_id": e.get("unique_id"),
            "disabled_by": e.get("disabled_by"),
            "device": device.get("name_by_user") or device.get("name"),
            "device_model": device.get("model"),
            "area_id": area.get("area_id"),
            "area": area.get("name"),
            "floor": area.get("floor"),
            "state": state.get("state"),
        }
        if include_attributes:
            item["attributes"] = state.get("attributes")
        out.append(item)
    return _dump(sorted(out, key=lambda x: x["entity_id"]))


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


if __name__ == "__main__":
    _start_event_listener()
    mcp.run(
        transport="streamable-http",
        host="0.0.0.0",
        port=8000,
        streamable_http_path=MCP_PATH,
    )
