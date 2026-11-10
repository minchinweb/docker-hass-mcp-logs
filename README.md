# docker-hass-mcp-logs

An [MCP](https://modelcontextprotocol.io/) server that gives Claude read-only access to a
Home Assistant instance: the error log, Lovelace dashboards, and automation
configs, traces and history. It is served over streamable HTTP and packaged as a Docker image.

## Endpoint

The MCP endpoint is `MCP_PUBLIC_URL` + `/` + `MCP_PATH` (default
`http://localhost:8000/mcp`). Clients must send `Authorization: Bearer <MCP_AUTH_TOKEN>`.

`GET /version` (no auth) returns `{"name", "version", "build_commit", "build_date"}` as
JSON, e.g. `curl http://localhost:8000/version`.

## Tools

### Server info

| Tool | Arguments | Description |
| --- | --- | --- |
| `get_server_info` | none | Server version, build commit and date, start time, and the list of registered tools. Use it to confirm a redeploy took effect. |

### Logs

| Tool | Arguments | Description |
| --- | --- | --- |
| `get_error_log` | `min_level="WARNING"`, `tail_lines=500`, `contains=""` | Home Assistant log, one line per entry, filtered by level and substring (`contains` also searches tracebacks), last N entries. Entries with a traceback or continuation lines are marked `[+N lines]`; read them with `get_log_entry`. |
| `get_log_entry` | `at`, `contains=""`, `max_lines=200` | Full text of log entries including traceback lines. `at` is the start of the timestamp shown by `get_error_log` (e.g. `2026-11-01 17:30:36.961`, or just to the second). |

### Dashboards

| Tool | Arguments | Description |
| --- | --- | --- |
| `list_dashboards` | none | Lovelace dashboards (`url_path`, title, mode). The default dashboard is not listed. |
| `get_dashboard_config` | `url_path=None` | Full Lovelace config for a dashboard. Omit `url_path` for the default one. |
| `list_lovelace_resources` | none | Custom card/module resources (e.g. HACS cards) registered with Lovelace. |

### Automations

| Tool | Arguments | Description |
| --- | --- | --- |
| `list_automations` | `contains=""` | Automations with state, `last_triggered`, mode and the config `id` used by the tools below. `contains` filters on entity ID or name. |
| `get_automation_config` | `automation_id` | Triggers, conditions and actions of an automation. |
| `list_traces` | `automation_id` | Recent runs (HA keeps about 5 by default) with `run_id`, timestamps, trigger and any error. An empty list means no recent runs. |
| `get_trace` | `automation_id`, `run_id` | Step-by-step trace of one run: trigger, conditions, and each action's result or error. |

`automation_id` is the `id` returned by `list_automations`. Automations defined in YAML
without an `id` can't be read by `get_automation_config`, `list_traces` or `get_trace`.

### Logbook and history

| Tool | Arguments | Description |
| --- | --- | --- |
| `get_logbook` | `entity_id=""`, `hours=48`, `contains=""`, `exclude=""`, `limit=0` | Logbook entries (automation triggers, state changes), optionally for one entity. Shows whether an automation triggered at all. `contains` keeps matching entries; `exclude` drops entries matching any of its comma-separated texts (e.g. `last_seen`); `limit` keeps the newest N. Replies with `matched` (before `limit`), `count` and `entries`. |
| `get_history` | `entity_id`, `hours=24`, `include_attributes=False` | State history of one entity: states and timestamps, with attributes if asked. |

### States, areas and integrations

| Tool | Arguments | Description |
| --- | --- | --- |
| `get_state` | `entity_id=""`, `contains=""` | Current state and attributes of one entity, or of every entity whose ID or name contains the text. |
| `list_areas` | none | Floors (with level) and areas (ID, name, floor, aliases). |
| `list_integration_entities` | `domain`, `include_attributes=True`, `state=""`, `contains=""`, `limit=0` | Every entity an integration provides (e.g. `house_structure`), with its device, area, floor, state and attributes. Finds renamed entities too. `state` (e.g. `unavailable`), `contains` (entity ID, device or area) and `limit` narrow it for big integrations. Replies with `total`, `state_counts` (whole integration), `matched`, `count` and `entities`. |
| `get_config_entries` | `domain` | An integration's config entries and their subentries (ID, type, title). Home Assistant's API doesn't give the saved settings, so they come from the integration's diagnostics where it has them. |
| `get_house_structure` | `include_sheet=True`, `detail="full"` | Current House Structure layout: house settings, each part's settings and built attributes, areas with neighbours, and by default the parts CSV sheet. Prefer this when reviewing room layout. `detail="sheet"` returns settings + CSV only. Falls back to diagnostics if the integration is older than the layout websocket. |
| `list_integrations` | `contains=""`, `custom_only=False` | Every installed integration with `activated` (loaded or not), its config entry states, and its version: the integration's own version for custom ones (e.g. HACS), the HA core version for core ones. `manifest_error` is set if HA refuses the manifest list. |
| `get_recent_events` | `contains=""`, `event_type=""`, `minutes=720` | Events seen live since the server started: state changes of the entities matching `EVENT_BUFFER_ENTITIES`, and `automation_triggered`. |

### Setting up integrations

| Tool | Arguments | Description |
| --- | --- | --- |
| `diagnose_integration` | `domain`, `log_lines=30` | Why an integration can't be set up or isn't working, in one call: `findings` in plain words, then whether Home Assistant knows it (its manifest), whether **Add integration** offers it (`offers_setup_form`), its config entries, whether its dependencies are loaded, the Marketplace's record of it (`store`, `store_record`; HACS before Home Assistant 2026.11), its repairs (and the Marketplace's restart repairs) and the warnings and errors that mention it. Tells apart "not installed" (and whether there's a Marketplace to install it from), "installed but Home Assistant not restarted", "failed to load" and "a dependency isn't loaded". |
| `list_repairs` | `domain=""`, `contains=""` | Home Assistant's repairs (Settings > Repairs): domain, issue ID, severity, translation key and placeholders, fixable, ignored. The Marketplace raises one when an install or update needs a restart. |
| `list_marketplace_repositories` | `contains=""`, `installed_only=True` | What the Marketplace (Settings > Marketplace, Home Assistant 2026.11 on) has installed, or HACS on older versions: GitHub name, category, the integration's domain, whether it's a custom repository, installed and newest versions, and `status` (e.g. pending-restart). A branch install has a commit hash for its version. `store` says which answered; `error` is set if neither did. |

### Matter

| Tool | Arguments | Description |
| --- | --- | --- |
| `get_matter_nodes` | `ping=True`, `include_topology=False` | Live state of every Matter node, asked of the Matter server through Home Assistant: `available`, network type, IP and MAC addresses, active fabrics, the devices behind it (a bridge carries many) and, with `ping`, whether the server can ping each address right now. `findings` says what that adds up to (no address: discovery problem; no ping: unreachable; pings but unavailable: session problem). `include_topology` adds the Thread/Wi-Fi topology graph. Far smaller than `get_config_entries("matter")`. |
| `get_matter_server_log` | `tail_lines=200`, `contains=""`, `since_minutes=0`, `fetch_lines=5000` | Log of the Matter server's Docker container (`MATTER_CONTAINER`): mDNS discovery, CASE/PASE sessions, commissioning errors, which never reach Home Assistant's log. Timestamps are Docker's (UTC). Also returns the container's status, start time, restart count, network mode, addresses and `TZ`. Needs the Docker socket (see Configuration). |
| `test_mdns` | `seconds=6`, `extra_types=""` | Listens for mDNS from this server's network and reports what answers: commissioned Matter nodes (`_matter._tcp`, matched to Home Assistant's node IDs and marked `ours` or another ecosystem's fabric), devices with a commissioning window open (`_matterc._udp`, with vendor; a device being shared to Home Assistant only appears while the other app has it open, so run it during the share) and Thread border routers (`_meshcop._udp`). Also lists this container's own addresses and compares its Docker network with `MATTER_CONTAINER`'s (needs the Docker socket): the results only show what the Matter server sees if `network.same_view` is true, and a Docker bridge network hears no LAN multicast at all. `findings` sums it up. |
| `list_config_flows` | `handler=""` | Config flows in progress now (open or stuck "Add integration"/discovery dialogs): flow ID, handler, step and source. Home Assistant doesn't keep finished or aborted flows, so look in the log for why one failed. |

Large results are truncated at about 60,000 characters.

## Configuration

Copy `.env.example` to `.env` and fill it in.

| Variable | Description |
| --- | --- |
| `HA_URL` | Base URL of Home Assistant, no trailing slash. |
| `HA_TOKEN` | Long-lived access token from your HA user profile. |
| `MCP_PUBLIC_URL` | Public base URL of this server (used for OAuth metadata). |
| `MCP_PATH` | Path the MCP endpoint is served on, e.g. `mcp`. |
| `MCP_AUTH_TOKEN` | Bearer token clients must send. |
| `MATTER_CONTAINER` | Optional. Name of the Matter server's Docker container, read by `get_matter_server_log`. Without it that tool says it isn't configured; the others work regardless. |
| `DOCKER_SOCKET` | Optional. Path of the Docker socket inside this container. Default `/var/run/docker.sock`. |

To read the Matter server's log, also mount the Docker socket:

```sh
docker run --env-file .env -e MATTER_CONTAINER=matter-server \
  -v /var/run/docker.sock:/var/run/docker.sock:ro -p 8000:8000 docker-hass-mcp-logs
```

`test_mdns` hears only what its own container's network hears. mDNS does not cross a
Docker bridge, so to test the LAN run this server with `--network host`, or to see exactly
what the Matter server sees give it `--network container:<matter container>`. (With host
networking `-p 8000:8000` is ignored: the server listens on host port 8000 directly, and
`HA_URL` must be reachable from the host.)

The `:ro` on the socket does not make Docker's API read-only, and access to the socket
is effectively root on the host. The server only ever issues two `GET` requests (inspect
and logs) and only for the container named in `MATTER_CONTAINER`, which comes from the
environment, never from a tool argument. If the log reads "permission denied", the
process in this container needs access to the socket (run as root, or add the socket's
group).

## Running

```sh
docker build --build-arg COMMIT=$(git rev-parse --short HEAD) \
  --build-arg BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ) -t docker-hass-mcp-logs .
docker run --env-file .env -p 8000:8000 docker-hass-mcp-logs
```

Or without Docker: `pip install -r requirements.txt && python mcp_server.py`.
