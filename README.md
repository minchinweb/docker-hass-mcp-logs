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
| `get_logbook` | `entity_id=""`, `hours=48` | Logbook entries (automation triggers, state changes), optionally for one entity. Shows whether an automation triggered at all. |
| `get_history` | `entity_id`, `hours=24`, `include_attributes=False` | State history of one entity: states and timestamps, with attributes if asked. |

### States, areas and integrations

| Tool | Arguments | Description |
| --- | --- | --- |
| `get_state` | `entity_id=""`, `contains=""` | Current state and attributes of one entity, or of every entity whose ID or name contains the text. |
| `list_areas` | none | Floors (with level) and areas (ID, name, floor, aliases). |
| `list_integration_entities` | `domain`, `include_attributes=True` | Every entity an integration provides (e.g. `house_structure`), with its device, area, floor, state and attributes. Finds renamed entities too. |
| `get_config_entries` | `domain` | An integration's config entries and their subentries (ID, type, title). Home Assistant's API doesn't give the saved settings, so they come from the integration's diagnostics where it has them. |
| `list_integrations` | `contains=""`, `custom_only=False` | Every installed integration with `activated` (loaded or not), its config entry states, and its version: the integration's own version for custom ones (e.g. HACS), the HA core version for core ones. `manifest_error` is set if HA refuses the manifest list. |
| `get_recent_events` | `contains=""`, `event_type=""`, `minutes=720` | Events seen live since the server started: state changes of the entities matching `EVENT_BUFFER_ENTITIES`, and `automation_triggered`. |

### Setting up integrations

| Tool | Arguments | Description |
| --- | --- | --- |
| `diagnose_integration` | `domain`, `log_lines=30` | Why an integration can't be set up or isn't working, in one call: `findings` in plain words, then whether Home Assistant knows it (its manifest), whether **Add integration** offers it (`offers_setup_form`), its config entries, whether its dependencies are loaded, HACS's record of it, its repairs (and HACS's restart repairs) and the warnings and errors that mention it. Tells apart "not downloaded", "downloaded but Home Assistant not restarted", "failed to load" and "a dependency isn't loaded". |
| `list_repairs` | `domain=""`, `contains=""` | Home Assistant's repairs (Settings > Repairs): domain, issue ID, severity, translation key and placeholders, fixable, ignored. HACS raises one when a download needs a restart. |
| `list_hacs_repositories` | `contains=""`, `installed_only=True` | What HACS has downloaded: GitHub name, category, the integration's domain, installed and newest versions, and HACS's `status` (e.g. pending a restart). A download from a branch has a commit hash for its version. `error` is set if HACS isn't installed. |

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

## Running

```sh
docker build --build-arg COMMIT=$(git rev-parse --short HEAD) \
  --build-arg BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ) -t docker-hass-mcp-logs .
docker run --env-file .env -p 8000:8000 docker-hass-mcp-logs
```

Or without Docker: `pip install -r requirements.txt && python mcp_server.py`.
