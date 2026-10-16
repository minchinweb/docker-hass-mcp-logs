# docker-hass-mcp-logs

An [MCP](https://modelcontextprotocol.io/) server that gives Claude read-only access to a
Home Assistant instance: the error log, Lovelace dashboards, and automation
configs, traces and history. It is served over streamable HTTP and packaged as a Docker image.

## Endpoint

The MCP endpoint is `MCP_PUBLIC_URL` + `/` + `MCP_PATH` (default
`http://localhost:8000/mcp`). Clients must send `Authorization: Bearer <MCP_AUTH_TOKEN>`.

## Tools

### Logs

| Tool | Arguments | Description |
| --- | --- | --- |
| `get_error_log` | `min_level="WARNING"`, `tail_lines=500`, `contains=""` | Home Assistant log, filtered by level and substring, last N lines. |

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
| `get_history` | `entity_id`, `hours=24` | State history of one entity: states and timestamps. |

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
docker build -t docker-hass-mcp-logs .
docker run --env-file .env -p 8000:8000 docker-hass-mcp-logs
```

Or without Docker: `pip install -r requirements.txt && python mcp_server.py`.
