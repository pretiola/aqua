# 💧 Aqua

<img align="right" src="https://github.com/pretiola/aqua/actions/workflows/ci.yml/badge.svg">

Aqua transforms unstructured device telemetry into a structured Prometheus
and Grafana stack, with no per-device code.

Telemetry tooling typically fails in the same way: a metric name such as
`Air Temp`, `Filter Speed`, or `PumpRPM` is hardcoded against a specific
device, the firmware renames a label, and the dashboard breaks silently.
Aqua avoids this class of failure by treating every response as raw text.
The collector strips HTML, splits the response on configurable separators,
and infers each `label → value → unit` triple heuristically. Output that
does not match the heuristic — clocks, weekday names, build codes — is
discarded.

When the heuristic succeeds, a working dashboard is produced from a single
configuration block. When it fails, the separator list (not the code) is
adjusted.

<img width="777" height="822" alt="image" src="https://github.com/user-attachments/assets/87551776-db8f-4817-b22e-fe1479a7a14d" />

## 🏗️ Architecture

```
 ┌────────────────────┐    HTTP scrape    ┌──────────────┐
 │  device endpoint   │◀──────────────────│  collector   │  Rust
 │  (HTML / text)     │                   │  /metrics    │──┐
 └────────────────────┘                   │  /snapshot   │  │
                                          └──────────────┘  │
                                                            ▼
                                          ┌──────────────┐  ┌──────────────┐
                                          │  prometheus  │◀─│   grafana    │
                                          └──────────────┘  └──────────────┘
                                                  ▲
                                          ┌──────────────┐
                                          │     mcp      │  MCP Toolbox / HTTP
                                          │ (tools.yaml) │  tools for agents
                                          └──────────────┘
```

- **collector** (Rust). Polls every pipeline declared in `pipelines.toml`,
  parses each response, and exposes a single Prometheus gauge
  `aqua_metric{pipeline,name,unit}`. Additional endpoints: `/snapshot`
  (latest values as JSON) and `/healthz`.
- **prometheus**. Scrapes the collector. No additional configuration
  required.
- **grafana**. Provisioned with the Prometheus datasource and a dashboard
  that repeats one gauge per discovered metric, styled by the inferred
  unit (`fahrenheit`, `percent`, `psi`, `gpm`, `rpm`, `state`, …).
- **mcp** (Google MCP Toolbox). Optional. Runs the
  `us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox` container,
  configured by `mcp/tools.yaml` to expose the Prometheus HTTP API as MCP
  tools (raw PromQL, label/metric listings, target health).

## 🔍 Parser

All pipelines share the same parsing stages. None of the stages contain
device-specific logic.

1. HTML entities (`&nbsp;`, `&#176;`, `&deg;`, …) are decoded and tags are
   stripped.
2. The response is split on the pipeline's configured `separators`.
   Default: `["xxx", "\n", "\t"]`. Alternative configurations include
   `["|", "\n"]`, `[","]`, and so on, depending on the endpoint's format.
3. Each chunk is classified:
   - A chunk containing both a label and a number (e.g. `Air Temp 70°F`)
     is emitted directly.
   - A chunk consisting only of a label (e.g. `Filter Speed`) is retained
     as the candidate label for the next value-only chunk (e.g. `85% Speed1`
     → `Filter Speed = 85 (percent)`).
   - Chunks ending in a known state word produce a discrete value with
     unit `state`. The vocabulary covers `on/off`, `open/closed`,
     `enabled/disabled`, `running/stopped`, `ready/standby`, `auto`,
     `manual`, `remote`, and `fault/error/alarm`. Trailing context words
     are dropped (`Heater1 Auto Control` → `Heater1 = AUTO`). Mapping:
     fault `-1`, off `0`, on `1`, auto `2`, manual `3`, remote `4`,
     standby `5`. The Grafana dashboard converts these back to the
     displayed text via field-override value mappings.
   - Chunks matching clock patterns (`22:27`), weekday and month names, or
     unbroken alphanumeric strings (build codes, serial numbers) are
     discarded.
4. The first unit-like token following the number determines the `unit`
   label: `°F → fahrenheit`, `°C → celsius`, `% → percent`, `PSI → psi`,
   `GPM → gpm`, `RPM → rpm`, `V → volt`, `pH → ph`, etc. Unrecognized
   tokens yield `unit="none"`.

The discovery logic contains no domain-specific vocabulary.

## 🚀 Getting started

```bash
git clone git@github.com:pretiola/aqua.git && cd aqua
cp .env.example .env                          # host ports, log level
cp pipelines.toml.sample pipelines.toml       # declare endpoints (not in git)
$EDITOR pipelines.toml
docker compose up -d --build
```

Default endpoints:

- Grafana → http://localhost:3005 (admin / admin)
- Prometheus → http://localhost:9091 (override `PROM_PORT` in `.env` if
  the port is already bound on the host)
- Collector → http://localhost:9090/metrics, http://localhost:9090/snapshot
- MCP → http://localhost:5000/mcp (override `MCP_PORT` if 5000 is bound)

## 🔗 Adding a pipeline

Each `[[pipeline]]` block in `pipelines.toml` is independent. The
collector runs all pipelines concurrently; the pipeline `name` becomes a
`pipeline="..."` label on every emitted metric, enabling per-source
filtering in the Grafana dashboard.

```toml
[[pipeline]]
name          = "lcd-panel"
url           = "http://device.local/status.htm"
method        = "POST"
body          = "Update Local Server&"
content_type  = "text/plain;charset=UTF-8"
interval_secs = 5
separators    = ["xxx", "\n", "\t"]

[[pipeline]]
name          = "remote-station"
url           = "http://10.0.0.50/metrics.txt"
method        = "GET"
interval_secs = 10
separators    = ["|", "\n"]
```

See `pipelines.toml.sample` for the full schema with comments and
additional examples (CSV-style endpoints, per-pipeline label blocklists,
etc.).

Pipelines may be combined freely. POST and GET endpoints, fixed-width
LCD-style output, pipe-delimited dumps, and newline-delimited key/value
formats can coexist in a single configuration and share a single
dashboard.

## 🤖 Using the MCP server

The `mcp` service runs the upstream
[MCP Toolbox for Databases](https://github.com/googleapis/mcp-toolbox)
container (`us-central1-docker.pkg.dev/database-toolbox/toolbox/toolbox:latest`)
configured against the Aqua Prometheus as a generic HTTP source. The entire
MCP surface is declared in `mcp/tools.yaml`; no custom server code is
required. Any compliant MCP client may connect to `http://localhost:5005/mcp`
and invoke the following tools:

| Tool                   | Arguments                                  | Description                                                            |
| ---------------------- | ------------------------------------------ | ---------------------------------------------------------------------- |
| `execute_promql`       | `query` (string)                           | Instant PromQL query. Default tool for arbitrary metric inspection.    |
| `execute_promql_range` | `query`, `start`, `end`, `step` (strings)  | Range PromQL query producing a time series.                            |
| `list_metric_names`    | —                                          | Every metric name currently stored in Prometheus.                      |
| `list_labels`          | —                                          | Every label name across all stored series.                             |
| `list_pipelines`       | —                                          | Every configured Aqua pipeline name.                                   |
| `list_metrics`         | —                                          | Every metric name discovered by any pipeline.                          |
| `list_units`           | —                                          | Every inferred unit currently in use.                                  |
| `get_targets`          | —                                          | Health of every Prometheus scrape target.                              |

Adding or modifying tools requires only an edit to `mcp/tools.yaml` and a
restart of the `mcp` service; the binary reloads the configuration on
startup.

### Claude Code

```bash
claude mcp add --transport http aqua http://localhost:5005/mcp
```

### Cursor, Windsurf, and other clients using `mcp.json`

```json
{
  "mcpServers": {
    "aqua": {
      "url": "http://localhost:5005/mcp",
      "transport": "http"
    }
  }
}
```

### Manual verification

```bash
# Initialize a session.
curl -s -X POST http://localhost:5005/mcp \
  -H 'Accept: application/json, text/event-stream' \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"shell","version":"0"}}}'

# Invoke execute_promql against the collector's discovered metrics.
curl -s -X POST http://localhost:5005/mcp \
  -H 'Accept: application/json, text/event-stream' \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"execute_promql","arguments":{"query":"aqua_metric"}}}'
```

The MCP service contains no independent state and may be removed by
deleting the `mcp` service block in `docker-compose.yml` if MCP access is
not required.

## 🎛️ Tuning

Two configuration parameters typically resolve unexpected parser output:

- **`separators`** — the list of substrings used to partition each
  response into chunks. The default `["xxx", "\n", "\t"]` covers most
  fixed-width text dumps; CSV-style responses require `[",", "\n"]`.
- **`blocklist`** — case-insensitive substrings that disqualify a chunk
  from being treated as a label. Defaults cover weekday and month names,
  common timezone abbreviations, and AM/PM. Custom entries (for example
  `["sunrise", "sunset"]`) may be added per pipeline.

Both parameters are per-pipeline. Aqua maintains no global hardcoded
vocabulary.

## 📝 Implementation notes

- The collector is a single Rust binary. Unit tests are colocated in
  `main.rs` and execute under `cargo test`.
- Metrics are exposed as plain Prometheus text exposition. No client
  library, time-series writer, or schema migration is required.
- The MCP service is the upstream
  [`google/mcp-toolbox`](https://github.com/googleapis/mcp-toolbox) image,
  configured entirely through `mcp/tools.yaml`. No custom server code is
  shipped with Aqua.
