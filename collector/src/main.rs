use anyhow::{anyhow, Context, Result};
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

// =====================================================================
// Configuration
// =====================================================================

#[derive(Debug, Deserialize, Clone)]
struct Config {
    #[serde(default = "default_listen")]
    listen: String,
    #[serde(default)]
    pipeline: Vec<Pipeline>,
}

fn default_listen() -> String {
    "0.0.0.0:9090".to_string()
}

#[derive(Debug, Deserialize, Clone)]
struct Pipeline {
    name: String,
    url: String,
    #[serde(default = "default_method")]
    method: String,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default = "default_interval")]
    interval_secs: u64,
    #[serde(default = "default_separators")]
    separators: Vec<String>,
    #[serde(default = "default_blocklist")]
    blocklist: Vec<String>,
}

fn default_method() -> String {
    "GET".to_string()
}
fn default_interval() -> u64 {
    10
}
fn default_separators() -> Vec<String> {
    vec!["xxx".into(), "\n".into(), "\t".into()]
}
fn default_blocklist() -> Vec<String> {
    [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
        "am",
        "pm",
        "est",
        "edt",
        "cst",
        "cdt",
        "mst",
        "mdt",
        "pst",
        "pdt",
        "utc",
        "gmt",
        "date",
        "time",
        "clock",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

// =====================================================================
// State
// =====================================================================

#[derive(Debug, Clone, Serialize)]
struct MetricSample {
    value: f64,
    unit: String,
    updated_unix: i64,
}

type PipelineMetrics = HashMap<String, MetricSample>;

#[derive(Default)]
struct AppState {
    metrics: RwLock<HashMap<String, PipelineMetrics>>,
}
type SharedState = Arc<AppState>;

// =====================================================================
// Parser
// =====================================================================

static RE_HTML_TAG: Lazy<Regex> = Lazy::new(|| Regex::new(r"<[^>]*>").unwrap());
static RE_ENTITY_NUM: Lazy<Regex> = Lazy::new(|| Regex::new(r"&#(\d+);?").unwrap());
static RE_ENTITY_NAMED: Lazy<Regex> = Lazy::new(|| Regex::new(r"&[a-zA-Z]+;?").unwrap());
static RE_NUMBER: Lazy<Regex> = Lazy::new(|| Regex::new(r"([-+]?\d+(?:\.\d+)?)").unwrap());
static RE_TIME: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^\d{1,2}:\d{2}(?::\d{2})?\s*([apAP][mM])?$").unwrap());
/// Map a single word to a numeric state value. Returns None if the word is
/// not a recognized state token. Vocabulary covers conventions seen across
/// industrial and HVAC telemetry: binary on/off, mode switches (auto,
/// manual, remote), lifecycle states (running, standby, idle), and fault
/// flags. Nothing here is domain-specific.
fn state_to_value(word: &str) -> Option<f64> {
    match word.to_lowercase().as_str() {
        "on" | "true" | "yes" | "open" | "enabled" | "active" | "running" | "ready" | "ok"
        | "high" => Some(1.0),
        "off" | "false" | "no" | "closed" | "disabled" | "inactive" | "stopped" | "stop"
        | "idle" | "low" => Some(0.0),
        "auto" | "automatic" => Some(2.0),
        "manual" | "hand" => Some(3.0),
        "remote" => Some(4.0),
        "standby" | "pause" | "paused" => Some(5.0),
        "fault" | "error" | "alarm" | "fail" | "failed" => Some(-1.0),
        _ => None,
    }
}

/// Tokenize the chunk and search from the right for the latest known state
/// word. Everything to its left becomes the label; anything after it is
/// dropped as trailing context. Returns None if no token is a state word.
fn try_state_parse(chunk: &str) -> Option<(Option<String>, f64, String)> {
    let words: Vec<&str> = chunk.split_whitespace().collect();
    for i in (0..words.len()).rev() {
        if let Some(v) = state_to_value(words[i]) {
            let label = words[..i].join(" ");
            return Some((
                if label.is_empty() { None } else { Some(label) },
                v,
                "state".to_string(),
            ));
        }
    }
    None
}

fn decode_entities(s: &str) -> String {
    let stage1 = RE_ENTITY_NUM
        .replace_all(s, |caps: &regex::Captures| {
            caps[1]
                .parse::<u32>()
                .ok()
                .and_then(char::from_u32)
                .map(|c| c.to_string())
                .unwrap_or_default()
        })
        .into_owned();

    let named = [
        ("&nbsp;", " "),
        ("&amp;", "&"),
        ("&lt;", "<"),
        ("&gt;", ">"),
        ("&quot;", "\""),
        ("&apos;", "'"),
        ("&deg;", "\u{00B0}"),
    ];
    let mut out = stage1;
    for (from, to) in named.iter() {
        out = out.replace(from, to);
    }
    RE_ENTITY_NAMED.replace_all(&out, " ").into_owned()
}

fn strip_html(s: &str) -> String {
    RE_HTML_TAG.replace_all(s, " ").into_owned()
}

fn collapse_whitespace(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_ws = true;
    for c in s.chars() {
        if c.is_whitespace() {
            if !prev_ws {
                out.push(' ');
            }
            prev_ws = true;
        } else {
            out.push(c);
            prev_ws = false;
        }
    }
    if out.ends_with(' ') {
        out.pop();
    }
    out
}

fn split_by_separators(text: &str, separators: &[String]) -> Vec<String> {
    let sentinel = "\u{001E}"; // ASCII record separator
    let mut acc = text.to_string();
    for sep in separators {
        if !sep.is_empty() {
            acc = acc.replace(sep.as_str(), sentinel);
        }
    }
    acc.split(sentinel).map(String::from).collect()
}

fn is_noise(chunk: &str) -> bool {
    // A single-token chunk (no spaces) with at least three consecutive
    // digits embedded among letters looks like a build code / serial number
    // (e.g. `EDUTDC333333`, `ABC9999XYZ`). A single trailing digit on a
    // name (e.g. `Heater1`, `Speed1`) is allowed and treated as a label.
    if chunk.contains(' ') {
        return false;
    }
    if chunk.chars().count() < 6 {
        return false;
    }
    if !chunk.chars().any(|c| c.is_alphabetic()) {
        return false;
    }
    let mut run = 0usize;
    for c in chunk.chars() {
        if c.is_ascii_digit() {
            run += 1;
            if run >= 3 {
                return true;
            }
        } else {
            run = 0;
        }
    }
    false
}

fn is_clean_label(chunk: &str) -> bool {
    // A label may carry a numeric suffix (e.g. `Heater1`, `Sensor2`) as
    // long as the majority of non-whitespace characters are alphabetic.
    let total: usize = chunk.chars().filter(|c| !c.is_whitespace()).count();
    if total == 0 {
        return false;
    }
    let alpha: usize = chunk.chars().filter(|c| c.is_alphabetic()).count();
    alpha * 2 >= total
}

fn is_blocklisted(label: &str, blocklist: &[String]) -> bool {
    let lower = label.to_lowercase();
    blocklist.iter().any(|b| {
        let bl = b.to_lowercase();
        !bl.is_empty() && lower.contains(&bl)
    })
}

fn normalize_unit(raw: &str) -> &'static str {
    let compact: String = raw
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .to_lowercase();
    match compact.as_str() {
        "" => "none",
        "%" => "percent",
        "\u{00B0}f" | "f" => "fahrenheit",
        "\u{00B0}c" | "c" => "celsius",
        "\u{00B0}" | "deg" => "degrees",
        "psi" | "psia" => "psi",
        "gpm" => "gpm",
        "rpm" => "rpm",
        "hz" => "hertz",
        "khz" => "kilohertz",
        "kw" => "kilowatt",
        "mw" => "megawatt",
        "w" => "watt",
        "v" | "volt" | "volts" => "volt",
        "a" | "amp" | "amps" | "ampere" | "amperes" => "ampere",
        "ma" => "milliampere",
        "mg/l" => "mg_per_l",
        "ppm" => "ppm",
        "ppb" => "ppb",
        "us/cm" | "\u{03BC}s/cm" | "\u{00B5}s/cm" => "us_per_cm",
        "ph" => "ph",
        "bar" => "bar",
        "kpa" => "kpa",
        "mpa" => "mpa",
        "pa" => "pascal",
        "in" | "inch" | "inches" => "inch",
        "ft" | "feet" => "foot",
        "m" | "meter" | "meters" => "meter",
        "cm" => "centimeter",
        "mm" => "millimeter",
        "l" | "liter" | "liters" => "liter",
        "gal" | "gallon" | "gallons" => "gallon",
        "s" | "sec" | "secs" | "seconds" => "second",
        "min" | "mins" | "minutes" => "minute",
        "h" | "hr" | "hrs" | "hour" | "hours" => "hour",
        _ => "none",
    }
}

fn extract_value(chunk: &str) -> Option<(Option<String>, f64, String)> {
    let trimmed = chunk.trim();

    // State vocabulary (on/off/auto/manual/fault/…). Tried first so chunks
    // like "Heater1 Auto Control" or "Valve Open" are classified as states
    // rather than scanned for numeric values.
    if let Some(state) = try_state_parse(trimmed) {
        return Some(state);
    }

    // Numeric value with adjacent unit.
    let cap = RE_NUMBER.captures(trimmed)?;
    let m = cap.get(1)?;
    // If the digit is glued to an alphabetic character (no whitespace
    // separator), the chunk is an identifier like `Heater1` or `Sensor3`,
    // not a label/value pair. Reject so the chunk can become a pending
    // label for the next chunk instead.
    if m.start() > 0 {
        if let Some(prev) = trimmed[..m.start()].chars().last() {
            if prev.is_alphabetic() {
                return None;
            }
        }
    }
    let value: f64 = m.as_str().parse().ok()?;
    let before = trimmed[..m.start()].trim().to_string();
    let after_full = &trimmed[m.end()..];
    let unit_raw: String = after_full
        .trim_start()
        .chars()
        .take_while(|c| !c.is_whitespace())
        .collect();
    let unit = normalize_unit(&unit_raw).to_string();

    Some((
        if before.is_empty() {
            None
        } else {
            Some(before)
        },
        value,
        unit,
    ))
}

fn parse(text: &str, separators: &[String], blocklist: &[String]) -> Vec<(String, MetricSample)> {
    let decoded = decode_entities(text);
    let stripped = strip_html(&decoded);
    let chunks = split_by_separators(&stripped, separators);

    let mut results: Vec<(String, MetricSample)> = Vec::new();
    let mut pending_label: Option<String> = None;
    let now = now_unix();

    for raw in chunks {
        let chunk = collapse_whitespace(raw.trim());
        if chunk.is_empty() {
            continue;
        }
        if is_noise(&chunk) {
            continue;
        }
        if RE_TIME.is_match(&chunk) {
            continue;
        }

        match extract_value(&chunk) {
            Some((Some(label), v, u)) => {
                if !is_blocklisted(&label, blocklist) {
                    results.push((
                        label,
                        MetricSample {
                            value: v,
                            unit: u,
                            updated_unix: now,
                        },
                    ));
                }
                pending_label = None;
            }
            Some((None, v, u)) => {
                if let Some(label) = pending_label.take() {
                    if !is_blocklisted(&label, blocklist) {
                        results.push((
                            label,
                            MetricSample {
                                value: v,
                                unit: u,
                                updated_unix: now,
                            },
                        ));
                    }
                }
            }
            None => {
                if is_clean_label(&chunk) && !is_blocklisted(&chunk, blocklist) {
                    pending_label = Some(chunk);
                } else {
                    pending_label = None;
                }
            }
        }
    }
    results
}

fn now_unix() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// =====================================================================
// Scraper
// =====================================================================

async fn run_pipeline(pipeline: Pipeline, state: SharedState) {
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            error!("[{}] failed to build HTTP client: {}", pipeline.name, e);
            return;
        }
    };

    let mut ticker = tokio::time::interval(Duration::from_secs(pipeline.interval_secs.max(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        match scrape_once(&client, &pipeline).await {
            Ok(body) => {
                let samples = parse(&body, &pipeline.separators, &pipeline.blocklist);
                if samples.is_empty() {
                    info!("[{}] poll returned 0 metrics", pipeline.name);
                    continue;
                }
                let mut store = state.metrics.write().await;
                let entry = store.entry(pipeline.name.clone()).or_default();
                for (name, sample) in samples {
                    info!(
                        "[{}] {} = {} ({})",
                        pipeline.name, name, sample.value, sample.unit
                    );
                    entry.insert(name, sample);
                }
            }
            Err(e) => warn!("[{}] scrape failed: {}", pipeline.name, e),
        }
    }
}

async fn scrape_once(client: &reqwest::Client, pipeline: &Pipeline) -> Result<String> {
    let method = pipeline.method.to_uppercase();
    let mut req = match method.as_str() {
        "POST" => client.post(&pipeline.url),
        _ => client.get(&pipeline.url),
    };
    if let Some(body) = &pipeline.body {
        req = req.body(body.clone());
    }
    if let Some(ct) = &pipeline.content_type {
        req = req.header("content-type", ct);
    }
    let resp = req.send().await.context("HTTP request failed")?;
    let status = resp.status();
    let text = resp.text().await.context("reading response body")?;
    if !status.is_success() {
        return Err(anyhow!("HTTP {}: {}", status, text));
    }
    Ok(text)
}

// =====================================================================
// HTTP server
// =====================================================================

fn quote_prom_label(s: &str) -> String {
    let escaped = s
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n");
    format!("\"{}\"", escaped)
}

async fn metrics_handler(State(state): State<SharedState>) -> Response {
    let store = state.metrics.read().await;
    let mut out = String::new();
    out.push_str(
        "# HELP aqua_metric Universal telemetry value discovered from a pipeline endpoint.\n",
    );
    out.push_str("# TYPE aqua_metric gauge\n");
    for (pipeline, metrics) in store.iter() {
        for (name, sample) in metrics.iter() {
            out.push_str(&format!(
                "aqua_metric{{pipeline={p},name={n},unit={u}}} {v}\n",
                p = quote_prom_label(pipeline),
                n = quote_prom_label(name),
                u = quote_prom_label(&sample.unit),
                v = sample.value,
            ));
        }
    }
    out.push_str(
        "# HELP aqua_metric_updated_seconds Unix timestamp when the metric was last observed.\n",
    );
    out.push_str("# TYPE aqua_metric_updated_seconds gauge\n");
    for (pipeline, metrics) in store.iter() {
        for (name, sample) in metrics.iter() {
            out.push_str(&format!(
                "aqua_metric_updated_seconds{{pipeline={p},name={n}}} {v}\n",
                p = quote_prom_label(pipeline),
                n = quote_prom_label(name),
                v = sample.updated_unix,
            ));
        }
    }
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        out,
    )
        .into_response()
}

async fn snapshot_handler(State(state): State<SharedState>) -> Response {
    let store = state.metrics.read().await;
    let body = serde_json::to_string(&*store).unwrap_or_else(|_| "{}".to_string());
    (StatusCode::OK, [("content-type", "application/json")], body).into_response()
}

async fn health_handler() -> &'static str {
    "ok"
}

// =====================================================================
// Main
// =====================================================================

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let config_path =
        std::env::var("AQUA_CONFIG").unwrap_or_else(|_| "/config/pipelines.toml".to_string());
    let raw = std::fs::read_to_string(&config_path)
        .with_context(|| format!("reading config from {}", config_path))?;
    let config: Config = toml::from_str(&raw).context("parsing pipelines.toml")?;

    if config.pipeline.is_empty() {
        return Err(anyhow!(
            "no [[pipeline]] entries in {} — define at least one",
            config_path
        ));
    }
    info!(
        "Loaded {} pipeline(s) from {}",
        config.pipeline.len(),
        config_path
    );

    let state: SharedState = Arc::new(AppState::default());

    for pipeline in &config.pipeline {
        info!(
            "[{}] -> {} every {}s (method={}, separators={:?})",
            pipeline.name,
            pipeline.url,
            pipeline.interval_secs,
            pipeline.method,
            pipeline.separators
        );
        let p = pipeline.clone();
        let s = state.clone();
        tokio::spawn(async move { run_pipeline(p, s).await });
    }

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/snapshot", get(snapshot_handler))
        .route("/healthz", get(health_handler))
        .with_state(state);

    let addr: SocketAddr = config.listen.parse().context("parsing listen addr")?;
    info!("HTTP server listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

// =====================================================================
// Tests
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn seps() -> Vec<String> {
        default_separators()
    }
    fn bl() -> Vec<String> {
        default_blocklist()
    }

    #[test]
    fn label_value_in_one_chunk() {
        let html = "<body>  Air Temp  70&#176;F   xxx\n&nbsp;xxx\nABC123DEF456xxx\n</body>";
        let r = parse(html, &seps(), &bl());
        let found = r.iter().find(|(n, _)| n == "Air Temp").expect("Air Temp");
        assert_eq!(found.1.value, 70.0);
        assert_eq!(found.1.unit, "fahrenheit");
    }

    #[test]
    fn label_then_value_across_chunks() {
        let html =
            "<body>    Filter Speed    xxx\n   85% Speed1       xxx\nEDUTDC333333xxx\n\n</body>";
        let r = parse(html, &seps(), &bl());
        let found = r
            .iter()
            .find(|(n, _)| n == "Filter Speed")
            .expect("Filter Speed");
        assert_eq!(found.1.value, 85.0);
        assert_eq!(found.1.unit, "percent");
    }

    #[test]
    fn label_with_digit_suffix_pairs_with_next_chunk() {
        // Real-device screen: "Heater1 xxx Auto Control xxx EDTDDS333333xxx".
        // `Heater1` must survive the noise filter, must NOT be parsed as
        // `Heater = 1`, and must become the label for the following
        // "Auto Control" state chunk.
        let html = "<body>Heater1 xxx Auto Control xxx EDTDDS333333xxx\n</body>";
        let r = parse(html, &seps(), &bl());
        let found = r.iter().find(|(n, _)| n == "Heater1").expect("Heater1");
        assert_eq!(found.1.value, 2.0); // auto
        assert_eq!(found.1.unit, "state");
        assert_eq!(r.len(), 1, "no other metrics expected: {:?}", r);
    }

    #[test]
    fn multi_state_vocabulary() {
        let html = "<body>Heater1 Auto Control xxx\nHeater1 OFF xxx\nValve Open xxx\nMode Manual Override xxx\nPump Fault xxx\n</body>";
        let r = parse(html, &seps(), &bl());

        // "Heater1 Auto Control" -> 2.0 (auto). "Heater1 OFF" -> 0.0 (off).
        // Both are emitted; map insertion would keep the last one, but parse()
        // returns the full ordered list.
        let heater_values: Vec<f64> = r
            .iter()
            .filter(|(n, _)| n == "Heater1")
            .map(|(_, s)| s.value)
            .collect();
        assert!(heater_values.contains(&2.0), "expected auto: {:?}", r);
        assert!(heater_values.contains(&0.0), "expected off: {:?}", r);

        let valve = r.iter().find(|(n, _)| n == "Valve").expect("valve");
        assert_eq!(valve.1.value, 1.0);
        assert_eq!(valve.1.unit, "state");

        let mode = r.iter().find(|(n, _)| n == "Mode").expect("mode");
        assert_eq!(mode.1.value, 3.0); // manual

        let pump = r.iter().find(|(n, _)| n == "Pump").expect("pump");
        assert_eq!(pump.1.value, -1.0); // fault
    }

    #[test]
    fn clock_screen_is_ignored() {
        let html =
            "<body>      Saturday      xxx\n       22:27        xxx\nEDUTDC333333xxx\n</body>";
        let r = parse(html, &seps(), &bl());
        assert!(r.is_empty(), "expected no metrics, got: {:?}", r);
    }

    #[test]
    fn on_off_states_become_binary() {
        let html = "<body>Booster Pump On xxx\n Heater Off xxx\n</body>";
        let r = parse(html, &seps(), &bl());
        let booster = r
            .iter()
            .find(|(n, _)| n == "Booster Pump")
            .expect("booster");
        let heater = r.iter().find(|(n, _)| n == "Heater").expect("heater");
        assert_eq!(booster.1.value, 1.0);
        assert_eq!(heater.1.value, 0.0);
    }

    #[test]
    fn build_codes_rejected_as_noise() {
        let html = "<body>EDUTDC333333xxx\nABC9999XYZxxx\n</body>";
        let r = parse(html, &seps(), &bl());
        assert!(r.is_empty(), "expected noise rejection, got: {:?}", r);
    }

    #[test]
    fn custom_separators() {
        // Pipe-delimited input
        let text = "Pressure|45 PSI|Flow|12.5 GPM|";
        let r = parse(text, &["|".to_string()], &bl());
        let pressure = r.iter().find(|(n, _)| n == "Pressure").expect("pressure");
        let flow = r.iter().find(|(n, _)| n == "Flow").expect("flow");
        assert_eq!(pressure.1.value, 45.0);
        assert_eq!(pressure.1.unit, "psi");
        assert_eq!(flow.1.value, 12.5);
        assert_eq!(flow.1.unit, "gpm");
    }

    #[test]
    fn negative_and_decimal_values() {
        let html = "<body>Outside Temp -12.5&#176;C xxx</body>";
        let r = parse(html, &seps(), &bl());
        let t = r.iter().find(|(n, _)| n == "Outside Temp").expect("temp");
        assert_eq!(t.1.value, -12.5);
        assert_eq!(t.1.unit, "celsius");
    }
}
