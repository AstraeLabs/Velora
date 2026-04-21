<div align="center">

# Velora

**A fast, plan-driven file downloader written in Rust.**

[![HTTP](https://img.shields.io/badge/HTTP-1%20%2F%202%20%2F%203-3182ce?style=for-the-badge&labelColor=2d3748)](#http-protocol-selection)
[![TLS](https://img.shields.io/badge/TLS-rustls-6b46c1?style=for-the-badge&labelColor=2d3748)](https://github.com/rustls/rustls)
[![Version](https://img.shields.io/badge/version-2.0.0-3182ce?style=for-the-badge&labelColor=2d3748)](./Cargo.toml)

</div>

---

## Overview

Velora is the download engine behind [VibraVid](https://github.com/AstraeLabs/VibraVid), shipped as a standalone binary via the [Binary](https://github.com/AstraeLabs/Binary) distribution. It consumes a single JSON **download plan** describing one or more URLs and writes files to disk concurrently, emitting a stream of newline-delimited JSON events on `stdout` so it can be driven by any host process.

- 🦀 **Rust, async, zero runtime surprises** — `tokio` + `reqwest` + `rustls`.
- ⚡ **Concurrent** — configurable worker pool with per-client connection reuse.
- 🔁 **Resilient** — exponential backoff with jitter and HTTP range retries.
- 🌐 **HTTP/1.1, HTTP/2, HTTP/3** — ALPN by default, QUIC behind a feature flag.

---

## Install

```bash
# From source
cargo build --release
./target/release/Velora --version

# With HTTP/3 (QUIC) support
cargo build --release --features http3
```

Requires a stable Rust toolchain (`rust-toolchain.toml` pins `stable`).

Prebuilt binaries for Windows / macOS / Linux (x64 · arm64) are distributed as part of [AstraeLabs/Binary](https://github.com/AstraeLabs/Binary).

---

## Usage

```bash
Velora <plan.json>
Velora --plan <plan.json>
Velora --version
```

Exit codes:

| Code | Meaning                               |
| ---- | ------------------------------------- |
| `0`  | All tasks completed successfully      |
| `1`  | Invalid CLI usage                     |
| `2`  | Plan load / validation / client error |
| `130`| Cancelled (Ctrl+C, SIGTERM, stop)     |

---

## Download plan

A plan is a JSON document describing the global download settings and the list of files to fetch.

```json
{
  "project": "VibraVid",
  "version": 1,
  "task_key": "ext_subtitle_ita_forced",
  "label": "Sub [vtt] it-IT [FORCED]",
  "display_label": "Sottotitoli italiani forzati",
  "output_dir": "C:/Users/Testing/Documents/Video/Trust/S01",
  "concurrency": 4,
  "retry_count": 8,
  "timeout_seconds": 30,
  "retry_base_delay_seconds": 1.0,
  "retry_max_delay_seconds": 4.0,
  "retry_jitter_seconds": 0.25,
  "proxy_url": "http://127.0.0.1:8080",
  "http_version": "auto",
  "headers": {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*"
  },
  "tasks": [
    {
      "task_key": "ext_subtitle_ita_forced",
      "label": "Sub [vtt] it-IT [FORCED]",
      "display_label": "Sottotitoli italiani forzati",
      "url": "https://example.com/subs-0000.vtt",
      "path": "C:/Users/Testing/Documents/Video/Trust/S01/ita_forced.vtt",
      "headers": {}
    }
  ]
}
```

### Fields

| Field                       | Type              | Default    | Description                                                   |
| --------------------------- | ----------------- | ---------- | ------------------------------------------------------------- |
| `project`                   | `string`          | `"Velora"` | Free-form project name, reflected in events.                  |
| `task_key`                  | `string`          | `"download"`| Default task key inherited by `tasks` when not set.          |
| `label` / `display_label`   | `string`          | —          | Human-readable labels propagated to events.                   |
| `output_dir`                | `string`          | —          | Base directory (created if missing).                          |
| `concurrency`               | `uint`            | `8`        | Maximum in-flight downloads.                                  |
| `retry_count`               | `uint`            | `3`        | Attempts per task before giving up.                           |
| `timeout_seconds`           | `uint`            | `30`       | Per-request timeout.                                          |
| `max_redirects`             | `uint`            | `10`       | Redirect follow limit.                                        |
| `retry_base_delay_seconds`  | `float`           | `1.0`      | Initial backoff delay.                                        |
| `retry_max_delay_seconds`   | `float`           | `30.0`     | Cap on backoff delay.                                         |
| `retry_jitter_seconds`      | `float`           | `0`        | Random jitter added to each retry delay.                      |
| `proxy_url`                 | `string`          | —          | HTTP / HTTPS / SOCKS proxy applied to all tasks.              |
| `headers`                   | `object`          | `{}`       | Headers merged into every request (task headers win).         |
| `user_agent`                | `string`          | `Velora/2` | Overrides the `User-Agent` header.                            |
| `max_speed_bytes`           | `uint`            | —          | Global rate-limit in bytes per second.                        |
| `http_version`              | `enum`            | `"auto"`   | See [HTTP protocol selection](#http-protocol-selection).      |
| `tasks`                     | `DownloadTask[]`  | `[]`       | Files to download (see below).                                |

Each `DownloadTask` supports `url`, `path`, optional `task_key` / `label` / `display_label`, and a per-task `headers` map that takes precedence over the plan-level headers.

Unknown fields are **rejected** (`deny_unknown_fields`).

### HTTP protocol selection

| `http_version` | Behaviour                                                              |
| -------------- | ---------------------------------------------------------------------- |
| `"auto"`       | ALPN negotiates HTTP/2 or HTTP/1.1 based on the server (default).      |
| `"http1"`      | Force HTTP/1.1.                                                        |
| `"http2"`      | Prior-knowledge HTTP/2 (explicit intent, h2c for plaintext URLs).      |
| `"http3"`      | QUIC / HTTP/3 — requires `cargo build --features http3`.               |

If `"http3"` is requested but the binary was not built with the feature, Velora emits a `warning` event and falls back to HTTP/2 via ALPN.

---

## Event stream (stdout)

Velora writes one JSON object per line to `stdout`. Host processes can consume it incrementally.

| Event        | When                                                 |
| ------------ | ---------------------------------------------------- |
| `version`    | Emitted on `--version`.                              |
| `start`      | Download of a task begins.                           |
| `retry`      | A task attempt failed and is being retried.          |
| `summary`    | Periodic progress snapshot with speed and ETA.       |
| `warning`    | Non-fatal issue (e.g. HTTP/3 fallback).              |
| `completed`  | All tasks in the plan finished.                      |
| `error`      | Fatal error (invalid plan, client build, IO).        |
| `cancelled`  | Cancellation was honoured and the process is exiting.|
