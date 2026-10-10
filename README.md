# Velora

A small command-line downloader written in Rust. It has no CLI options for URLs: it reads a `plan.json` file describing what to download and prints progress to stdout as JSON lines. It is meant to be launched by another program.

## Usage

```
Velora plan.json
Velora --plan plan.json
Velora --version
```

## How it works

1. Reads `plan.json` and validates it (`http`/`https` URLs, paths without `..`).
2. Downloads files in parallel (`concurrency`), each into a `.part` file that is renamed once the download completes.
3. If a download is interrupted it retries (`retry_count`) and resumes where it left off (`Range` header).
4. If the file already exists with the same remote size, it is skipped.
5. Every event (`start`, `progress`, `completed`, `retry`, `error`, `summary`) is printed to stdout as one JSON line.
6. It stops on Ctrl+C, SIGTERM, or when `{"event":"stop"}` is written to stdin.

## Example plan.json

```json
{
  "concurrency": 4,
  "retry_count": 3,
  "timeout_seconds": 30,
  "headers": { "Referer": "https://example.com" },
  "tasks": [
    { "url": "https://example.com/a.bin", "path": "out/a.bin" },
    { "url": "https://example.com/b.bin", "path": "out/b.bin" }
  ]
}
```

Unknown fields are rejected, so a typo in a key makes the plan fail to load.

## Plan reference

Top-level fields (all optional except `tasks`):

| Field | Default | Description |
|---|---|---|
| `project` | `"Velora"` | Name reported in events. |
| `version` | `1` | Plan format version. |
| `task_key` | `"download"` | Identifier reported in events. |
| `label`, `display_label` | project name | Labels reported in events. |
| `concurrency` | `8` | Parallel downloads (1–256). |
| `retry_count` | `3` | Attempts per file (1–100). |
| `timeout_seconds` | `30` | Connect timeout and idle timeout between chunks (1–3600). |
| `max_redirects` | `10` | Max redirects followed (1–50). |
| `retry_base_delay_seconds` | `1.0` | First retry delay, doubled each attempt. |
| `retry_max_delay_seconds` | `30.0` | Cap for the retry delay. |
| `retry_jitter_seconds` | `0` | Random extra delay added to each retry. |
| `segment_delay_seconds` | `0` | Pause before each file starts. |
| `segment_delay_jitter_seconds` | `0` | Random extra pause before each file. |
| `max_speed_bytes_per_sec` | `0` | Global speed limit, `0` = unlimited. |
| `proxy_url` | none | `http`, `https`, `socks5` or `socks5h` proxy. |
| `verify_tls` | `true` | Set to `false` to skip certificate checks (a warning event is printed). |
| `http_version` | `"1.1"` | `"1.1"` forces HTTP/1.1; `"2"`/`"3"` let the client negotiate. |
| `headers` | `{}` | Headers sent with every request. |
| `user_agent` | `Velora/2` | Used unless `headers` already sets `User-Agent`. |
| `tasks` | required | List of downloads (see below). |

Each task:

| Field | Description |
|---|---|
| `url` | Required. Must be `http` or `https`. |
| `path` | Required. Output file; must not contain `..`. Parent folders are created. |
| `headers` | Extra headers for this file only (can override the global ones). |
| `task_key`, `label`, `display_label` | Override the top-level values in events. |

All delay and timeout values are clamped to 0–3600 s; non-finite numbers become `0`. Requests are sent with `Accept-Encoding: identity` unless you set it yourself, and a task `Range` header disables resuming.

## Command line

| Flag | Meaning |
|---|---|
| `<plan.json>` | Run the plan (single positional argument). |
| `--plan`, `-p`, `--input <file>` | Same, explicit form. |
| `--version`, `-V` | Print a `version` event and exit. |

Velora does not read any environment variables at runtime.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Finished (check the `summary` event for failed files). |
| `1` | Wrong command-line usage. |
| `2` | Plan could not be read, parsed or validated, or the HTTP client could not be built. |
| `130` | Cancelled (Ctrl+C, SIGTERM, `stop` on stdin, or stdout closed). |

## Events

One JSON object per line on stdout, always with an `event` field:

- `start`: run began (task count, concurrency, TLS and speed settings).
- `progress`: about every 300 ms with `total_bytes` and `speed`.
- `completed`: a file finished or was skipped (`path`, `url`, `bytes`, `skipped`, `pct`, `segments`).
- `retry`: an attempt failed and will be retried (`attempt`, `retry_delay_seconds`, `message`).
- `error`: a file failed for good, or the plan was invalid.
- `warning`: a header was dropped, or TLS verification is off.
- `summary`: totals (`completed`, `failed`, `skipped`, `bytes`, `average_speed`).
- `cancelled`, `version`: emitted on cancellation and `--version`.

## Build

```
cargo build --release
```

The executable is `target/release/Velora`.

Build requirements (the TLS stack is BoringSSL, built from source):

- A C/C++ toolchain, CMake, and **libclang**. If the build fails with "Unable to find libclang", install LLVM and set `LIBCLANG_PATH` to the folder containing `libclang.dll` / `libclang.so`.
- On Windows also NASM and Visual Studio Build Tools.
- Use `cargo build --locked` to build exactly the dependency versions in `Cargo.lock`.
