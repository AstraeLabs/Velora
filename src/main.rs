mod models;
mod runner;
mod speed;

use std::io::Write;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::json;
use tokio::io::{AsyncBufReadExt, BufReader};

use models::DownloadPlan;
use runner::{build_client, is_stdout_pipe_closed, ClientKey, DownloadRunner};

#[derive(Debug)]
enum CliAction {
    RunPlan(String),
    ShowVersion,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------
#[tokio::main]
async fn main() {
    let action = match resolve_cli_action() {
        Some(action) => action,
        None => {
            eprintln!("Usage: Velora <plan.json>");
            eprintln!("   or: Velora --plan <plan.json>");
            eprintln!("   or: Velora --version");
            process::exit(1);
        }
    };

    if matches!(action, CliAction::ShowVersion) {
        let _ = write_event_stdout(&json!({
            "event": "version",
            "name": "Velora",
            "version": env!("CARGO_PKG_VERSION"),
        }));
        process::exit(0);
    }

    let CliAction::RunPlan(plan_path) = action else {
        process::exit(0);
    };

    let mut plan = match load_plan(&plan_path) {
        Ok(p) => p,
        Err(e) => {
            let _ = write_event_stdout(&json!({
                "event": "error",
                "message": e.to_string(),
                "plan_path": plan_path,
            }));
            process::exit(2);
        }
    };

    if let Err(e) = validate_plan(&plan) {
        let _ = write_event_stdout(&json!({
            "event": "error",
            "message": e.to_string(),
            "plan_path": plan_path,
        }));
        process::exit(2);
    }

    plan.normalise();

    let cancel_requested = Arc::new(AtomicBool::new(false));
    install_signal_handlers(cancel_requested.clone());
    install_stdin_stop_handler(cancel_requested.clone());

    let key = ClientKey::from_plan(&plan);
    let client = match build_client(&key, plan.concurrency.max(1)) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            let _ = write_event_stdout(&json!({
                "event": "error",
                "message": format!("Failed to build HTTP client: {e}"),
            }));
            process::exit(2);
        }
    };

    if !cancel_requested.load(Ordering::Relaxed) && !is_stdout_pipe_closed() {
        let runner = DownloadRunner::new(plan, client, cancel_requested.clone());
        runner.run().await;
    }

    if cancel_requested.load(Ordering::Relaxed) || is_stdout_pipe_closed() {
        let _ = write_event_stdout(&json!({
            "event": "cancelled",
            "message": "Cancellation requested",
        }));
        process::exit(130);
    }

    process::exit(0);
}

fn install_signal_handlers(cancel_requested: Arc<AtomicBool>) {
    let ctrlc_flag = cancel_requested.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            ctrlc_flag.store(true, Ordering::SeqCst);
        }
    });

    #[cfg(unix)]
    {
        let term_flag = cancel_requested.clone();
        tokio::spawn(async move {
            use tokio::signal::unix::{signal, SignalKind};

            if let Ok(mut stream) = signal(SignalKind::terminate()) {
                if stream.recv().await.is_some() {
                    term_flag.store(true, Ordering::SeqCst);
                }
            }
        });
    }
}

fn install_stdin_stop_handler(cancel_requested: Arc<AtomicBool>) {
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin).lines();

        while let Ok(Some(line)) = reader.next_line().await {
            if line.trim().is_empty() {
                continue;
            }

            if let Ok(payload) = serde_json::from_str::<serde_json::Value>(&line) {
                if payload
                    .get("event")
                    .and_then(|v| v.as_str())
                    .map(|v| v.eq_ignore_ascii_case("stop"))
                    .unwrap_or(false)
                {
                    cancel_requested.store(true, Ordering::SeqCst);
                    break;
                }
            }
        }
    });
}

// ---------------------------------------------------------------------------
// CLI argument parsing
// ---------------------------------------------------------------------------
fn resolve_cli_action() -> Option<CliAction> {
    let args: Vec<String> = std::env::args().skip(1).collect();

    if args.iter().any(|a| a == "--version" || a == "-V") {
        return Some(CliAction::ShowVersion);
    }

    if args.len() == 1 && !args[0].starts_with('-') {
        return Some(CliAction::RunPlan(canonicalize(&args[0])));
    }

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--plan" | "-p" | "--input" if i + 1 < args.len() => {
                return Some(CliAction::RunPlan(canonicalize(&args[i + 1])));
            }
            _ => {}
        }
        i += 1;
    }

    None
}

fn canonicalize(path: &str) -> String {
    std::fs::canonicalize(path)
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| path.to_owned())
}

// ---------------------------------------------------------------------------
// Plan loading and validation
// ---------------------------------------------------------------------------
fn load_plan(path: &str) -> anyhow::Result<DownloadPlan> {
    let json = std::fs::read_to_string(path)?;
    serde_json::from_str::<DownloadPlan>(&json)
        .map_err(|e| anyhow::anyhow!("Unable to parse download plan {path}: {e}"))
}

fn validate_plan(plan: &DownloadPlan) -> anyhow::Result<()> {
    if plan.tasks.is_empty() {
        return Err(anyhow::anyhow!("Plan has no tasks"));
    }

    for (task_idx, task) in plan.tasks.iter().enumerate() {
        if task.url.trim().is_empty() {
            return Err(anyhow::anyhow!("Task #{task_idx} has empty url"));
        }
        if task.path.trim().is_empty() {
            return Err(anyhow::anyhow!("Task #{task_idx} has empty path"));
        }
        if has_disallowed_scheme(&task.url, &["http", "https"]) {
            return Err(anyhow::anyhow!("Task #{task_idx} url must use http or https"));
        }
    }

    if let Some(proxy) = plan.proxy_url.as_deref().filter(|p| !p.trim().is_empty()) {
        if has_disallowed_scheme(proxy, &["http", "https", "socks4", "socks4a", "socks5", "socks5h"]) {
            return Err(anyhow::anyhow!(
                "proxy_url must use http, https, socks4, socks4a, socks5 or socks5h"
            ));
        }
    }

    Ok(())
}

fn has_disallowed_scheme(url: &str, allowed: &[&str]) -> bool {
    match url.trim().split_once("://") {
        Some((scheme, _)) => !allowed.iter().any(|a| scheme.eq_ignore_ascii_case(a)),
        None => false,
    }
}

#[cfg(test)]
mod scheme_tests {
    use super::has_disallowed_scheme;

    const PROXY: &[&str] = &["http", "https", "socks4", "socks4a", "socks5", "socks5h"];

    #[test]
    fn proxy_schemes_accepted_as_before() {
        for p in [
            "http://h:1", "HTTPS://h:1", "socks4://h:1", "socks4a://h:1",
            "socks5://h:1", "socks5h://h:1", "127.0.0.1:8080", "user:pw@h:1",
        ] {
            assert!(!has_disallowed_scheme(p, PROXY), "{p}");
        }
    }

    #[test]
    fn explicit_foreign_schemes_are_rejected() {
        assert!(has_disallowed_scheme("file:///etc/passwd", &["http", "https"]));
        assert!(has_disallowed_scheme("ftp://h/x", &["http", "https"]));
        assert!(has_disallowed_scheme("gopher://h:1", PROXY));
        assert!(!has_disallowed_scheme("https://h/x", &["http", "https"]));
    }
}

// ---------------------------------------------------------------------------
// Stdout event writer
// ---------------------------------------------------------------------------
fn write_event_stdout(payload: &serde_json::Value) -> std::io::Result<()> {
    let mut out = std::io::stdout().lock();
    writeln!(out, "{}", payload)?;
    out.flush()
}
