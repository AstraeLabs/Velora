use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::stream;
use futures::StreamExt;
use rand::Rng;
use reqwest::header::{
    HeaderName,
    HeaderValue,
    ACCEPT_ENCODING,
    CONTENT_LENGTH,
    RANGE,
    USER_AGENT,
};
use reqwest::{Client, Proxy, StatusCode};
use serde_json::{json, Value};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::models::{DownloadPlan, DownloadTask};
use crate::speed::SpeedTracker;

const DEFAULT_USER_AGENT: &str = "Velora/2";
const WRITE_BUFFER_BYTES: usize = 256 * 1024;
static STDOUT_PIPE_CLOSED: AtomicBool = AtomicBool::new(false);

// ---------------------------------------------------------------------------
// HTTP client cache key + builder
// ---------------------------------------------------------------------------
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ClientKey {
    pub timeout_seconds: u64,
    pub proxy_url: Option<String>,
    pub max_redirects: u32,
    pub verify_tls: bool,
}

impl ClientKey {
    pub fn from_plan(plan: &DownloadPlan) -> Self {
        Self {
            timeout_seconds: plan.timeout_seconds.max(1),
            proxy_url: plan.proxy_url.clone(),
            max_redirects: plan.max_redirects.max(1),
            verify_tls: plan.verify_tls,
        }
    }
}

pub fn build_client(key: &ClientKey, pool_max_idle: usize) -> anyhow::Result<Client> {
    let mut builder = Client::builder()
        .timeout(std::time::Duration::from_secs(key.timeout_seconds))
        .connection_verbose(false)
        .use_rustls_tls()
        .http1_only()
        .pool_max_idle_per_host(pool_max_idle.max(1))
        .redirect(reqwest::redirect::Policy::limited(key.max_redirects.max(1) as usize));

    if !key.verify_tls {
        builder = builder.danger_accept_invalid_certs(true);
    }

    if let Some(ref proxy_url) = key.proxy_url {
        builder = builder.proxy(Proxy::all(proxy_url)?);
    }

    Ok(builder.build()?)
}

pub fn is_stdout_pipe_closed() -> bool {
    STDOUT_PIPE_CLOSED.load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------
pub fn format_size(bytes: i64) -> String {
    match bytes {
        b if b >= 1_073_741_824 => format!("{:.2}GB", b as f64 / 1_073_741_824.0),
        b if b >= 1_048_576 => format!("{:.1}MB", b as f64 / 1_048_576.0),
        b if b >= 1_024 => format!("{:.0}KB", b as f64 / 1_024.0),
        b => format!("{}B", b),
    }
}

pub fn format_speed(bps: f64) -> String {
    if bps <= 0.0 {
        return "---".into();
    }
    if bps >= 1_048_576.0 {
        return format!("{:.2}MB/s", bps / 1_048_576.0);
    }
    if bps >= 1_024.0 {
        return format!("{:.0}KB/s", bps / 1_024.0);
    }
    format!("{:.0}B/s", bps)
}

fn write_event(payload: Value) {
    if STDOUT_PIPE_CLOSED.load(Ordering::Relaxed) {
        return;
    }

    let line = payload.to_string();
    let mut out = std::io::stdout().lock();

    if let Err(err) = writeln!(out, "{line}") {
        if err.kind() == std::io::ErrorKind::BrokenPipe {
            STDOUT_PIPE_CLOSED.store(true, Ordering::Relaxed);
        }
        return;
    }

    if let Err(err) = out.flush() {
        if err.kind() == std::io::ErrorKind::BrokenPipe {
            STDOUT_PIPE_CLOSED.store(true, Ordering::Relaxed);
        }
    }
}

fn write_warning(scope: &str, key: &str, value: &str, detail: &str) {
    write_event(json!({
        "event": "warning",
        "scope": scope,
        "key": key,
        "value": value,
        "message": detail,
    }));
}

// ---------------------------------------------------------------------------
// Download attempt error model
// ---------------------------------------------------------------------------
#[derive(Debug, Clone)]
struct AttemptError {
    message: String,
    retryable: bool,
    cancelled: bool,
}

impl AttemptError {
    fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
            cancelled: false,
        }
    }

    fn fatal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
            cancelled: false,
        }
    }

    fn cancelled(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
            cancelled: true,
        }
    }
}

// ---------------------------------------------------------------------------
// DownloadRunner
// ---------------------------------------------------------------------------
pub struct DownloadRunner {
    plan: DownloadPlan,
    client: Arc<Client>,
    speed: Arc<SpeedTracker>,
    cancel_requested: Arc<AtomicBool>,

    plan_headers: Vec<(HeaderName, HeaderValue)>,
    default_user_agent: HeaderValue,

    completed_count: Arc<AtomicU32>,
    failed_count: Arc<AtomicU32>,
    completed_bytes: Arc<AtomicI64>,
}

impl DownloadRunner {
    pub fn new(plan: DownloadPlan, client: Arc<Client>, cancel_requested: Arc<AtomicBool>) -> Self {
        let (plan_headers, has_plan_ua) = parse_headers(&plan.headers, "plan");

        let resolved_ua = if has_plan_ua {
            None
        } else {
            plan.user_agent
                .as_ref()
                .filter(|v| !v.trim().is_empty())
                .cloned()
                .or_else(|| Some(DEFAULT_USER_AGENT.to_string()))
        };

        let default_user_agent = HeaderValue::from_str(
            resolved_ua
                .as_deref()
                .unwrap_or(DEFAULT_USER_AGENT),
        )
        .unwrap_or_else(|_| HeaderValue::from_static(DEFAULT_USER_AGENT));

        Self {
            plan,
            client,
            speed: Arc::new(SpeedTracker::new()),
            cancel_requested,
            plan_headers,
            default_user_agent,
            completed_count: Arc::new(AtomicU32::new(0)),
            failed_count: Arc::new(AtomicU32::new(0)),
            completed_bytes: Arc::new(AtomicI64::new(0)),
        }
    }

    pub async fn run(self) {
        write_event(json!({
            "event": "start",
            "project": self.plan.project,
            "task_key": self.plan.task_key,
            "label": self.plan.label,
            "display_label": self.plan.display_label,
            "task_count": self.plan.tasks.len(),
            "concurrency": self.plan.concurrency,
            "max_redirects": self.plan.max_redirects,
            "verify_tls": self.plan.verify_tls,
        }));

        if self.plan.tasks.is_empty() {
            write_event(json!({
                "event": "summary",
                "task_key": self.plan.task_key,
                "completed": 0,
                "failed": 0,
                "skipped": 0,
                "total": 0,
                "bytes": 0,
                "average_speed": "---",
                "elapsed_seconds": 0.0,
            }));
            return;
        }

        let start = Instant::now();
        let total = self.plan.tasks.len();

        let plan = Arc::new(self.plan);
        let client = self.client.clone();
        let speed = self.speed.clone();
        let cancel_requested = self.cancel_requested.clone();
        let completed_count = self.completed_count.clone();
        let failed_count = self.failed_count.clone();
        let completed_bytes = self.completed_bytes.clone();
        let plan_headers = Arc::new(self.plan_headers);
        let default_user_agent = self.default_user_agent.clone();
        let concurrency = plan.concurrency.max(1);

        let plan_for_tasks = plan.clone();
        let client_for_tasks = client.clone();
        let speed_for_tasks = speed.clone();
        let cancel_requested_for_tasks = cancel_requested.clone();
        let completed_count_for_tasks = completed_count.clone();
        let failed_count_for_tasks = failed_count.clone();
        let completed_bytes_for_tasks = completed_bytes.clone();
        let plan_headers_for_tasks = plan_headers.clone();
        let default_user_agent_for_tasks = default_user_agent.clone();
        let task_count = plan_for_tasks.tasks.len();

        stream::iter(0..task_count)
            .for_each_concurrent(concurrency, move |index| {
                let plan = plan_for_tasks.clone();
                let client = client_for_tasks.clone();
                let speed = speed_for_tasks.clone();
                let cancel_requested = cancel_requested_for_tasks.clone();
                let completed_count = completed_count_for_tasks.clone();
                let failed_count = failed_count_for_tasks.clone();
                let completed_bytes = completed_bytes_for_tasks.clone();
                let plan_headers = plan_headers_for_tasks.clone();
                let default_user_agent = default_user_agent_for_tasks.clone();

                async move {
                    run_one_task(
                        &plan.tasks[index],
                        index,
                        total,
                        &plan,
                        &client,
                        &speed,
                        &plan_headers,
                        &default_user_agent,
                        &cancel_requested,
                        &completed_count,
                        &failed_count,
                        &completed_bytes,
                    )
                    .await;
                }
            })
            .await;

        let elapsed = start.elapsed().as_secs_f64().max(0.001);
        let done_bytes = completed_bytes.load(Ordering::Relaxed);
        let done = completed_count.load(Ordering::Relaxed);
        let failed = failed_count.load(Ordering::Relaxed);

        write_event(json!({
            "event": "summary",
            "task_key": plan.task_key,
            "completed": done,
            "failed": failed,
            "skipped": (total as u32).saturating_sub(done + failed),
            "total": total,
            "bytes": done_bytes,
            "average_speed": format_speed(done_bytes as f64 / elapsed),
            "elapsed_seconds": elapsed,
        }));
    }
}

// ---------------------------------------------------------------------------
// Header parsing helpers
// ---------------------------------------------------------------------------
fn parse_headers(headers: &HashMap<String, String>, scope: &str) -> (Vec<(HeaderName, HeaderValue)>, bool) {
    let mut parsed = Vec::with_capacity(headers.len());
    let mut has_user_agent = false;

    for (raw_k, raw_v) in headers {
        let name = match HeaderName::from_bytes(raw_k.as_bytes()) {
            Ok(v) => v,
            Err(e) => {
                write_warning(scope, raw_k, raw_v, &format!("Invalid header name dropped: {e}"));
                continue;
            }
        };

        let value = match HeaderValue::from_str(raw_v) {
            Ok(v) => v,
            Err(e) => {
                write_warning(scope, raw_k, raw_v, &format!("Invalid header value dropped: {e}"));
                continue;
            }
        };

        if name == USER_AGENT {
            has_user_agent = true;
        }

        parsed.push((name, value));
    }

    (parsed, has_user_agent)
}

fn apply_headers(mut req: reqwest::RequestBuilder, plan_headers: &[(HeaderName, HeaderValue)], task_headers: &[(HeaderName, HeaderValue)], default_user_agent: &HeaderValue) -> reqwest::RequestBuilder {
    let mut has_ua = false;
    let mut has_accept_encoding = false;

    for (name, value) in plan_headers {
        if *name == USER_AGENT {
            has_ua = true;
        }
        if *name == ACCEPT_ENCODING {
            has_accept_encoding = true;
        }
        req = req.header(name, value.clone());
    }

    for (name, value) in task_headers {
        if *name == USER_AGENT {
            has_ua = true;
        }
        if *name == ACCEPT_ENCODING {
            has_accept_encoding = true;
        }
        req = req.header(name, value.clone());
    }

    if !has_ua {
        req = req.header(USER_AGENT, default_user_agent.clone());
    }

    if !has_accept_encoding {
        req = req.header(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    }

    req
}

// ---------------------------------------------------------------------------
// Single-task orchestration
// ---------------------------------------------------------------------------
#[allow(clippy::too_many_arguments)]
async fn run_one_task(
    task: &DownloadTask,
    _index: usize,
    total: usize,
    plan: &DownloadPlan,
    client: &Client,
    speed: &SpeedTracker,
    plan_headers: &[(HeaderName, HeaderValue)],
    default_user_agent: &HeaderValue,
    cancel_requested: &AtomicBool,
    completed_count: &AtomicU32,
    failed_count: &AtomicU32,
    completed_bytes: &AtomicI64,
) {
    if cancel_requested.load(Ordering::Relaxed) || is_stdout_pipe_closed() {
        return;
    }

    let task_start = Instant::now();

    if task.url.is_empty() || task.path.is_empty() {
        register_failure(
            task,
            total,
            "Task is missing url or path",
            task_start.elapsed().as_secs_f64(),
            plan,
            failed_count,
            completed_count,
        )
        .await;
        return;
    }

    let (task_headers, _has_task_ua) = parse_headers(&task.headers, "task");

    let final_path = PathBuf::from(&task.path);
    let temp_path = PathBuf::from(format!("{}.part", final_path.display()));

    // - if output exists and remote length is known and matches => skip
    // - if output exists and remote length says it's partial => resume from .part
    if let Ok(meta) = fs::metadata(&final_path).await {
        if meta.len() > 0 && fs::metadata(&temp_path).await.is_err() {
            match probe_remote_content_length(
                task,
                plan,
                client,
                plan_headers,
                &task_headers,
                default_user_agent,
            )
            .await
            {
                Some(remote_len) if remote_len == meta.len() => {
                    register_completion(
                        task,
                        &final_path,
                        meta.len(),
                        total,
                        true,
                        task_start.elapsed().as_secs_f64(),
                        plan,
                        speed,
                        completed_count,
                        completed_bytes,
                    )
                    .await;
                    return;
                }
                Some(remote_len) if remote_len > meta.len() => {
                    let _ = fs::rename(&final_path, &temp_path).await;
                }
                Some(remote_len) if remote_len < meta.len() => {
                    let _ = fs::remove_file(&final_path).await;
                }
                _ => {
                    register_completion(
                        task,
                        &final_path,
                        meta.len(),
                        total,
                        true,
                        task_start.elapsed().as_secs_f64(),
                        plan,
                        speed,
                        completed_count,
                        completed_bytes,
                    )
                    .await;
                    return;
                }
            }
        }
    }

    if let Some(parent) = final_path.parent() {
        let _ = fs::create_dir_all(parent).await;
    }

    maybe_segment_delay(plan, cancel_requested).await;

    let mut last_error = String::new();

    for attempt in 1..=plan.retry_count {
        if cancel_requested.load(Ordering::Relaxed) || is_stdout_pipe_closed() {
            let _ = fs::remove_file(&temp_path).await;
            return;
        }

        match try_download_once(
            task,
            &temp_path,
            plan,
            client,
            speed,
            plan_headers,
            &task_headers,
            default_user_agent,
            cancel_requested,
        )
        .await
        {
            Ok(file_size) => {
                if let Err(e) = promote_temp_to_final(&temp_path, &final_path).await {
                    last_error = format!("Unable to promote temp file: {e}");
                    break;
                }

                register_completion(
                    task,
                    &final_path,
                    file_size,
                    total,
                    false,
                    task_start.elapsed().as_secs_f64(),
                    plan,
                    speed,
                    completed_count,
                    completed_bytes,
                )
                .await;
                return;
            }
            Err(e) => {
                last_error = e.message.clone();

                if e.cancelled {
                    let _ = fs::remove_file(&temp_path).await;
                    return;
                }

                if attempt < plan.retry_count && e.retryable {
                    let delay = compute_retry_delay(attempt, plan);
                    write_event(json!({
                        "event": "retry",
                        "task_key": task.task_key,
                        "label": task.label,
                        "display_label": task.display_label,
                        "path": final_path,
                        "url": task.url,
                        "attempt": attempt,
                        "retry_count": plan.retry_count,
                        "retry_delay_seconds": (delay * 100.0).round() / 100.0,
                        "message": &last_error,
                        "elapsed_seconds": task_start.elapsed().as_secs_f64(),
                    }));

                    tokio::time::sleep(std::time::Duration::from_secs_f64(delay)).await;
                    continue;
                }

                break;
            }
        }
    }

    register_failure(
        task,
        total,
        &last_error,
        task_start.elapsed().as_secs_f64(),
        plan,
        failed_count,
        completed_count,
    )
    .await;
}

async fn probe_remote_content_length(
    task: &DownloadTask,
    plan: &DownloadPlan,
    client: &Client,
    plan_headers: &[(HeaderName, HeaderValue)],
    task_headers: &[(HeaderName, HeaderValue)],
    default_user_agent: &HeaderValue,
) -> Option<u64> {
    let req = client
        .head(&task.url)
        .timeout(std::time::Duration::from_secs(plan.timeout_seconds.max(1)));

    let req = apply_headers(req, plan_headers, task_headers, default_user_agent);

    let response = req.send().await.ok()?;

    if !response.status().is_success() {
        return None;
    }

    response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
}

#[allow(clippy::too_many_arguments)]
async fn try_download_once(
    task: &DownloadTask,
    temp_path: &Path,
    plan: &DownloadPlan,
    client: &Client,
    speed: &SpeedTracker,
    plan_headers: &[(HeaderName, HeaderValue)],
    task_headers: &[(HeaderName, HeaderValue)],
    default_user_agent: &HeaderValue,
    cancel_requested: &AtomicBool,
) -> Result<u64, AttemptError> {
    if cancel_requested.load(Ordering::Relaxed) || is_stdout_pipe_closed() {
        return Err(AttemptError::cancelled("Cancellation requested"));
    }

    let mut resume_from = fs::metadata(temp_path).await.map(|m| m.len()).unwrap_or(0);
    let task_has_range = task_headers.iter().any(|(name, _)| *name == RANGE);
    if task_has_range && resume_from > 0 {
        let _ = fs::remove_file(temp_path).await;
        resume_from = 0;
    }

    let mut req = client
        .get(&task.url)
        .timeout(std::time::Duration::from_secs(plan.timeout_seconds.max(1)));

    req = apply_headers(req, plan_headers, task_headers, default_user_agent);

    if resume_from > 0 {
        req = req.header(RANGE, format!("bytes={resume_from}-"));
    }

    let response = req.send().await.map_err(|e| AttemptError::retryable(format!("Request failed: {e}")))?;
    let status = response.status();
    let response_content_length = response.content_length();

    if resume_from > 0 && status == StatusCode::OK {
        // Server ignored Range and returned full content. Restart from zero.
        let _ = fs::remove_file(temp_path).await;
        resume_from = 0;
    }

    if status == StatusCode::RANGE_NOT_SATISFIABLE {
        return Err(AttemptError::fatal(format!(
            "HTTP 416 while resuming {}",
            task.url
        )));
    }

    if !status.is_success() {
        return Err(AttemptError {
            message: format!(
                "HTTP {} {}",
                status.as_u16(),
                status.canonical_reason().unwrap_or("")
            ),
            retryable: is_retryable_status(status),
            cancelled: false,
        });
    }

    let append = resume_from > 0 && status == StatusCode::PARTIAL_CONTENT;
    let file = if append {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(temp_path)
            .await
            .map_err(|e| AttemptError::fatal(format!("Open append failed: {e}")))?
    } else {
        File::create(temp_path)
            .await
            .map_err(|e| AttemptError::fatal(format!("Create temp file failed: {e}")))?
    };
    let mut file = BufWriter::with_capacity(WRITE_BUFFER_BYTES, file);

    let mut stream = response.bytes_stream();
    let mut file_size = if append { resume_from } else { 0 };

    while let Some(chunk_result) = stream.next().await {
        if cancel_requested.load(Ordering::Relaxed) || is_stdout_pipe_closed() {
            return Err(AttemptError::cancelled("Cancelled during transfer"));
        }

        let chunk: Bytes = match chunk_result {
            Ok(chunk) => chunk,
            Err(e) => {
                
                // The body was truncated mid-transfer (premature EOF / chunk decode error). Discard the partial file before retrying
                drop(file);
                let _ = fs::remove_file(temp_path).await;
                return Err(AttemptError::retryable(format!("Stream read failed: {e}")));
            }
        };
        let len = chunk.len() as u64;

        file.write_all(&chunk)
            .await
            .map_err(|e| AttemptError::fatal(format!("Write failed: {e}")))?;

        file_size += len;
        speed.record(len);
    }

    file.flush()
        .await
        .map_err(|e| AttemptError::fatal(format!("Flush failed: {e}")))?;

    if file_size == 0 {
        drop(file);
        let _ = fs::remove_file(temp_path).await;
        return Err(AttemptError::retryable(format!(
            "Downloaded file is empty: {}",
            task.url
        )));
    }

    if let Some(expected_body) = response_content_length {
        let expected_total = if append { resume_from + expected_body } else { expected_body };
        if file_size != expected_total {
            drop(file);
            let _ = fs::remove_file(temp_path).await;
            return Err(AttemptError::retryable(format!(
                "Incomplete download: got {file_size} bytes, expected {expected_total}: {}",
                task.url
            )));
        }
    }

    Ok(file_size)
}

fn is_retryable_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}

// ---------------------------------------------------------------------------
// Registration helpers
// ---------------------------------------------------------------------------
#[allow(clippy::too_many_arguments)]
async fn register_completion(
    task: &DownloadTask,
    final_path: &Path,
    bytes: u64,
    total: usize,
    skipped: bool,
    elapsed_seconds: f64,
    plan: &DownloadPlan,
    speed: &SpeedTracker,
    completed_count: &AtomicU32,
    completed_bytes: &AtomicI64,
) {
    if !skipped {
        completed_count.fetch_add(1, Ordering::Relaxed);
    }

    completed_bytes.fetch_add(bytes as i64, Ordering::Relaxed);

    let done = completed_count.load(Ordering::Relaxed);
    let cb = completed_bytes.load(Ordering::Relaxed);

    let processed = done;
    let estimated_total = if processed > 0 {
        ((cb as f64 / processed as f64) * total as f64).max(cb as f64) as i64
    } else {
        cb
    };

    write_event(json!({
        "event": "completed",
        "task_key": task.task_key.as_deref().unwrap_or(&plan.task_key),
        "label": task.label.as_deref().or(plan.label.as_deref()),
        "display_label": task.display_label.as_deref().or(plan.display_label.as_deref()),
        "pct": if total > 0 { (processed as f64 / total as f64 * 100.0).round() as u32 } else { 100 },
        "segments": format!("{}/{}", processed, total),
        "size": if estimated_total > 0 {
            format!("{}/{}", format_size(cb), format_size(estimated_total))
        } else {
            format_size(cb)
        },
        "final_size": format_size(bytes as i64),
        "speed": format_speed(speed.bytes_per_second()),
        "path": final_path,
        "url": task.url,
        "bytes": bytes,
        "skipped": skipped,
        "elapsed_seconds": elapsed_seconds,
    }));
}

#[allow(clippy::too_many_arguments)]
async fn register_failure(
    task: &DownloadTask,
    total: usize,
    error_message: &str,
    elapsed_seconds: f64,
    plan: &DownloadPlan,
    failed_count: &AtomicU32,
    completed_count: &AtomicU32,
) {
    failed_count.fetch_add(1, Ordering::Relaxed);

    let done = completed_count.load(Ordering::Relaxed);
    let failed = failed_count.load(Ordering::Relaxed);
    let processed = done + failed;

    write_event(json!({
        "event": "error",
        "task_key": task.task_key.as_deref().unwrap_or(&plan.task_key),
        "label": task.label.as_deref().or(plan.label.as_deref()),
        "display_label": task.display_label.as_deref().or(plan.display_label.as_deref()),
        "pct": if total > 0 { (processed as f64 / total as f64 * 100.0).round() as u32 } else { 100 },
        "segments": format!("{}/{}", processed, total),
        "path": task.path,
        "url": task.url,
        "message": error_message,
        "elapsed_seconds": elapsed_seconds,
    }));
}

// ---------------------------------------------------------------------------
// Retry delay
// ---------------------------------------------------------------------------
fn compute_retry_delay(attempt: u32, plan: &DownloadPlan) -> f64 {
    let base = plan.retry_base_delay_seconds * 2f64.powi(attempt as i32 - 1);
    let delay = base.min(plan.retry_max_delay_seconds);
    if plan.retry_jitter_seconds > 0.0 {
        delay + rand::thread_rng().gen::<f64>() * plan.retry_jitter_seconds
    } else {
        delay
    }
}

// ---------------------------------------------------------------------------
// Delay per-segment pacing
// ---------------------------------------------------------------------------
async fn maybe_segment_delay(plan: &DownloadPlan, cancel_requested: &AtomicBool) {
    if plan.segment_delay_seconds <= 0.0 && plan.segment_delay_jitter_seconds <= 0.0 {
        return;
    }

    let jitter = if plan.segment_delay_jitter_seconds > 0.0 {
        rand::thread_rng().gen::<f64>() * plan.segment_delay_jitter_seconds
    } else {
        0.0
    };

    let total = (plan.segment_delay_seconds + jitter).max(0.0);
    if total <= 0.0 {
        return;
    }

    let mut remaining = total;
    while remaining > 0.0 {
        if cancel_requested.load(Ordering::Relaxed) || is_stdout_pipe_closed() {
            return;
        }
        let slice = remaining.min(0.25);
        tokio::time::sleep(std::time::Duration::from_secs_f64(slice)).await;
        remaining -= slice;
    }
}

// ---------------------------------------------------------------------------
// Temp -> final promotion
// ---------------------------------------------------------------------------
async fn promote_temp_to_final(temp_path: &Path, final_path: &Path) -> std::io::Result<()> {
    #[cfg(windows)]
    {
        let temp = temp_path.to_path_buf();
        let final_path = final_path.to_path_buf();

        tokio::task::spawn_blocking(move || promote_temp_to_final_windows(&temp, &final_path))
            .await
            .map_err(|e| std::io::Error::other(e.to_string()))?
    }

    #[cfg(not(windows))]
    {
        fs::rename(temp_path, final_path).await
    }
}

#[cfg(windows)]
fn promote_temp_to_final_windows(temp_path: &Path, final_path: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::Storage::FileSystem::{
        ReplaceFileW,
        REPLACEFILE_IGNORE_ACL_ERRORS,
        REPLACEFILE_IGNORE_MERGE_ERRORS,
    };

    if !final_path.exists() {
        return std::fs::rename(temp_path, final_path);
    }

    let final_w: Vec<u16> = final_path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();

    let temp_w: Vec<u16> = temp_path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();

    let ok = unsafe {
        ReplaceFileW(
            final_w.as_ptr(),
            temp_w.as_ptr(),
            std::ptr::null(),
            REPLACEFILE_IGNORE_MERGE_ERRORS | REPLACEFILE_IGNORE_ACL_ERRORS,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };

    if ok == 0 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(())
}