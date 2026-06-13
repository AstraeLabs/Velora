use std::collections::HashMap;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// DownloadPlan
// ---------------------------------------------------------------------------
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DownloadPlan {
    #[serde(default = "default_project")]
    pub project: String,

    #[serde(default = "default_version")]
    pub version: u32,

    #[serde(default = "default_task_key")]
    pub task_key: String,

    #[serde(default)]
    pub label: Option<String>,

    #[serde(default)]
    pub display_label: Option<String>,

    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    #[serde(default = "default_retry_count")]
    pub retry_count: u32,

    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,

    #[serde(default = "default_max_redirects")]
    pub max_redirects: u32,

    #[serde(default = "default_retry_base_delay")]
    pub retry_base_delay_seconds: f64,

    #[serde(default = "default_retry_max_delay")]
    pub retry_max_delay_seconds: f64,

    #[serde(default)]
    pub retry_jitter_seconds: f64,

    #[serde(default)]
    pub proxy_url: Option<String>,

    #[serde(default = "default_verify_tls")]
    pub verify_tls: bool,

    #[serde(default)]
    pub headers: HashMap<String, String>,

    #[serde(default)]
    pub user_agent: Option<String>,

    #[serde(default)]
    pub tasks: Vec<DownloadTask>,
}

impl DownloadPlan {
    pub fn normalise(&mut self) {
        if self.project.trim().is_empty() {
            self.project = self
                .label
                .clone()
                .filter(|v| !v.trim().is_empty())
                .unwrap_or_else(default_project);
        }
        if self.task_key.is_empty() {
            self.task_key = "download".into();
        }
        let label = self
            .label
            .get_or_insert_with(|| self.project.clone())
            .clone();
        let display_label = self
            .display_label
            .get_or_insert_with(|| label.clone())
            .clone();

        self.concurrency              = self.concurrency.max(1);
        self.retry_count              = self.retry_count.max(1);
        self.timeout_seconds          = self.timeout_seconds.max(1);
        self.max_redirects            = self.max_redirects.max(1);
        self.retry_base_delay_seconds = self.retry_base_delay_seconds.max(0.0);
        self.retry_max_delay_seconds  =
            self.retry_max_delay_seconds.max(self.retry_base_delay_seconds);
        self.retry_jitter_seconds     = self.retry_jitter_seconds.max(0.0);

        for task in &mut self.tasks {
            task.task_key.get_or_insert_with(|| self.task_key.clone());
            task.label.get_or_insert_with(|| label.clone());
            task.display_label.get_or_insert_with(|| display_label.clone());
        }
    }
}

// ---------------------------------------------------------------------------
// DownloadTask
// ---------------------------------------------------------------------------
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DownloadTask {
    #[serde(default)]
    pub task_key: Option<String>,

    #[serde(default)]
    pub label: Option<String>,

    #[serde(default)]
    pub display_label: Option<String>,

    #[serde(default)]
    pub url: String,

    #[serde(default)]
    pub path: String,

    #[serde(default)]
    pub headers: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Default helpers
// ---------------------------------------------------------------------------
fn default_project()         -> String { "Velora".into() }
fn default_version()         -> u32    { 1 }
fn default_task_key()        -> String { "download".into() }
fn default_concurrency()     -> usize  { 8 }
fn default_retry_count()     -> u32    { 3 }
fn default_timeout_seconds() -> u64    { 30 }
fn default_max_redirects()   -> u32    { 10 }
fn default_verify_tls()      -> bool   { true }
fn default_retry_base_delay()-> f64    { 1.0 }
fn default_retry_max_delay() -> f64    { 30.0 }
