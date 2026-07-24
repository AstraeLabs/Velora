use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub struct SpeedTracker {
    start: Instant,
    total_bytes: AtomicU64,
    last_snapshot_bytes: AtomicU64,
    last_snapshot_ms: AtomicU64,
    smoothed_bps: AtomicU64,
}

impl SpeedTracker {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            total_bytes: AtomicU64::new(0),
            last_snapshot_bytes: AtomicU64::new(0),
            last_snapshot_ms: AtomicU64::new(0),
            smoothed_bps: AtomicU64::new(0),
        }
    }

    pub fn record(&self, bytes: u64) {
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    pub fn bytes_per_second(&self) -> f64 {
        let now_ms = self.start.elapsed().as_millis() as u64;
        let total = self.total_bytes.load(Ordering::Relaxed);

        let prev_ms = self.last_snapshot_ms.swap(now_ms, Ordering::Relaxed);
        let prev_bytes = self.last_snapshot_bytes.swap(total, Ordering::Relaxed);

        if prev_ms == 0 || now_ms <= prev_ms {
            return average_bps(total, now_ms);
        }

        let delta_ms = (now_ms - prev_ms).max(1);
        let delta_bytes = total.saturating_sub(prev_bytes);
        let instant_bps = (delta_bytes as f64 / delta_ms as f64 * 1_000.0) as u64;

        let prev_smooth = self.smoothed_bps.load(Ordering::Relaxed);
        let next_smooth = if prev_smooth == 0 {
            instant_bps
        } else {
            ((prev_smooth as f64 * 0.5) + (instant_bps as f64 * 0.5)) as u64
        };

        self.smoothed_bps.store(next_smooth, Ordering::Relaxed);

        if next_smooth == 0 {
            average_bps(total, now_ms)
        } else {
            next_smooth as f64
        }
    }
}

fn average_bps(total_bytes: u64, elapsed_ms: u64) -> f64 {
    if elapsed_ms == 0 {
        0.0
    } else {
        total_bytes as f64 / (elapsed_ms as f64 / 1_000.0)
    }
}
