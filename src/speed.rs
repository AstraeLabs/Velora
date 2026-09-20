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

    pub fn current_bps(&self) -> f64 {
        let smooth = self.smoothed_bps.load(Ordering::Relaxed);
        if smooth != 0 {
            return smooth as f64;
        }
        average_bps(
            self.total_bytes.load(Ordering::Relaxed),
            self.start.elapsed().as_millis() as u64,
        )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_bps_does_not_disturb_bytes_per_second_sampling_window() {
        let tracker = SpeedTracker::new();
        tracker.record(1_000_000);

        // Prime the windowed sampler once so it has a non-zero smoothed rate.
        std::thread::sleep(std::time::Duration::from_millis(20));
        tracker.record(1_000_000);
        let _ = tracker.bytes_per_second();

        let before = tracker.last_snapshot_bytes.load(Ordering::Relaxed);
        let before_ms = tracker.last_snapshot_ms.load(Ordering::Relaxed);

        // Many concurrent "current_bps" reads (simulating per-segment
        // completion events) must not move the ticker's sampling baseline.
        for _ in 0..50 {
            let _ = tracker.current_bps();
        }

        assert_eq!(tracker.last_snapshot_bytes.load(Ordering::Relaxed), before);
        assert_eq!(tracker.last_snapshot_ms.load(Ordering::Relaxed), before_ms);
    }

    #[test]
    fn bytes_per_second_does_move_the_sampling_window() {
        let tracker = SpeedTracker::new();
        tracker.record(1_000_000);
        let _ = tracker.bytes_per_second();
        let before_ms = tracker.last_snapshot_ms.load(Ordering::Relaxed);

        std::thread::sleep(std::time::Duration::from_millis(10));
        tracker.record(1_000_000);
        let _ = tracker.bytes_per_second();

        assert_ne!(tracker.last_snapshot_ms.load(Ordering::Relaxed), before_ms);
    }
}
