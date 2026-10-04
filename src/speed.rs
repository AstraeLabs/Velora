use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct RateLimiter {
    bytes_per_sec: f64,
    next_free: Mutex<Instant>,
}

impl RateLimiter {
    /// `None` when `bytes_per_sec` is 0 (unlimited).
    pub fn new(bytes_per_sec: u64) -> Option<Self> {
        (bytes_per_sec > 0).then(|| Self {
            bytes_per_sec: bytes_per_sec as f64,
            next_free: Mutex::new(Instant::now()),
        })
    }

    /// Books `bytes` on the timeline and returns how long the caller must wait before using them.
    pub fn reserve(&self, bytes: u64) -> Duration {
        let now = Instant::now();
        let mut next = self.next_free.lock().unwrap_or_else(|e| e.into_inner());
        let start = (*next).max(now);
        *next = start + Duration::from_secs_f64(bytes as f64 / self.bytes_per_sec);
        start.saturating_duration_since(now)
    }
}

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
    fn rate_limiter_is_off_for_zero() {
        assert!(RateLimiter::new(0).is_none());
    }

    #[test]
    fn rate_limiter_paces_consecutive_reservations() {
        let limiter = RateLimiter::new(1_000_000).unwrap();

        // First chunk is free; the next ones queue behind it at 1 MB/s.
        assert!(limiter.reserve(500_000) < Duration::from_millis(50));
        let second = limiter.reserve(500_000);
        assert!(second > Duration::from_millis(400) && second < Duration::from_millis(600), "{second:?}");
        let third = limiter.reserve(500_000);
        assert!(third > Duration::from_millis(900) && third < Duration::from_millis(1100), "{third:?}");
    }

    #[test]
    fn rate_limiter_does_not_bank_idle_time() {
        let limiter = RateLimiter::new(1_000_000).unwrap();
        std::thread::sleep(Duration::from_millis(200));

        // 200 ms idle must not allow a burst: two 500 KB chunks still take ~0.5 s apart.
        let _ = limiter.reserve(500_000);
        assert!(limiter.reserve(500_000) > Duration::from_millis(400));
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
