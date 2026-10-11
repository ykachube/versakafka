//! Local adaptive circuit breaker

use crate::{BreakerError, Result, BreakerState, config::LocalBreakerConfig};
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{Duration, Instant};
use std::collections::VecDeque;

pub struct LocalBreaker {
    config: LocalBreakerConfig,
    state: Arc<RwLock<BreakerState>>,
    window: Arc<RwLock<SlidingWindow>>,
    last_state_change: Arc<RwLock<Instant>>,
    half_open_successes: Arc<RwLock<usize>>,
}

#[derive(Default)]
struct SlidingWindow {
    requests: VecDeque<RequestResult>,
    window_size: Duration,
}

#[derive(Debug, Clone)]
struct RequestResult {
    timestamp: Instant,
    success: bool,
    latency: Duration,
}

impl LocalBreaker {
    pub fn new(config: LocalBreakerConfig) -> Self {
        let window_size = config.window_size;
        Self {
            config,
            state: Arc::new(RwLock::new(BreakerState::Closed)),
            window: Arc::new(RwLock::new(SlidingWindow {
                window_size,
                ..Default::default()
            })),
            last_state_change: Arc::new(RwLock::new(Instant::now())),
            half_open_successes: Arc::new(RwLock::new(0)),
        }
    }

    pub fn state(&self) -> BreakerState {
        *self.state.read()
    }

    pub async fn call<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        if !self.can_execute() {
            return Err(BreakerError::Local("Circuit breaker is open".to_string()));
        }

        let start = Instant::now();
        let result = f().await;
        let latency = start.elapsed();
        let success = result.is_ok();

        self.record_result(success, latency);

        result
    }

    fn can_execute(&self) -> bool {
        let state = *self.state.read();
        match state {
            BreakerState::Closed => true,
            BreakerState::Open => {
                // Check if we should transition to half-open
                let last_change = *self.last_state_change.read();
                if last_change.elapsed() >= self.config.half_open_interval {
                    self.transition_to_half_open();
                    true
                } else {
                    false
                }
            }
            BreakerState::HalfOpen => true,
        }
    }

    fn record_result(&self, success: bool, latency: Duration) {
        let mut window = self.window.write();
        let now = Instant::now();
        
        window.requests.push_back(RequestResult {
            timestamp: now,
            success,
            latency,
        });

        // Remove old entries
        while let Some(front) = window.requests.front() {
            if now.duration_since(front.timestamp) > window.window_size {
                window.requests.pop_front();
            } else {
                break;
            }
        }

        // Check if we should open the breaker
        self.evaluate_state(success);
    }

    fn evaluate_state(&self, last_success: bool) {
        let window = self.window.read();
        if window.requests.len() < self.config.min_requests {
            return;
        }

        let total = window.requests.len();
        let errors = window.requests.iter().filter(|r| !r.success).count();
        let error_rate = errors as f64 / total as f64;

        let avg_latency = window.requests.iter()
            .map(|r| r.latency)
            .sum::<Duration>() / total as u32;

        let should_open = error_rate >= self.config.error_rate_threshold
            || avg_latency >= self.config.latency_threshold;

        let mut state = self.state.write();
        match *state {
            BreakerState::Closed if should_open => {
                *state = BreakerState::Open;
                *self.last_state_change.write() = Instant::now();
            }
            BreakerState::HalfOpen if last_success => {
                let mut successes = self.half_open_successes.write();
                *successes += 1;
                if *successes >= self.config.half_open_requests {
                    *state = BreakerState::Closed;
                    *self.half_open_successes.write() = 0;
                }
            }
            BreakerState::HalfOpen if !last_success => {
                *state = BreakerState::Open;
                *self.last_state_change.write() = Instant::now();
                *self.half_open_successes.write() = 0;
            }
            _ => {}
        }
    }

    fn transition_to_half_open(&self) {
        *self.state.write() = BreakerState::HalfOpen;
        *self.half_open_successes.write() = 0;
    }
}