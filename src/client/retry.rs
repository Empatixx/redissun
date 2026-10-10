use fred::prelude::ReconnectPolicy;
use std::time::Duration;
use uuid::Uuid;

/// How long to pause before the next retry or reconnection attempt, like Redisson's `DelayStrategy`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DelayStrategy {
    /// The same pause every time, like Redisson's `ConstantDelay`.
    Constant(Duration),
    /// A pause that doubles from `base` up to `max` and then is cut to a random value between half and all of it, like Redisson's `EqualJitterDelay`.
    EqualJitter {
        /// The pause before the first retry, before jitter.
        base: Duration,
        /// The longest pause, before jitter.
        max: Duration,
    },
    /// A pause that doubles from `base` up to `max` and then is cut to a random value between zero and all of it, like Redisson's `FullJitterDelay`.
    FullJitter {
        /// The pause before the first retry, before jitter.
        base: Duration,
        /// The longest pause, before jitter.
        max: Duration,
    },
    /// A random pause between `min` and three times the previous pause, at most `max`, like Redisson's `DecorrelatedJitterDelay`.
    DecorrelatedJitter {
        /// The shortest pause.
        min: Duration,
        /// The longest pause.
        max: Duration,
    },
}

fn random_up_to(bound: u64) -> u64 {
    (Uuid::new_v4().as_u128() % (u128::from(bound) + 1)) as u64
}

fn millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn exponential(base: u64, max: u64, attempt: u32) -> u64 {
    if attempt >= 63 {
        return max;
    }
    base.checked_mul(1u64 << attempt)
        .map_or(max, |delay| delay.min(max))
}

impl DelayStrategy {
    pub(crate) fn delay(&self, attempt: u32, previous: Duration) -> Duration {
        Duration::from_millis(match *self {
            DelayStrategy::Constant(delay) => millis(delay),
            DelayStrategy::EqualJitter { base, max } => {
                let (base, max) = (millis(base), millis(max));
                let full = if base >= max {
                    max
                } else {
                    exponential(base, max, attempt)
                };
                let half = full / 2;
                half + random_up_to(half)
            }
            DelayStrategy::FullJitter { base, max } => {
                let (base, max) = (millis(base), millis(max));
                let full = if base == 0 {
                    max
                } else {
                    exponential(base, max, attempt)
                };
                random_up_to(full.max(1))
            }
            DelayStrategy::DecorrelatedJitter { min, max } => {
                let previous = if previous.is_zero() { min } else { previous };
                let range = millis(previous).saturating_mul(3);
                let random = if range == 0 {
                    0
                } else {
                    random_up_to(range - 1)
                };
                millis(min).saturating_add(random).min(millis(max))
            }
        })
    }

    pub(crate) fn longest(&self, attempt: u32) -> Duration {
        match *self {
            DelayStrategy::Constant(delay) => delay,
            DelayStrategy::EqualJitter { base, max } | DelayStrategy::FullJitter { base, max } => {
                if base >= max || base.is_zero() {
                    max
                } else {
                    Duration::from_millis(exponential(millis(base), millis(max), attempt))
                }
            }
            DelayStrategy::DecorrelatedJitter { max, .. } => max,
        }
    }

    pub(crate) fn reconnect_policy(&self) -> ReconnectPolicy {
        let clamp = |duration: Duration| u32::try_from(duration.as_millis()).unwrap_or(u32::MAX);
        let mut policy = match *self {
            DelayStrategy::Constant(delay) => ReconnectPolicy::new_constant(0, clamp(delay)),
            DelayStrategy::EqualJitter { base, max } => {
                let mut policy =
                    ReconnectPolicy::new_exponential(0, clamp(base / 2).max(1), clamp(max), 2);
                policy.set_jitter(clamp(base / 2));
                return policy;
            }
            DelayStrategy::FullJitter { base, max } => {
                ReconnectPolicy::new_exponential(0, clamp(base).max(1), clamp(max), 2)
            }
            DelayStrategy::DecorrelatedJitter { min, max } => {
                ReconnectPolicy::new_exponential(0, clamp(min).max(1), clamp(max), 3)
            }
        };
        policy.set_jitter(0);
        policy
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Retry {
    pub(crate) attempts: u32,
    pub(crate) delay: DelayStrategy,
    pub(crate) timeout: Duration,
}

impl Retry {
    pub(crate) fn unlock_latch_ttl(&self) -> Duration {
        (self.timeout + self.delay.longest(self.attempts)).saturating_mul(self.attempts.max(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: Duration = Duration::from_secs(1);

    #[test]
    fn constant_delay_never_changes() {
        let strategy = DelayStrategy::Constant(SECOND);
        for attempt in 0..10 {
            assert_eq!(strategy.delay(attempt, Duration::ZERO), SECOND);
        }
    }

    #[test]
    fn equal_jitter_stays_between_half_and_all_of_the_exponential_delay() {
        let strategy = DelayStrategy::EqualJitter {
            base: Duration::from_millis(100),
            max: Duration::from_secs(10),
        };
        for _ in 0..200 {
            let first = strategy.delay(0, Duration::ZERO).as_millis();
            assert!((50..=100).contains(&first), "{first}");
            let fourth = strategy.delay(3, Duration::ZERO).as_millis();
            assert!((400..=800).contains(&fourth), "{fourth}");
            let late = strategy.delay(40, Duration::ZERO).as_millis();
            assert!((5000..=10_000).contains(&late), "{late}");
        }
    }

    #[test]
    fn full_jitter_stays_below_the_exponential_delay() {
        let strategy = DelayStrategy::FullJitter {
            base: Duration::from_millis(100),
            max: SECOND,
        };
        for _ in 0..200 {
            assert!(strategy.delay(1, Duration::ZERO) <= Duration::from_millis(200));
            assert!(strategy.delay(30, Duration::ZERO) <= SECOND);
        }
    }

    #[test]
    fn decorrelated_jitter_grows_from_the_previous_delay() {
        let strategy = DelayStrategy::DecorrelatedJitter {
            min: Duration::from_millis(100),
            max: SECOND,
        };
        let mut previous = Duration::ZERO;
        for attempt in 0..50 {
            let next = strategy.delay(attempt, previous);
            let bound = if previous.is_zero() {
                Duration::from_millis(100)
            } else {
                previous
            };
            assert!(next >= Duration::from_millis(100));
            assert!(next <= SECOND);
            assert!(next < Duration::from_millis(100) + bound * 3);
            previous = next;
        }
    }

    #[test]
    fn the_unlock_latch_outlives_every_retry() {
        let retry = Retry {
            attempts: 4,
            delay: DelayStrategy::EqualJitter {
                base: SECOND,
                max: 2 * SECOND,
            },
            timeout: 3 * SECOND,
        };
        assert_eq!(retry.unlock_latch_ttl(), Duration::from_secs(20));
    }

    #[test]
    fn reconnect_policies_follow_the_strategy() {
        assert_eq!(
            DelayStrategy::Constant(SECOND).reconnect_policy(),
            ReconnectPolicy::Constant {
                attempts: 0,
                max_attempts: 0,
                delay: 1000,
                jitter: 0,
            }
        );
        assert_eq!(
            DelayStrategy::EqualJitter {
                base: Duration::from_millis(100),
                max: Duration::from_secs(10),
            }
            .reconnect_policy(),
            ReconnectPolicy::Exponential {
                attempts: 0,
                max_attempts: 0,
                min_delay: 50,
                max_delay: 10_000,
                base: 2,
                jitter: 50,
            }
        );
    }
}
