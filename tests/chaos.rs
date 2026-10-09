mod common;

use common::{connect_with, redis_url, unique};
use redissun::{Client, RateType};
use std::collections::HashSet;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout, Instant};

const CLIENTS: usize = 4;
const TASKS_PER_CLIENT: usize = 4;
const RUN_FOR: Duration = Duration::from_secs(8);

async fn admin(parts: &[&str]) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let url = redis_url().await;
    let Ok(mut stream) = tokio::net::TcpStream::connect(url.trim_start_matches("redis://")).await
    else {
        return;
    };
    let mut request = format!("*{}\r\n", parts.len());
    for part in parts {
        request.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
    }
    if stream.write_all(request.as_bytes()).await.is_ok() {
        let mut reply = [0u8; 64];
        let _ = timeout(Duration::from_secs(2), stream.read(&mut reply)).await;
    }
}

static ONE_AT_A_TIME: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct Nemesis {
    stop: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<usize>,
}

impl Nemesis {
    fn start() -> Nemesis {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let calm = std::env::var("CHAOS_CALM").is_ok();
        let only = std::env::var("CHAOS_ONLY")
            .ok()
            .and_then(|v| v.parse::<u64>().ok());
        let handle = tokio::spawn(async move {
            let mut faults = 0;
            let mut step = 0u64;
            while !flag.load(Ordering::Relaxed) {
                sleep(Duration::from_millis(250 + (step * 137) % 400)).await;
                match if calm { 3 } else { only.unwrap_or(step % 4) } {
                    0 => admin(&["CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes"]).await,
                    1 => admin(&["CLIENT", "KILL", "TYPE", "pubsub"]).await,
                    2 => admin(&["CLIENT", "PAUSE", "300", "ALL"]).await,
                    _ => {}
                }
                faults += 1;
                step += 1;
            }
            faults
        });
        Nemesis { stop, handle }
    }

    async fn heal(self) -> usize {
        self.stop.store(true, Ordering::Relaxed);
        let faults = self.handle.await.unwrap();
        admin(&["CLIENT", "UNPAUSE"]).await;
        faults
    }
}

async fn clients() -> (tokio::sync::MutexGuard<'static, ()>, std::vec::Vec<Client>) {
    let turn = ONE_AT_A_TIME.lock().await;
    let mut clients = std::vec::Vec::new();
    for _ in 0..CLIENTS {
        clients.push(connect_with(|builder| builder.lock_lease(Duration::from_secs(3))).await);
    }
    (turn, clients)
}

async fn run_workers<F, Fut>(clients: &[Client], worker: F) -> usize
where
    F: Fn(Client, usize) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let nemesis = Nemesis::start();
    let mut workers = JoinSet::new();
    for (index, client) in clients.iter().enumerate() {
        for task in 0..TASKS_PER_CLIENT {
            workers.spawn(worker(client.clone(), index * TASKS_PER_CLIENT + task));
        }
    }
    let joined = timeout(RUN_FOR + Duration::from_secs(30), async {
        while let Some(result) = workers.join_next().await {
            result.unwrap();
        }
    })
    .await;
    let faults = nemesis.heal().await;
    joined.expect("workers did not finish: deadlock or lost wake-up");
    assert!(faults > 5, "the nemesis injected too few faults");
    faults
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(20);
    while !check().await {
        assert!(Instant::now() < deadline, "{what}");
        sleep(Duration::from_millis(100)).await;
    }
}

struct Exclusive {
    inside: AtomicUsize,
    entries: AtomicUsize,
    violations: AtomicUsize,
}

impl Exclusive {
    fn new() -> Arc<Exclusive> {
        Arc::new(Exclusive {
            inside: AtomicUsize::new(0),
            entries: AtomicUsize::new(0),
            violations: AtomicUsize::new(0),
        })
    }

    async fn critical_section(&self, limit: usize) {
        let before = self.inside.fetch_add(1, Ordering::SeqCst);
        if before >= limit {
            self.violations.fetch_add(1, Ordering::SeqCst);
        }
        self.entries.fetch_add(1, Ordering::SeqCst);
        sleep(Duration::from_millis(15)).await;
        self.inside.fetch_sub(1, Ordering::SeqCst);
    }

    fn check(&self, what: &str) {
        let entries = self.entries.load(Ordering::SeqCst);
        let violations = self.violations.load(Ordering::SeqCst);
        println!("{what}: {entries} critical sections");
        assert!(entries > 0, "{what}: no critical section ran");
        assert_eq!(
            violations, 0,
            "{what}: {violations} of {entries} entries overlapped"
        );
    }
}

#[tokio::test]
#[ignore = "runs a fault-injection workload for several seconds; run with --ignored"]
async fn lock_keeps_mutual_exclusion_under_faults() {
    let (_turn, clients) = clients().await;
    let name = unique("chaos-lock");
    let state = Exclusive::new();
    let deadline = Instant::now() + RUN_FOR;

    run_workers(&clients, |client, _| {
        let name = name.clone();
        let state = state.clone();
        async move {
            let lock = client.lock(name);
            while Instant::now() < deadline {
                if let Ok(Some(guard)) = lock.lock().timeout(Duration::from_secs(2)).await {
                    state.critical_section(1).await;
                    let _ = guard.unlock().await;
                }
            }
        }
    })
    .await;

    state.check("Lock");
    let lock = clients[0].lock(name);
    eventually("Lock stayed held after every holder left", || async {
        matches!(lock.is_locked().await, Ok(false))
    })
    .await;
}

#[tokio::test]
#[ignore = "runs a fault-injection workload for several seconds; run with --ignored"]
async fn fair_lock_keeps_mutual_exclusion_under_faults() {
    let (_turn, clients) = clients().await;
    let name = unique("chaos-fair");
    let state = Exclusive::new();
    let deadline = Instant::now() + RUN_FOR;

    run_workers(&clients, |client, _| {
        let name = name.clone();
        let state = state.clone();
        async move {
            let lock = client.fair_lock(name);
            while Instant::now() < deadline {
                if let Ok(Some(guard)) = lock.lock().timeout(Duration::from_secs(2)).await {
                    state.critical_section(1).await;
                    let _ = guard.unlock().await;
                }
            }
        }
    })
    .await;

    state.check("FairLock");
    let lock = clients[0].fair_lock(name);
    eventually("FairLock stayed held after every holder left", || async {
        matches!(lock.is_locked().await, Ok(false))
    })
    .await;
}

#[tokio::test]
#[ignore = "runs a fault-injection workload for several seconds; run with --ignored"]
async fn fenced_lock_tokens_only_grow_under_faults() {
    let (_turn, clients) = clients().await;
    let name = unique("chaos-fenced");
    let state = Exclusive::new();
    let tokens = Arc::new(Mutex::new(std::vec::Vec::new()));
    let deadline = Instant::now() + RUN_FOR;

    run_workers(&clients, |client, _| {
        let name = name.clone();
        let state = state.clone();
        let tokens = tokens.clone();
        async move {
            let lock = client.fenced_lock(name);
            while Instant::now() < deadline {
                if let Ok(Some(guard)) = lock.lock().timeout(Duration::from_secs(2)).await {
                    tokens.lock().unwrap().push(guard.fencing_token().unwrap());
                    state.critical_section(1).await;
                    let _ = guard.unlock().await;
                }
            }
        }
    })
    .await;

    state.check("FencedLock");
    let tokens = tokens.lock().unwrap();
    let decreasing = tokens.windows(2).filter(|pair| pair[1] <= pair[0]).count();
    assert_eq!(decreasing, 0, "fencing tokens went backwards: {tokens:?}");
}

#[tokio::test]
#[ignore = "runs a fault-injection workload for several seconds; run with --ignored"]
async fn rw_lock_never_mixes_a_writer_with_others_under_faults() {
    let (_turn, clients) = clients().await;
    let name = unique("chaos-rw");
    let readers = Arc::new(AtomicUsize::new(0));
    let writers = Arc::new(AtomicUsize::new(0));
    let violations = Arc::new(AtomicUsize::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    let deadline = Instant::now() + RUN_FOR;

    run_workers(&clients, |client, worker| {
        let name = name.clone();
        let (readers, writers, violations, writes) = (
            readers.clone(),
            writers.clone(),
            violations.clone(),
            writes.clone(),
        );
        async move {
            let lock = client.rw_lock(name);
            while Instant::now() < deadline {
                if worker.is_multiple_of(3) {
                    if let Ok(Some(guard)) = lock.write().timeout(Duration::from_secs(2)).await {
                        if writers.fetch_add(1, Ordering::SeqCst) > 0
                            || readers.load(Ordering::SeqCst) > 0
                        {
                            violations.fetch_add(1, Ordering::SeqCst);
                        }
                        writes.fetch_add(1, Ordering::SeqCst);
                        sleep(Duration::from_millis(10)).await;
                        writers.fetch_sub(1, Ordering::SeqCst);
                        let _ = guard.unlock().await;
                    }
                } else if let Ok(Some(guard)) = lock.read().timeout(Duration::from_secs(2)).await {
                    readers.fetch_add(1, Ordering::SeqCst);
                    if writers.load(Ordering::SeqCst) > 0 {
                        violations.fetch_add(1, Ordering::SeqCst);
                    }
                    sleep(Duration::from_millis(10)).await;
                    readers.fetch_sub(1, Ordering::SeqCst);
                    let _ = guard.unlock().await;
                }
            }
        }
    })
    .await;

    println!("RwLock: {} writes", writes.load(Ordering::SeqCst));
    assert!(writes.load(Ordering::SeqCst) > 0, "writers starved");
    assert_eq!(
        violations.load(Ordering::SeqCst),
        0,
        "a writer overlapped with others"
    );
    let lock = clients[0].rw_lock(name);
    eventually(
        "RwLock stayed write locked after every holder left",
        || async { matches!(lock.try_write().await, Ok(Some(_))) },
    )
    .await;
}

#[tokio::test]
#[ignore = "runs a fault-injection workload for several seconds; run with --ignored"]
async fn multi_lock_never_deadlocks_and_stays_exclusive_under_faults() {
    let (_turn, clients) = clients().await;
    let names: std::vec::Vec<String> = (0..5).map(|i| unique(&format!("chaos-multi{i}"))).collect();
    let states: std::vec::Vec<Arc<Exclusive>> = (0..5).map(|_| Exclusive::new()).collect();
    let deadline = Instant::now() + RUN_FOR;

    run_workers(&clients, |client, worker| {
        let names = names.clone();
        let states = states.clone();
        async move {
            let mut picked: std::vec::Vec<usize> = (0..3).map(|i| (worker + i * 2) % 5).collect();
            picked.sort_unstable();
            let multi = client
                .multi_lock(picked.iter().map(|&i| client.lock(names[i].clone())))
                .unwrap();
            while Instant::now() < deadline {
                if let Ok(Some(guard)) = multi.lock().timeout(Duration::from_secs(2)).await {
                    let sections = picked.iter().map(|&i| states[i].critical_section(1));
                    futures::future::join_all(sections).await;
                    let _ = guard.unlock().await;
                }
            }
        }
    })
    .await;

    for (index, state) in states.iter().enumerate() {
        state.check(&format!("MultiLock member {index}"));
    }
}

#[tokio::test]
#[ignore = "runs a fault-injection workload for several seconds; run with --ignored"]
async fn semaphore_never_overbooks_and_loses_permits_only_to_failed_calls_under_faults() {
    let (_turn, clients) = clients().await;
    let name = unique("chaos-semaphore");
    assert!(clients[0]
        .semaphore(name.clone())
        .try_set_permits(3)
        .await
        .unwrap());
    let state = Exclusive::new();
    let failed_calls = Arc::new(AtomicI64::new(0));
    let deadline = Instant::now() + RUN_FOR;

    run_workers(&clients, |client, worker| {
        let name = name.clone();
        let state = state.clone();
        let failed_calls = failed_calls.clone();
        async move {
            let semaphore = client.semaphore(name);
            let mut round = 0usize;
            while Instant::now() < deadline {
                round += 1;
                let wait = Duration::from_millis(if (worker + round).is_multiple_of(5) {
                    5
                } else {
                    2000
                });
                match semaphore.acquire(1).timeout(wait).await {
                    Ok(Some(permits)) => {
                        state.critical_section(3).await;
                        if permits.release().await.is_err() {
                            failed_calls.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    Ok(None) => {}
                    Err(_) => {
                        failed_calls.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        }
    })
    .await;

    state.check("Semaphore");
    sleep(Duration::from_millis(500)).await;
    let available = clients[0]
        .semaphore(name)
        .available_permits()
        .await
        .unwrap();
    let failed_calls = failed_calls.load(Ordering::SeqCst);
    assert!(
        available <= 3,
        "the semaphore has {available} permits, more than 3"
    );
    assert!(
        3 - available <= failed_calls,
        "{} permits are gone but only {failed_calls} calls failed",
        3 - available
    );
}

#[tokio::test]
#[ignore = "runs a fault-injection workload for several seconds; run with --ignored"]
async fn counter_never_loses_an_acknowledged_increment_under_faults() {
    let (_turn, clients) = clients().await;
    let name = unique("chaos-counter");
    let acknowledged = Arc::new(AtomicI64::new(0));
    let attempted = Arc::new(AtomicI64::new(0));
    let deadline = Instant::now() + RUN_FOR;

    run_workers(&clients, |client, _| {
        let name = name.clone();
        let (acknowledged, attempted) = (acknowledged.clone(), attempted.clone());
        async move {
            let counter = client.atomic_i64(name);
            while Instant::now() < deadline {
                attempted.fetch_add(1, Ordering::SeqCst);
                if counter.incr().await.is_ok() {
                    acknowledged.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    })
    .await;

    let value = clients[0].atomic_i64(name).get().await.unwrap();
    let acknowledged = acknowledged.load(Ordering::SeqCst);
    let attempted = attempted.load(Ordering::SeqCst);
    println!(
        "counter: {value}, acknowledged {acknowledged}, attempted {attempted}, applied twice at least {}",
        (value - attempted).max(0)
    );
    assert!(
        value >= acknowledged,
        "counter {value} lost acknowledged increments ({acknowledged})"
    );
}

#[tokio::test]
#[ignore = "runs a fault-injection workload for several seconds; run with --ignored"]
async fn rate_limiter_never_grants_more_than_its_rate_under_faults() {
    let (_turn, clients) = clients().await;
    let name = unique("chaos-limiter");
    clients[0]
        .rate_limiter(name.clone())
        .try_set_rate(RateType::Overall, 10, Duration::from_secs(1))
        .await
        .unwrap();
    let grants = Arc::new(Mutex::new(std::vec::Vec::new()));
    let deadline = Instant::now() + RUN_FOR;

    run_workers(&clients, |client, _| {
        let name = name.clone();
        let grants = grants.clone();
        async move {
            let limiter = client.rate_limiter(name);
            while Instant::now() < deadline {
                let asked = Instant::now();
                if let Ok(true) = limiter.try_acquire(1).await {
                    grants.lock().unwrap().push((asked, Instant::now()));
                }
                sleep(Duration::from_millis(5)).await;
            }
        }
    })
    .await;

    let mut grants = grants.lock().unwrap().clone();
    grants.sort();
    assert!(!grants.is_empty(), "no permit was granted");
    for (index, (asked, _)) in grants.iter().enumerate() {
        let window_end = *asked + Duration::from_secs(1);
        let inside = grants[index..]
            .iter()
            .filter(|(_, answered)| *answered < window_end)
            .count();
        assert!(inside <= 10, "{inside} permits granted within one second");
    }
}

#[tokio::test]
#[ignore = "runs a fault-injection workload for several seconds; run with --ignored"]
async fn queue_delivers_only_values_that_were_pushed_under_faults() {
    let (_turn, clients) = clients().await;
    let name = unique("chaos-queue");
    let pushed = Arc::new(Mutex::new(HashSet::new()));
    let attempted = Arc::new(Mutex::new(HashSet::new()));
    let popped = Arc::new(Mutex::new(std::vec::Vec::new()));
    let deadline = Instant::now() + RUN_FOR;

    run_workers(&clients, |client, worker| {
        let name = name.clone();
        let (pushed, popped, attempted) = (pushed.clone(), popped.clone(), attempted.clone());
        async move {
            let queue = client.vec_deque::<String>(name);
            let mut sequence = 0;
            while Instant::now() < deadline {
                if worker.is_multiple_of(2) {
                    sequence += 1;
                    let value = format!("{worker}-{sequence}");
                    attempted.lock().unwrap().insert(value.clone());
                    if queue.push_back(&value).await.is_ok() {
                        pushed.lock().unwrap().insert(value);
                    }
                    sleep(Duration::from_millis(5)).await;
                } else if let Ok(Some(value)) = queue
                    .pop_front_wait()
                    .timeout(Duration::from_millis(500))
                    .await
                {
                    popped.lock().unwrap().push(value);
                }
            }
        }
    })
    .await;

    let queue = clients[0].vec_deque::<String>(name);
    loop {
        match queue.pop_front().await {
            Ok(Some(value)) => popped.lock().unwrap().push(value),
            Ok(None) => break,
            Err(_) => sleep(Duration::from_millis(100)).await,
        }
    }
    let popped = popped.lock().unwrap();
    let pushed = pushed.lock().unwrap();
    let attempted = attempted.lock().unwrap();
    let unique_popped: HashSet<&String> = popped.iter().collect();
    let lost = pushed
        .iter()
        .filter(|value| !unique_popped.contains(value))
        .count();
    println!(
        "queue: pushed {}, delivered {}, delivered twice {}, lost in flight {lost}",
        pushed.len(),
        popped.len(),
        popped.len() - unique_popped.len()
    );
    let phantom = unique_popped
        .iter()
        .filter(|value| !attempted.contains(value.as_str()))
        .count();
    assert_eq!(phantom, 0, "a value nobody pushed was delivered");
    assert!(!pushed.is_empty(), "no push succeeded");
}
