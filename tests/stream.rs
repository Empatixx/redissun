mod common;

use common::{client, unique};
use redissun::{Object, Stream, StreamId};
use std::time::Duration;
use tokio::time::{sleep, Instant};

type Log = Stream<String, String, redissun::JsonCodec>;

async fn stream() -> Log {
    client().await.stream::<String, String>(unique("stream"))
}

fn pairs<'a>(items: &'a [(&'a str, &'a str)]) -> impl Iterator<Item = (&'a str, &'a str)> {
    items.iter().copied()
}

#[tokio::test]
async fn add_gives_increasing_ids_and_len_counts() {
    let log = stream().await;
    let first = log.add(pairs(&[("a", "1")])).await.unwrap();
    let second = log.add(pairs(&[("a", "2")])).await.unwrap();
    assert_ne!(first, second);
    assert!(first.as_str().contains('-'));
    assert_eq!(log.len().await.unwrap(), 2);
}

#[tokio::test]
async fn range_returns_entries_with_their_fields_in_order() {
    let log = stream().await;
    let id = log
        .add(pairs(&[("z", "last?"), ("a", "first?"), ("m", "mid")]))
        .await
        .unwrap();
    log.add(pairs(&[("n", "2")])).await.unwrap();
    let entries = log
        .range(&StreamId::min(), &StreamId::max(), None)
        .await
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].id, id);
    let keys: Vec<&str> = entries[0].fields.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, ["z", "a", "m"]);
    assert_eq!(entries[0].fields[1].1, "first?");

    let limited = log
        .range(&StreamId::min(), &StreamId::max(), Some(1))
        .await
        .unwrap();
    assert_eq!(limited.len(), 1);
    let reversed = log
        .rev_range(&StreamId::max(), &StreamId::min(), Some(1))
        .await
        .unwrap();
    assert_eq!(reversed[0].fields[0].0, "n");
}

#[tokio::test]
async fn max_len_trims_old_entries() {
    let log = stream().await;
    for index in 0..5 {
        log.add(pairs(&[("i", &index.to_string())]))
            .max_len(2)
            .await
            .unwrap();
    }
    assert_eq!(log.len().await.unwrap(), 2);
}

#[tokio::test]
async fn remove_and_trim_report_how_many() {
    let log = stream().await;
    let mut ids = Vec::new();
    for index in 0..6 {
        ids.push(log.add(pairs(&[("i", &index.to_string())])).await.unwrap());
    }
    assert_eq!(log.remove(&ids[..2]).await.unwrap(), 2);
    assert_eq!(log.remove(&ids[..2]).await.unwrap(), 0);
    assert_eq!(log.trim(2).await.unwrap(), 2);
    assert_eq!(log.len().await.unwrap(), 2);
}

#[tokio::test]
async fn read_returns_entries_after_an_id() {
    let log = stream().await;
    let first = log.add(pairs(&[("n", "1")])).await.unwrap();
    log.add(pairs(&[("n", "2")])).await.unwrap();
    let after = log.read(&first, None).await.unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].fields[0].1, "2");
    let none = log.read(&after[0].id, None).await.unwrap();
    assert!(none.is_empty());
}

#[tokio::test]
async fn read_wait_waits_for_a_new_entry() {
    let log = stream().await;
    let last = log.add(pairs(&[("n", "1")])).await.unwrap();
    let waiting = tokio::spawn({
        let log = log.clone();
        let last = last.clone();
        async move { log.read_wait(&last, None).await.unwrap() }
    });
    sleep(Duration::from_millis(200)).await;
    assert!(!waiting.is_finished());
    log.add(pairs(&[("n", "2")])).await.unwrap();
    let entries = waiting.await.unwrap();
    assert_eq!(entries[0].fields[0].1, "2");
}

#[tokio::test]
async fn read_wait_times_out_with_none() {
    let log = stream().await;
    let last = log.add(pairs(&[("n", "1")])).await.unwrap();
    let started = Instant::now();
    let outcome = log
        .read_wait(&last, None)
        .timeout(Duration::from_millis(300))
        .await
        .unwrap();
    assert!(outcome.is_none());
    assert!(started.elapsed() >= Duration::from_millis(250));
}

#[tokio::test]
async fn a_group_hands_each_entry_to_one_consumer() {
    let log = stream().await;
    assert!(log
        .create_group("workers", &StreamId::zero())
        .await
        .unwrap());
    assert!(!log
        .create_group("workers", &StreamId::zero())
        .await
        .unwrap());
    for index in 0..4 {
        log.add(pairs(&[("job", &index.to_string())]))
            .await
            .unwrap();
    }
    let a = log.read_group("workers", "a", Some(2)).await.unwrap();
    let b = log.read_group("workers", "b", Some(10)).await.unwrap();
    assert_eq!(a.len(), 2);
    assert_eq!(b.len(), 2);
    assert!(a.iter().all(|x| b.iter().all(|y| x.id != y.id)));
    assert!(log
        .read_group("workers", "a", None)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn ack_removes_entries_from_the_pending_list() {
    let log = stream().await;
    log.create_group("g", &StreamId::zero()).await.unwrap();
    for index in 0..3 {
        log.add(pairs(&[("job", &index.to_string())]))
            .await
            .unwrap();
    }
    let got = log.read_group("g", "c", None).await.unwrap();
    let summary = log.pending("g").await.unwrap();
    assert_eq!(summary.count, 3);
    assert_eq!(summary.first.as_ref(), Some(&got[0].id));
    assert_eq!(summary.last.as_ref(), Some(&got[2].id));
    assert_eq!(summary.consumers, [("c".to_string(), 3)]);

    let ids: Vec<StreamId> = got.iter().take(2).map(|e| e.id.clone()).collect();
    assert_eq!(log.ack("g", &ids).await.unwrap(), 2);
    assert_eq!(log.pending("g").await.unwrap().count, 1);
    let entries = log.pending_entries("g", 10).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].consumer, "c");
    assert_eq!(entries[0].deliveries, 1);
}

#[tokio::test]
async fn an_idle_entry_can_be_claimed_by_another_consumer() {
    let log = stream().await;
    log.create_group("g", &StreamId::zero()).await.unwrap();
    log.add(pairs(&[("job", "x")])).await.unwrap();
    let got = log.read_group("g", "dead", None).await.unwrap();
    sleep(Duration::from_millis(250)).await;
    let ids = vec![got[0].id.clone()];
    let none = log
        .claim("g", "alive", Duration::from_secs(60), &ids)
        .await
        .unwrap();
    assert!(none.is_empty());
    let claimed = log
        .claim("g", "alive", Duration::from_millis(100), &ids)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].fields[0].1, "x");
    assert_eq!(
        log.pending_entries("g", 10).await.unwrap()[0].consumer,
        "alive"
    );
}

#[tokio::test]
async fn auto_claim_takes_over_idle_entries() {
    let log = stream().await;
    log.create_group("g", &StreamId::zero()).await.unwrap();
    for index in 0..3 {
        log.add(pairs(&[("job", &index.to_string())]))
            .await
            .unwrap();
    }
    log.read_group("g", "dead", None).await.unwrap();
    sleep(Duration::from_millis(250)).await;
    let (next, claimed) = log
        .auto_claim(
            "g",
            "alive",
            Duration::from_millis(100),
            &StreamId::zero(),
            10,
        )
        .await
        .unwrap();
    assert_eq!(claimed.len(), 3);
    assert_eq!(next, StreamId::zero());
}

#[tokio::test]
async fn read_group_wait_waits_for_a_new_entry() {
    let log = stream().await;
    log.create_group("g", &StreamId::latest()).await.unwrap();
    let waiting = tokio::spawn({
        let log = log.clone();
        async move { log.read_group_wait("g", "c", None).await.unwrap() }
    });
    sleep(Duration::from_millis(200)).await;
    assert!(!waiting.is_finished());
    log.add(pairs(&[("job", "late")])).await.unwrap();
    let entries = waiting.await.unwrap();
    assert_eq!(entries[0].fields[0].1, "late");
}

#[tokio::test]
async fn destroy_group_and_object_methods() {
    let log = stream().await;
    log.create_group("g", &StreamId::zero()).await.unwrap();
    assert!(log.destroy_group("g").await.unwrap());
    assert!(!log.destroy_group("g").await.unwrap());
    assert!(log.exists().await.unwrap());
    assert!(log.del().await.unwrap());
}

#[tokio::test]
async fn an_invalid_id_is_a_config_error() {
    assert!("12-3".parse::<StreamId>().is_ok());
    assert!("12".parse::<StreamId>().is_ok());
    assert!("nonsense".parse::<StreamId>().is_err());
}
