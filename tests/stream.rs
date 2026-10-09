mod common;

use common::{client, raw_command, unique};
use redissun::{Object, Stream, StreamEntry, StreamId};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::{sleep, Instant};

type Log = Stream<String, String, redissun::JsonCodec>;

async fn stream() -> Log {
    client().await.stream::<String, String>(unique("stream"))
}

fn map(entry: &StreamEntry<String, String>) -> HashMap<String, String> {
    entry.fields.iter().cloned().collect()
}

fn owned(items: &[(&str, &str)]) -> HashMap<String, String> {
    items
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn ids_of(entries: &[StreamEntry<String, String>]) -> Vec<StreamId> {
    entries.iter().map(|entry| entry.id.clone()).collect()
}

async fn add(log: &Log, items: &[(&str, &str)]) -> StreamId {
    log.add(pairs(items)).await.unwrap()
}

async fn two_consumers(log: &Log) -> [StreamId; 4] {
    add(log, &[("0", "0")]).await;
    log.create_group("testGroup", &StreamId::latest())
        .await
        .unwrap();
    let id1 = add(log, &[("1", "1")]).await;
    let id2 = add(log, &[("2", "2")]).await;
    let s = log
        .read_group("testGroup", "consumer1", None)
        .await
        .unwrap();
    assert_eq!(s.len(), 2);
    let id3 = add(log, &[("3", "33")]).await;
    let id4 = add(log, &[("4", "44")]).await;
    let s2 = log
        .read_group("testGroup", "consumer2", None)
        .await
        .unwrap();
    assert_eq!(s2.len(), 2);
    [id1, id2, id3, id4]
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
    let claimed = log
        .auto_claim(
            "g",
            "alive",
            Duration::from_millis(100),
            &StreamId::zero(),
            10,
        )
        .await
        .unwrap();
    assert_eq!(claimed.entries.len(), 3);
    assert_eq!(claimed.next, StreamId::zero());
    assert!(claimed.deleted.is_empty());
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
    assert!("18446744073709551616-0".parse::<StreamId>().is_err());
    assert!("1-*".parse::<StreamId>().is_err());
    assert!("1-18446744073709551615".parse::<StreamId>().is_ok());
}

#[tokio::test]
async fn read_wait_after_latest_returns_only_entries_added_later() {
    let log = stream().await;
    log.add(pairs(&[("n", "old")])).await.unwrap();
    let waiting = tokio::spawn({
        let log = log.clone();
        async move { log.read_wait(&StreamId::latest(), None).await.unwrap() }
    });
    sleep(Duration::from_millis(200)).await;
    assert!(!waiting.is_finished());
    log.add(pairs(&[("n", "new")])).await.unwrap();
    let entries = waiting.await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].fields[0].1, "new");
}

#[tokio::test]
async fn huge_counts_do_not_wrap_around() {
    let log = stream().await;
    log.add(pairs(&[("n", "1")])).await.unwrap();
    log.add(pairs(&[("n", "2")])).await.unwrap();
    let all = log
        .range(&StreamId::min(), &StreamId::max(), Some(usize::MAX))
        .await
        .unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(
        log.read(&StreamId::zero(), Some(usize::MAX))
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(log.trim(u64::MAX).await.unwrap(), 0);
    log.add(pairs(&[("n", "3")]))
        .max_len(u64::MAX)
        .await
        .unwrap();
    assert_eq!(log.len().await.unwrap(), 3);
}

#[tokio::test]
async fn test_type() {
    let log = stream().await;
    log.create_group("group", &StreamId::latest())
        .await
        .unwrap();
    add(&log, &[("key", "value")]).await;
    assert!(raw_command(&["TYPE", log.name()]).await.contains("stream"));
}

#[tokio::test]
async fn test_busy_group_is_not_retry_exception() {
    let log = stream().await;
    assert!(log
        .create_group("group", &StreamId::latest())
        .await
        .unwrap());
    assert!(!log
        .create_group("group", &StreamId::latest())
        .await
        .unwrap());
}

#[tokio::test]
async fn test_auto_claim() {
    let log = stream().await;
    let [_, _, id3, _] = two_consumers(&log).await;
    sleep(Duration::from_millis(5)).await;
    let res = log
        .auto_claim("testGroup", "consumer1", Duration::from_millis(1), &id3, 2)
        .await
        .unwrap();
    assert_eq!(res.entries.len(), 2);
    for entry in &res.entries {
        let fields = map(entry);
        assert!(fields.contains_key("3") || fields.contains_key("4"));
        assert!(fields.values().any(|v| v == "33" || v == "44"));
    }
}

#[tokio::test]
async fn test_auto_claim_deleted_ids() {
    let log = stream().await;
    add(&log, &[("1", "1")]).await;
    add(&log, &[("2", "2")]).await;
    log.create_group("testGroup", &StreamId::zero())
        .await
        .unwrap();
    assert!(log.create_consumer("testGroup", "consumer1").await.unwrap());
    log.read_group("testGroup", "consumer1", None)
        .await
        .unwrap();
    sleep(Duration::from_millis(5)).await;
    let res = log
        .auto_claim(
            "testGroup",
            "consumer1",
            Duration::from_millis(1),
            &StreamId::min(),
            2,
        )
        .await
        .unwrap();
    assert_eq!(res.entries.len(), 2);
    let claimed = ids_of(&res.entries);
    log.remove(&claimed).await.unwrap();
    let res1 = log
        .auto_claim(
            "testGroup",
            "consumer1",
            Duration::from_millis(1),
            &StreamId::min(),
            2,
        )
        .await
        .unwrap();
    assert_eq!(res1.deleted, claimed);
}

async fn assert_pending_idle(log: &Log, ids: &[StreamId; 4]) {
    let list = log
        .pending_entries("testGroup", 10)
        .range(&StreamId::min(), &StreamId::max())
        .min_idle(Duration::from_millis(1))
        .await
        .unwrap();
    assert_eq!(list.len(), 4);
    for entry in &list {
        assert!(ids.contains(&entry.id));
        assert!(entry.consumer == "consumer1" || entry.consumer == "consumer2");
        assert_eq!(entry.deliveries, 1);
    }
    let list2 = log
        .pending_entries("testGroup", 10)
        .consumer("consumer1")
        .min_idle(Duration::from_millis(1))
        .await
        .unwrap();
    assert_eq!(list2.len(), 2);
    for entry in &list2 {
        assert!(ids[..2].contains(&entry.id));
        assert_eq!(entry.consumer, "consumer1");
        assert_eq!(entry.deliveries, 1);
    }
}

#[tokio::test]
async fn test_pending_idle() {
    let log = stream().await;
    let ids = two_consumers(&log).await;
    sleep(Duration::from_millis(5)).await;
    assert_pending_idle(&log, &ids).await;
}

#[tokio::test]
async fn test_pending_idle2() {
    let log = stream().await;
    let ids = two_consumers(&log).await;
    sleep(Duration::from_millis(5)).await;
    assert_pending_idle(&log, &ids).await;
}

#[tokio::test]
async fn test_trim() {
    let log = stream().await;
    add(&log, &[("0", "0")]).await;
    add(&log, &[("1", "1")]).await;
    add(&log, &[("2", "2")]).await;
    assert_eq!(log.trim(2).await.unwrap(), 1);
    let other = stream().await;
    assert_eq!(other.trim(0).await.unwrap(), 0);
}

#[tokio::test]
async fn trim_min_id_and_non_strict() {
    let log = stream().await;
    let mut ids = Vec::new();
    for index in 0..4 {
        ids.push(add(&log, &[("i", &index.to_string())]).await);
    }
    assert_eq!(log.trim_min_id(&ids[2]).await.unwrap(), 2);
    let dropped = log.trim(0).non_strict().limit(100).await.unwrap();
    assert_eq!(log.len().await.unwrap(), 2 - dropped);
}

#[tokio::test]
async fn test_pending_empty() {
    let log = stream().await;
    log.create_group("testGroup", &StreamId::latest())
        .await
        .unwrap();
    let result = log.pending("testGroup").await.unwrap();
    assert_eq!(result.count, 0);
    assert!(result.first.is_none());
    assert!(result.last.is_none());
    assert!(result.consumers.is_empty());
}

#[tokio::test]
async fn test_update_group_message_id() {
    let log = stream().await;
    let id = add(&log, &[("0", "0")]).await;
    log.create_group("testGroup", &StreamId::latest())
        .await
        .unwrap();
    add(&log, &[("1", "1")]).await;
    add(&log, &[("2", "2")]).await;
    let s = log
        .read_group("testGroup", "consumer1", None)
        .await
        .unwrap();
    assert_eq!(s.len(), 2);
    log.update_group_message_id("testGroup", &id).await.unwrap();
    let s2 = log
        .read_group("testGroup", "consumer2", None)
        .await
        .unwrap();
    assert_eq!(s2.len(), 2);
}

#[tokio::test]
async fn test_remove_consumer() {
    let log = stream().await;
    add(&log, &[("0", "0")]).await;
    log.create_group("testGroup", &StreamId::latest())
        .await
        .unwrap();
    add(&log, &[("1", "1")]).await;
    add(&log, &[("2", "2")]).await;
    let s = log
        .read_group("testGroup", "consumer1", None)
        .await
        .unwrap();
    assert_eq!(s.len(), 2);
    assert_eq!(
        log.remove_consumer("testGroup", "consumer1").await.unwrap(),
        2
    );
    assert_eq!(
        log.remove_consumer("testGroup", "consumer2").await.unwrap(),
        0
    );
}

#[tokio::test]
async fn test_remove_group() {
    let log = stream().await;
    add(&log, &[("0", "0")]).await;
    log.create_group("testGroup", &StreamId::latest())
        .await
        .unwrap();
    add(&log, &[("1", "1")]).await;
    add(&log, &[("2", "2")]).await;
    log.destroy_group("testGroup").await.unwrap();
    assert!(log
        .read_group("testGroup", "consumer1", None)
        .await
        .is_err());
}

#[tokio::test]
async fn test_remove_messages() {
    let log = stream().await;
    let id1 = add(&log, &[("0", "0")]).await;
    let id2 = add(&log, &[("1", "1")]).await;
    assert_eq!(log.len().await.unwrap(), 2);
    assert_eq!(log.remove(&[id1, id2]).await.unwrap(), 2);
    assert_eq!(log.len().await.unwrap(), 0);
}

#[tokio::test]
async fn test_claim_remove() {
    let log = stream().await;
    let [_, _, id3, id4] = two_consumers(&log).await;
    log.remove(std::slice::from_ref(&id3)).await.unwrap();
    sleep(Duration::from_millis(2)).await;
    let res = log
        .claim(
            "testGroup",
            "consumer1",
            Duration::from_millis(1),
            &[id3, id4.clone()],
        )
        .await
        .unwrap();
    assert_eq!(ids_of(&res), [id4]);
}

#[tokio::test]
async fn test_claim() {
    let log = stream().await;
    let [_, _, id3, id4] = two_consumers(&log).await;
    sleep(Duration::from_millis(5)).await;
    let res = log
        .claim(
            "testGroup",
            "consumer1",
            Duration::from_millis(1),
            &[id3.clone(), id4.clone()],
        )
        .await
        .unwrap();
    assert_eq!(ids_of(&res), [id3, id4]);
    for entry in &res {
        let fields = map(entry);
        assert!(fields.contains_key("3") || fields.contains_key("4"));
        assert!(fields.values().any(|v| v == "33" || v == "44"));
    }
}

#[tokio::test]
async fn test_auto_claim_ids() {
    let log = stream().await;
    let [_, _, id3, id4] = two_consumers(&log).await;
    sleep(Duration::from_millis(5)).await;
    let (next, ids) = log
        .fast_auto_claim("testGroup", "consumer1", Duration::from_millis(1), &id3, 10)
        .await
        .unwrap();
    assert_eq!(next, StreamId::new(0, 0));
    assert_eq!(ids, [id3, id4]);
}

#[tokio::test]
async fn test_claim_ids() {
    let log = stream().await;
    let [_, _, id3, id4] = two_consumers(&log).await;
    sleep(Duration::from_millis(5)).await;
    let res = log
        .fast_claim(
            "testGroup",
            "consumer1",
            Duration::from_millis(1),
            &[id3.clone(), id4.clone()],
        )
        .await
        .unwrap();
    assert_eq!(res, [id3, id4]);
}

async fn assert_pending(log: &Log, ids: &[StreamId; 4]) {
    let pi = log.pending("testGroup").await.unwrap();
    assert_eq!(pi.first.as_ref(), Some(&ids[0]));
    assert_eq!(pi.last.as_ref(), Some(&ids[3]));
    assert_eq!(pi.count, 4);
    let names: Vec<&str> = pi.consumers.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(names, ["consumer1", "consumer2"]);
    let list = log
        .pending_entries("testGroup", 10)
        .range(&StreamId::min(), &StreamId::max())
        .await
        .unwrap();
    assert_eq!(list.len(), 4);
    for entry in &list {
        assert!(ids.contains(&entry.id));
        assert_eq!(entry.deliveries, 1);
    }
    let list2 = log
        .pending_entries("testGroup", 10)
        .consumer("consumer1")
        .await
        .unwrap();
    assert_eq!(list2.len(), 2);
    for entry in &list2 {
        assert!(ids[..2].contains(&entry.id));
        assert_eq!(entry.consumer, "consumer1");
    }
}

#[tokio::test]
async fn test_pending() {
    let log = stream().await;
    let ids = two_consumers(&log).await;
    assert_pending(&log, &ids).await;
}

#[tokio::test]
async fn test_pending2() {
    let log = stream().await;
    let ids = two_consumers(&log).await;
    assert_pending(&log, &ids).await;
}

#[tokio::test]
async fn test_pending_range() {
    let log = stream().await;
    add(&log, &[("0", "0")]).await;
    log.create_group("testGroup", &StreamId::latest())
        .await
        .unwrap();
    let id1 = add(&log, &[("11", "12")]).await;
    let id2 = add(&log, &[("21", "22")]).await;
    let s = log
        .read_group("testGroup", "consumer1", None)
        .await
        .unwrap();
    assert_eq!(s.len(), 2);
    let pres = log
        .pending_entries("testGroup", 10)
        .messages()
        .await
        .unwrap();
    assert_eq!(ids_of(&pres), [id1.clone(), id2.clone()]);
    assert_eq!(map(&pres[0]), owned(&[("11", "12")]));
    assert_eq!(map(&pres[1]), owned(&[("21", "22")]));
    let pres2 = log
        .pending_entries("testGroup", 10)
        .consumer("consumer1")
        .range(&StreamId::min(), &StreamId::max())
        .messages()
        .await
        .unwrap();
    assert_eq!(ids_of(&pres2), [id1, id2]);
    let pres3 = log
        .pending_entries("testGroup", 10)
        .consumer("consumer2")
        .messages()
        .await
        .unwrap();
    assert!(pres3.is_empty());
}

#[tokio::test]
async fn test_ack() {
    let log = stream().await;
    add(&log, &[("0", "0")]).await;
    log.create_group("testGroup", &StreamId::latest())
        .await
        .unwrap();
    let id1 = add(&log, &[("1", "1")]).await;
    let id2 = add(&log, &[("2", "2")]).await;
    let s = log
        .read_group("testGroup", "consumer1", None)
        .await
        .unwrap();
    assert_eq!(s.len(), 2);
    assert_eq!(log.ack("testGroup", &[id1, id2]).await.unwrap(), 2);
}

#[tokio::test]
async fn test_read_group_blocking() {
    let log = stream().await;
    let id0 = add(&log, &[("0", "0")]).await;
    log.create_group("testGroup", &id0).await.unwrap();
    add(&log, &[("1", "1")]).await;
    add(&log, &[("2", "2")]).await;
    add(&log, &[("3", "3")]).await;
    let s = log
        .read_group_wait("testGroup", "consumer1", Some(3))
        .timeout(Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s.len(), 3);
    assert!(["1", "2", "3"].contains(&s[0].fields[0].0.as_str()));
}

#[tokio::test]
async fn test_create_empty() {
    let log = stream().await;
    log.create_group("testGroup", &StreamId::zero())
        .await
        .unwrap();
    add(&log, &[("1", "2")]).await;
    let s = log
        .read_group("testGroup", "consumer1", None)
        .await
        .unwrap();
    assert_eq!(s.len(), 1);
}

#[tokio::test]
async fn test_read_group() {
    let log = stream().await;
    let id0 = add(&log, &[("0", "0")]).await;
    log.create_group("testGroup", &id0).await.unwrap();
    add(&log, &[("1", "1")]).await;
    add(&log, &[("2", "2")]).await;
    add(&log, &[("3", "3")]).await;
    let s = log
        .read_group("testGroup", "consumer1", None)
        .await
        .unwrap();
    assert_eq!(s.len(), 3);
    add(&log, &[("1", "1")]).await;
    add(&log, &[("2", "2")]).await;
    add(&log, &[("3", "3")]).await;
    let s1 = log
        .read_group("testGroup", "consumer1", Some(1))
        .await
        .unwrap();
    assert_eq!(s1.len(), 1);
    let id = add(&log, &[("1", "1")]).await;
    add(&log, &[("2", "2")]).await;
    add(&log, &[("3", "3")]).await;
    let s2 = log
        .read_group("testGroup", "consumer1", None)
        .after(id)
        .await
        .unwrap();
    assert!(s2.is_empty());
}

#[tokio::test]
async fn read_group_after_rereads_own_pending_entries_and_no_ack_skips_them() {
    let log = stream().await;
    log.create_group("g", &StreamId::zero()).await.unwrap();
    add(&log, &[("1", "1")]).await;
    let first = log.read_group("g", "c", None).await.unwrap();
    let again = log
        .read_group("g", "c", None)
        .after(StreamId::zero())
        .await
        .unwrap();
    assert_eq!(ids_of(&again), ids_of(&first));
    add(&log, &[("2", "2")]).await;
    assert_eq!(
        log.read_group("g", "c", None).no_ack().await.unwrap().len(),
        1
    );
    assert_eq!(log.pending("g").await.unwrap().count, 1);
}

#[tokio::test]
async fn test_autogenerate_stream_sequence_id() {
    let log = stream().await;
    assert_eq!(log.len().await.unwrap(), 0);
    let id = StreamId::auto_sequence(1);
    log.add(pairs(&[("test", "value1")]))
        .id(id.clone())
        .await
        .unwrap();
    log.add(pairs(&[("test", "value2")])).id(id).await.unwrap();
    let r = log
        .range(&StreamId::min(), &StreamId::max(), Some(10))
        .await
        .unwrap();
    assert_eq!(ids_of(&r), [StreamId::new(1, 0), StreamId::new(1, 1)]);
    assert_eq!(map(&r[0]), owned(&[("test", "value1")]));
    assert_eq!(map(&r[1]), owned(&[("test", "value2")]));
}

const ENTRIES1: [(&str, &str); 2] = [("1", "11"), ("3", "31")];
const ENTRIES2: [(&str, &str); 2] = [("5", "55"), ("7", "77")];

async fn two_entries(log: &Log) {
    assert_eq!(log.len().await.unwrap(), 0);
    log.add(pairs(&ENTRIES1))
        .id(StreamId::new(1, 0))
        .max_len(1)
        .non_strict()
        .await
        .unwrap();
    assert_eq!(log.len().await.unwrap(), 1);
    log.add(pairs(&ENTRIES2))
        .id(StreamId::new(2, 0))
        .max_len(1)
        .non_strict()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_range_reversed() {
    let log = stream().await;
    two_entries(&log).await;
    let r2 = log
        .rev_range(&StreamId::max(), &StreamId::min(), Some(10))
        .await
        .unwrap();
    assert_eq!(ids_of(&r2), [StreamId::new(2, 0), StreamId::new(1, 0)]);
    assert_eq!(map(&r2[1]), owned(&ENTRIES1));
    assert_eq!(map(&r2[0]), owned(&ENTRIES2));
}

#[tokio::test]
async fn test_range() {
    let log = stream().await;
    two_entries(&log).await;
    let r = log
        .range(&StreamId::new(0, 0), &StreamId::new(1, 0), Some(10))
        .await
        .unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(map(&r[0]), owned(&ENTRIES1));
    let r2 = log
        .range(&StreamId::min(), &StreamId::max(), Some(10))
        .await
        .unwrap();
    assert_eq!(ids_of(&r2), [StreamId::new(1, 0), StreamId::new(2, 0)]);
    assert_eq!(map(&r2[1]), owned(&ENTRIES2));
}

#[tokio::test]
async fn test_range_reversed2() {
    let log = stream().await;
    two_entries(&log).await;
    let r2 = log
        .rev_range(
            &StreamId::max().exclusive(),
            &StreamId::min().exclusive(),
            Some(10),
        )
        .await
        .unwrap();
    assert_eq!(ids_of(&r2), [StreamId::new(2, 0), StreamId::new(1, 0)]);
}

#[tokio::test]
async fn test_range2() {
    let log = stream().await;
    two_entries(&log).await;
    let r = log
        .range(&StreamId::new(0, 0), &StreamId::new(1, 0), Some(10))
        .await
        .unwrap();
    assert_eq!(r.len(), 1);
    let r3 = log
        .range(&StreamId::new(1, 0).exclusive(), &StreamId::new(2, 0), None)
        .await
        .unwrap();
    assert_eq!(r3.len(), 1);
    assert_eq!(map(&r3[0]), owned(&ENTRIES2));
}

#[tokio::test]
async fn test_poll() {
    let log = stream().await;
    let writer = log.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(2)).await;
        writer
            .add(pairs(&ENTRIES1))
            .id(StreamId::new(1, 0))
            .await
            .unwrap();
    });
    let started = Instant::now();
    let s = log
        .read_wait(&StreamId::new(0, 0), Some(2))
        .timeout(Duration::from_secs(4))
        .await
        .unwrap()
        .unwrap();
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(1900), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(4), "{elapsed:?}");
    assert_eq!(s.len(), 1);
    assert_eq!(s[0].id, StreamId::new(1, 0));
    assert_eq!(map(&s[0]), owned(&ENTRIES1));
}

#[tokio::test]
async fn test_size() {
    let log = stream().await;
    two_entries(&log).await;
    assert_eq!(log.len().await.unwrap(), 2);
}

const ENTRIES3: [(&str, &str); 2] = [("15", "05"), ("17", "07")];

#[tokio::test]
async fn test_read_multi() {
    let log = stream().await;
    two_entries(&log).await;
    log.add(pairs(&ENTRIES3))
        .id(StreamId::new(3, 0))
        .max_len(1)
        .non_strict()
        .await
        .unwrap();
    let result = log.read(&StreamId::new(0, 0), Some(10)).await.unwrap();
    assert_eq!(result.len(), 3);
    assert_eq!(map(&result[0]), owned(&ENTRIES1));
    assert_eq!(map(&result[1]), owned(&ENTRIES2));
    assert_eq!(map(&result[2]), owned(&ENTRIES3));
}

#[tokio::test]
async fn test_read_single() {
    let log = stream().await;
    log.add(pairs(&ENTRIES1))
        .id(StreamId::new(1, 0))
        .await
        .unwrap();
    let result = log.read(&StreamId::new(0, 0), Some(10)).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].id, StreamId::new(1, 0));
    assert_eq!(map(&result[0]), owned(&ENTRIES1));
}

#[tokio::test]
async fn test_read_empty() {
    let log = stream().await;
    assert!(log
        .read(&StreamId::new(0, 0), Some(10))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn test_read_empty_async() {
    let log = stream().await;
    let after = StreamId::new(0, 0);
    let pending = log.read(&after, Some(10));
    assert!(pending.await.unwrap().is_empty());
}

#[tokio::test]
async fn test_read_group_empty_async() {
    let log = stream().await;
    log.create_group("testGroup", &StreamId::latest())
        .await
        .unwrap();
    add(&log, &[("1", "1")]).await;
    log.read_group("testGroup", "consumer1", None)
        .await
        .unwrap();
    let result = log
        .read_group("testGroup", "consumer1", None)
        .await
        .unwrap();
    assert!(result.is_empty());
}

#[tokio::test]
async fn test_add() {
    let log = stream().await;
    let s = add(&log, &[("12", "33")]).await;
    let (millis, sequence) = s.as_str().split_once('-').unwrap();
    assert!(millis.parse::<u64>().is_ok());
    assert!(sequence.parse::<u64>().is_ok());
    assert_eq!(log.len().await.unwrap(), 1);
}

#[tokio::test]
async fn test_add_all() {
    let log = stream().await;
    assert_eq!(log.len().await.unwrap(), 0);
    let id = StreamId::new(12, 42);
    log.add(pairs(&[("6", "61"), ("4", "41")]))
        .id(id.clone())
        .max_len(10)
        .non_strict()
        .await
        .unwrap();
    assert_eq!(log.len().await.unwrap(), 1);
    let res = log.read(&StreamId::new(10, 42), None).await.unwrap();
    assert_eq!(res[0].id, id);
    assert_eq!(res[0].fields.len(), 2);
    log.add(pairs(&ENTRIES1))
        .id(StreamId::new(i64::MAX as u64, 0))
        .max_len(1)
        .non_strict()
        .await
        .unwrap();
    assert_eq!(log.len().await.unwrap(), 2);
}

#[tokio::test]
async fn test_stream_consumers() {
    let log = stream().await;
    let id1 = StreamId::new(12, 44);
    log.create_group("testGroup", &id1).await.unwrap();
    for (k, v) in [("1", "1"), ("2", "2"), ("3", "3")] {
        add(&log, &[(k, v)]).await;
    }
    log.create_group("testGroup2", &id1).await.unwrap();
    for (k, v) in [("1", "1"), ("2", "2"), ("3", "3")] {
        add(&log, &[(k, v)]).await;
    }
    let read = log
        .read_group("testGroup", "consumer1", None)
        .await
        .unwrap();
    assert_eq!(read.len(), 6);
    let s1 = log.consumers("testGroup").await.unwrap();
    assert_eq!(s1.len(), 1);
    assert_eq!(s1[0].name, "consumer1");
    assert_eq!(s1[0].pending, 6);
    assert!(s1[0].idle < Duration::from_secs(5));
    let read2 = log
        .read_group("testGroup2", "consumer2", None)
        .await
        .unwrap();
    assert_eq!(read2.len(), 6);
    let s2 = log.consumers("testGroup2").await.unwrap();
    assert_eq!(s2.len(), 1);
    assert_eq!(s2[0].name, "consumer2");
    assert_eq!(s2[0].pending, 6);
}

#[tokio::test]
async fn test_stream_groups_info() {
    let log = stream().await;
    log.add(pairs(&[("6", "61"), ("4", "41")]))
        .id(StreamId::new(12, 42))
        .await
        .unwrap();
    assert!(log.groups().await.unwrap().is_empty());
    let id1 = StreamId::new(12, 44);
    log.create_group("testGroup", &id1).await.unwrap();
    for (k, v) in [("1", "1"), ("2", "2"), ("3", "3")] {
        add(&log, &[(k, v)]).await;
    }
    log.create_group("testGroup2", &id1).await.unwrap();
    let s2 = log.groups().await.unwrap();
    assert_eq!(s2.len(), 2);
    assert_eq!(s2[0].name, "testGroup");
    assert_eq!(s2[0].consumers, 0);
    assert_eq!(s2[0].pending, 0);
    assert_eq!(s2[0].last_delivered_id, id1);
    assert_eq!(s2[1].name, "testGroup2");
    assert_eq!(s2[1].last_delivered_id, id1);
}

#[tokio::test]
async fn test_stream_info_empty() {
    let log = stream().await;
    log.create_group("testGroup", &StreamId::new(12, 44))
        .await
        .unwrap();
    let info = log.info().await.unwrap();
    assert_eq!(info.length, 0);
    assert_eq!(info.groups, 1);
    assert!(info.first_entry.is_none());
}

#[tokio::test]
async fn test_stream_info() {
    let log = stream().await;
    let id = StreamId::new(12, 42);
    log.add(pairs(&[("6", "61"), ("4", "41")]))
        .id(id.clone())
        .await
        .unwrap();
    let last_id = StreamId::new(12, 43);
    log.add(pairs(&[("10", "52"), ("44", "89")]))
        .id(last_id.clone())
        .await
        .unwrap();
    let info = log.info().await.unwrap();
    assert_eq!(info.length, 2);
    assert_eq!(info.radix_tree_keys, 1);
    assert_eq!(info.radix_tree_nodes, 2);
    assert_eq!(info.last_generated_id, last_id);
    let first = info.first_entry.unwrap();
    assert_eq!(first.id, id);
    assert_eq!(map(&first), owned(&[("6", "61"), ("4", "41")]));
    let last = info.last_entry.unwrap();
    assert_eq!(last.id, last_id);
    assert_eq!(map(&last), owned(&[("10", "52"), ("44", "89")]));
}

#[tokio::test]
async fn add_is_sent_once_and_read_wait_zero_timeout_does_not_block() {
    let log = stream().await;
    let started = Instant::now();
    let none = log
        .read_wait(&StreamId::latest(), None)
        .timeout(Duration::ZERO)
        .await
        .unwrap();
    assert!(none.is_none());
    assert!(started.elapsed() < Duration::from_secs(1));
}
