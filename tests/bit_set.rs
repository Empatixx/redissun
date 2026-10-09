mod common;

use common::{client, unique};
use redissun::{Error, Object};

#[tokio::test]
async fn a_missing_bit_set_reads_as_all_zero() {
    let bits = client().await.bit_set(unique("bits"));
    assert!(!bits.get(0).await.unwrap());
    assert!(!bits.get(1_000_000).await.unwrap());
    assert_eq!(bits.count().await.unwrap(), 0);
    assert!(!bits.exists().await.unwrap());
}

#[tokio::test]
async fn set_returns_the_previous_bit() {
    let bits = client().await.bit_set(unique("bits"));
    assert!(!bits.set(7, true).await.unwrap());
    assert!(bits.set(7, true).await.unwrap());
    assert!(bits.get(7).await.unwrap());
    assert!(bits.set(7, false).await.unwrap());
    assert!(!bits.get(7).await.unwrap());
}

#[tokio::test]
async fn count_and_first_set() {
    let bits = client().await.bit_set(unique("bits"));
    assert_eq!(bits.first_set().await.unwrap(), None);
    for index in [3u64, 9, 100, 4_000] {
        bits.set(index, true).await.unwrap();
    }
    assert_eq!(bits.count().await.unwrap(), 4);
    assert_eq!(bits.first_set().await.unwrap(), Some(3));
    bits.set(3, false).await.unwrap();
    assert_eq!(bits.first_set().await.unwrap(), Some(9));
}

#[tokio::test]
async fn len_is_the_highest_set_bit_plus_one_rounded_to_bytes() {
    let bits = client().await.bit_set(unique("bits"));
    assert_eq!(bits.len().await.unwrap(), 0);
    bits.set(10, true).await.unwrap();
    assert_eq!(bits.len().await.unwrap(), 16);
}

#[tokio::test]
async fn an_index_beyond_the_redis_limit_is_a_config_error() {
    let bits = client().await.bit_set(unique("bits"));
    assert!(matches!(
        bits.set(u64::MAX, true).await,
        Err(Error::Config(_))
    ));
    assert!(matches!(bits.get(1 << 32).await, Err(Error::Config(_))));
}

#[tokio::test]
async fn object_methods_apply_to_the_key() {
    let bits = client().await.bit_set(unique("bits"));
    bits.set(1, true).await.unwrap();
    assert!(bits.exists().await.unwrap());
    assert!(bits.del().await.unwrap());
    assert!(!bits.get(1).await.unwrap());
}
