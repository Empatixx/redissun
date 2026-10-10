use crate::common::{client, unique};
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
async fn size_is_the_string_length_in_bits_and_length_the_highest_set_bit_plus_one() {
    let bits = client().await.bit_set(unique("bits"));
    assert_eq!(bits.size().await.unwrap(), 0);
    assert_eq!(bits.length().await.unwrap(), 0);
    bits.set(10, true).await.unwrap();
    assert_eq!(bits.size().await.unwrap(), 16);
    assert_eq!(bits.length().await.unwrap(), 11);
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

#[tokio::test]
async fn test_unsigned() {
    let bs = client().await.bit_set(unique("testUnsigned"));
    assert_eq!(bs.set_unsigned(8, 1, 120).await.unwrap(), 0);
    assert_eq!(bs.increment_and_get_unsigned(8, 1, 1).await.unwrap(), 121);
    assert_eq!(bs.get_unsigned(8, 1).await.unwrap(), 121);
}

#[tokio::test]
async fn test_signed() {
    let bs = client().await.bit_set(unique("testSigned"));
    assert_eq!(bs.set_signed(8, 1, -120).await.unwrap(), 0);
    assert_eq!(bs.increment_and_get_signed(8, 1, 1).await.unwrap(), -119);
    assert_eq!(bs.get_signed(8, 1).await.unwrap(), -119);
}

#[tokio::test]
async fn bitfield_sizes_are_checked() {
    let bs = client().await.bit_set(unique("bits"));
    assert!(matches!(bs.get_signed(65, 0).await, Err(Error::Config(_))));
    assert!(matches!(
        bs.get_unsigned(64, 0).await,
        Err(Error::Config(_))
    ));
    assert!(matches!(bs.get_signed(0, 0).await, Err(Error::Config(_))));
}

#[tokio::test]
async fn test_increment() {
    let bs2 = client().await.bit_set(unique("testbitset1"));
    assert_eq!(bs2.set_signed(8, 2, 12).await.unwrap(), 0);
    assert_eq!(bs2.get_signed(8, 2).await.unwrap(), 12);
    assert_eq!(bs2.increment_and_get_signed(8, 2, 12).await.unwrap(), 24);
    assert_eq!(bs2.get_signed(8, 2).await.unwrap(), 24);
}

#[tokio::test]
async fn test_set_get_number() {
    let client = client().await;
    let bs = client.bit_set(unique("testbitset"));
    assert_eq!(bs.set_signed(64, 2, 12).await.unwrap(), 0);
    assert_eq!(bs.get_signed(64, 2).await.unwrap(), 12);

    let bs2 = client.bit_set(unique("testbitset1"));
    assert_eq!(bs2.set_signed(8, 2, 12).await.unwrap(), 0);
    assert_eq!(bs2.get_signed(8, 2).await.unwrap(), 12);

    let bs3 = client.bit_set(unique("testbitset2"));
    assert_eq!(bs3.set_signed(16, 2, 2312).await.unwrap(), 0);
    assert_eq!(bs3.get_signed(16, 2).await.unwrap(), 2312);

    let bs4 = client.bit_set(unique("testbitset3"));
    assert_eq!(bs4.set_signed(32, 2, 323241).await.unwrap(), 0);
    assert_eq!(bs4.get_signed(32, 2).await.unwrap(), 323241);
}

#[tokio::test]
async fn test_index_range() {
    let bs = client().await.bit_set(unique("testbitset"));
    let top_index = i32::MAX as u64 * 2;
    assert!(!bs.get(top_index).await.unwrap());
    bs.set(top_index, true).await.unwrap();
    assert!(bs.get(top_index).await.unwrap());
    bs.del().await.unwrap();
}

#[tokio::test]
async fn test_length() {
    let bs = client().await.bit_set(unique("testbitset"));
    bs.set_range(0..5, true).await.unwrap();
    bs.set_range(0..1, false).await.unwrap();
    assert_eq!(bs.length().await.unwrap(), 5);

    bs.clear().await.unwrap();
    bs.set(28, true).await.unwrap();
    bs.set(31, true).await.unwrap();
    assert_eq!(bs.length().await.unwrap(), 32);

    bs.clear().await.unwrap();
    bs.set(3, true).await.unwrap();
    bs.set(7, true).await.unwrap();
    assert_eq!(bs.length().await.unwrap(), 8);

    bs.clear().await.unwrap();
    bs.set(3, true).await.unwrap();
    bs.set(120, true).await.unwrap();
    bs.set(121, true).await.unwrap();
    assert_eq!(bs.length().await.unwrap(), 122);

    bs.clear().await.unwrap();
    bs.set(0, true).await.unwrap();
    assert_eq!(bs.length().await.unwrap(), 1);

    bs.clear().await.unwrap();
    bs.set_range(0..2, true).await.unwrap();
    bs.set(9, true).await.unwrap();
    bs.set_range(9..10, false).await.unwrap();
    assert_eq!(bs.length().await.unwrap(), 2);

    bs.clear().await.unwrap();
    bs.set(7, true).await.unwrap();
    bs.set(9, true).await.unwrap();
    bs.set_range(9..10, false).await.unwrap();
    assert_eq!(bs.length().await.unwrap(), 8);
}

#[tokio::test]
async fn test_clear() {
    let bs = client().await.bit_set(unique("testbitset"));
    bs.set_range(0..8, true).await.unwrap();
    bs.set_range(0..3, false).await.unwrap();
    assert_eq!(bs.ones().await.unwrap(), [3, 4, 5, 6, 7]);
}

#[tokio::test]
async fn test_not() {
    let bs = client().await.bit_set(unique("testbitset"));
    bs.set(3, true).await.unwrap();
    bs.set(5, true).await.unwrap();
    assert_eq!(bs.not().await.unwrap(), 1);
    assert_eq!(bs.ones().await.unwrap(), [0, 1, 2, 4, 6, 7]);
}

#[tokio::test]
async fn test_set() {
    let client = client().await;
    let name = unique("testbitset");
    let bs = client.bit_set(name.clone());
    assert!(!bs.set(3, true).await.unwrap());
    assert!(!bs.set(5, true).await.unwrap());
    assert!(bs.set(5, true).await.unwrap());
    assert_eq!(bs.ones().await.unwrap(), [3, 5]);

    bs.replace(&[1, 10]).await.unwrap();
    let bs = client.bit_set(name);
    assert_eq!(bs.ones().await.unwrap(), [1, 10]);

    let bs2 = client.bit_set(unique("testbitset2"));
    bs2.set_many(&[1, 3, 5, 7], true).await.unwrap();
    assert_eq!(bs2.ones().await.unwrap(), [1, 3, 5, 7]);
    bs2.set_many(&[3, 5], false).await.unwrap();
    assert_eq!(bs2.ones().await.unwrap(), [1, 7]);
}

#[tokio::test]
async fn test_set_get() {
    let bitset = client().await.bit_set(unique("testbitset"));
    assert_eq!(bitset.count().await.unwrap(), 0);
    assert_eq!(bitset.size().await.unwrap(), 0);

    assert!(!bitset.set(10, true).await.unwrap());
    assert!(!bitset.set(31, true).await.unwrap());
    assert!(!bitset.get(0).await.unwrap());
    assert!(bitset.get(31).await.unwrap());
    assert!(bitset.get(10).await.unwrap());
    assert_eq!(bitset.count().await.unwrap(), 2);
    assert_eq!(bitset.size().await.unwrap(), 32);
}

#[tokio::test]
async fn test_set_range() {
    let bs = client().await.bit_set(unique("testbitset"));
    bs.set_range(3..10, true).await.unwrap();
    assert_eq!(bs.count().await.unwrap(), 7);
    assert_eq!(bs.size().await.unwrap(), 16);
}

#[tokio::test]
async fn test_as_bit_set() {
    let client = client().await;
    let bs = client.bit_set(unique("testbitset"));
    bs.set(3, true).await.unwrap();
    bs.set(41, true).await.unwrap();
    assert_eq!(bs.size().await.unwrap(), 48);

    assert_eq!(bs.ones().await.unwrap(), [3, 41]);
    assert_eq!(bs.count().await.unwrap(), 2);
    assert_eq!(bs.to_bytes().await.unwrap().len(), 6);

    let empty = client.bit_set(unique("emptybitset"));
    assert!(empty.ones().await.unwrap().is_empty());
    assert!(empty.to_bytes().await.unwrap().is_empty());
}

#[tokio::test]
async fn test_and() {
    let client = client().await;
    let tag = unique("bits");
    let bs1 = client.bit_set(format!("{{{tag}}}:testbitset1"));
    bs1.set_range(3..5, true).await.unwrap();
    assert_eq!(bs1.count().await.unwrap(), 2);
    assert_eq!(bs1.size().await.unwrap(), 8);

    let name2 = format!("{{{tag}}}:testbitset2");
    let bs2 = client.bit_set(name2.clone());
    bs2.set(4, true).await.unwrap();
    bs2.set(10, true).await.unwrap();
    assert_eq!(bs1.and(&[name2.as_str()]).await.unwrap(), 2);
    assert!(!bs1.get(3).await.unwrap());
    assert!(bs1.get(4).await.unwrap());
    assert!(!bs1.get(5).await.unwrap());
    assert!(bs2.get(10).await.unwrap());

    assert_eq!(bs1.count().await.unwrap(), 1);
    assert_eq!(bs1.size().await.unwrap(), 16);
}

#[tokio::test]
async fn or_and_xor_combine_into_this_set() {
    let client = client().await;
    let tag = unique("bits");
    let bs1 = client.bit_set(format!("{{{tag}}}:a"));
    let name2 = format!("{{{tag}}}:b");
    let bs2 = client.bit_set(name2.clone());
    bs1.set_many(&[1, 2], true).await.unwrap();
    bs2.set_many(&[2, 3], true).await.unwrap();
    bs1.or(&[name2.as_str()]).await.unwrap();
    assert_eq!(bs1.ones().await.unwrap(), [1, 2, 3]);
    bs1.xor(&[name2.as_str()]).await.unwrap();
    assert_eq!(bs1.ones().await.unwrap(), [1]);
}

#[tokio::test]
async fn test_get_with_indexes() {
    let bitset = client().await.bit_set(unique("testbitset"));
    bitset.set_range(4..10, true).await.unwrap();
    assert_eq!(
        bitset.get_many(&[2, 4, 7, 8]).await.unwrap(),
        [false, true, true, true]
    );
}
