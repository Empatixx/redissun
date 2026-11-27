use crate::common::{client, unique};
use redissun::{Error, Object};

#[tokio::test]
async fn a_missing_bit_set_reads_as_all_zero() {
    let bits = client().await.bit_set(unique("bits"));
    assert!(!bits.get(0).await.unwrap());
    assert!(!bits.get(1_000_000).await.unwrap());
    assert_eq!(bits.count_ones().await.unwrap(), 0);
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
async fn count_ones_and_first_set() {
    let bits = client().await.bit_set(unique("bits"));
    assert_eq!(bits.first_set().await.unwrap(), None);
    for index in [3u64, 9, 100, 4_000] {
        bits.set(index, true).await.unwrap();
    }
    assert_eq!(bits.count_ones().await.unwrap(), 4);
    assert_eq!(bits.first_set().await.unwrap(), Some(3));
    bits.set(3, false).await.unwrap();
    assert_eq!(bits.first_set().await.unwrap(), Some(9));
}

#[tokio::test]
async fn len_is_the_string_length_in_bits_and_last_set_the_highest_set_bit() {
    let bits = client().await.bit_set(unique("bits"));
    assert_eq!(bits.len().await.unwrap(), 0);
    assert_eq!(bits.last_set().await.unwrap(), None);
    bits.set(10, true).await.unwrap();
    assert_eq!(bits.len().await.unwrap(), 16);
    assert_eq!(bits.last_set().await.unwrap(), Some(10));
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

// Redisson: testUnsigned
#[tokio::test]
async fn an_unsigned_field_can_be_set_incremented_and_read() {
    let bs = client().await.bit_set(unique("testUnsigned"));
    assert_eq!(bs.set_unsigned(8, 1, 120).await.unwrap(), 0);
    assert_eq!(bs.add_unsigned(8, 1, 1).await.unwrap(), 121);
    assert_eq!(bs.get_unsigned(8, 1).await.unwrap(), 121);
}

// Redisson: testSigned
#[tokio::test]
async fn a_negative_signed_field_can_be_set_incremented_and_read() {
    let bs = client().await.bit_set(unique("testSigned"));
    assert_eq!(bs.set_signed(8, 1, -120).await.unwrap(), 0);
    assert_eq!(bs.add_signed(8, 1, 1).await.unwrap(), -119);
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

// Redisson: testIncrement
#[tokio::test]
async fn add_signed_adds_to_the_stored_field() {
    let bs2 = client().await.bit_set(unique("testbitset1"));
    assert_eq!(bs2.set_signed(8, 2, 12).await.unwrap(), 0);
    assert_eq!(bs2.get_signed(8, 2).await.unwrap(), 12);
    assert_eq!(bs2.add_signed(8, 2, 12).await.unwrap(), 24);
    assert_eq!(bs2.get_signed(8, 2).await.unwrap(), 24);
}

// Redisson: testSetGetNumber
#[tokio::test]
async fn signed_fields_of_8_to_64_bits_round_trip() {
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

// Redisson: testIndexRange
#[tokio::test]
async fn a_bit_at_twice_i32_max_can_be_set_and_read() {
    let bs = client().await.bit_set(unique("testbitset"));
    let top_index = i32::MAX as u64 * 2;
    assert!(!bs.get(top_index).await.unwrap());
    bs.set(top_index, true).await.unwrap();
    assert!(bs.get(top_index).await.unwrap());
    bs.del().await.unwrap();
}

// Redisson: testLength
#[tokio::test]
async fn last_set_is_the_highest_set_bit_after_clearing_bits() {
    let bs = client().await.bit_set(unique("testbitset"));
    bs.set_range(0..5, true).await.unwrap();
    bs.set_range(0..1, false).await.unwrap();
    assert_eq!(bs.last_set().await.unwrap(), Some(4));

    bs.clear().await.unwrap();
    bs.set(28, true).await.unwrap();
    bs.set(31, true).await.unwrap();
    assert_eq!(bs.last_set().await.unwrap(), Some(31));

    bs.clear().await.unwrap();
    bs.set(3, true).await.unwrap();
    bs.set(7, true).await.unwrap();
    assert_eq!(bs.last_set().await.unwrap(), Some(7));

    bs.clear().await.unwrap();
    bs.set(3, true).await.unwrap();
    bs.set(120, true).await.unwrap();
    bs.set(121, true).await.unwrap();
    assert_eq!(bs.last_set().await.unwrap(), Some(121));

    bs.clear().await.unwrap();
    bs.set(0, true).await.unwrap();
    assert_eq!(bs.last_set().await.unwrap(), Some(0));

    bs.clear().await.unwrap();
    bs.set_range(0..2, true).await.unwrap();
    bs.set(9, true).await.unwrap();
    bs.set_range(9..10, false).await.unwrap();
    assert_eq!(bs.last_set().await.unwrap(), Some(1));

    bs.clear().await.unwrap();
    bs.set(7, true).await.unwrap();
    bs.set(9, true).await.unwrap();
    bs.set_range(9..10, false).await.unwrap();
    assert_eq!(bs.last_set().await.unwrap(), Some(7));
}

// Redisson: testClear
#[tokio::test]
async fn set_range_to_false_clears_the_leading_bits() {
    let bs = client().await.bit_set(unique("testbitset"));
    bs.set_range(0..8, true).await.unwrap();
    bs.set_range(0..3, false).await.unwrap();
    assert_eq!(bs.ones().await.unwrap(), [3, 4, 5, 6, 7]);
}

// Redisson: testNot
#[tokio::test]
async fn not_inverts_every_bit_of_the_stored_bytes() {
    let bs = client().await.bit_set(unique("testbitset"));
    bs.set(3, true).await.unwrap();
    bs.set(5, true).await.unwrap();
    assert_eq!(bs.not().await.unwrap(), 1);
    assert_eq!(bs.ones().await.unwrap(), [0, 1, 2, 4, 6, 7]);
}

// Redisson: testSet
#[tokio::test]
async fn set_replace_and_set_many_change_the_set_bits() {
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

// Redisson: testSetGet
#[tokio::test]
async fn set_bits_are_read_back_and_counted() {
    let bitset = client().await.bit_set(unique("testbitset"));
    assert_eq!(bitset.count_ones().await.unwrap(), 0);
    assert_eq!(bitset.len().await.unwrap(), 0);

    assert!(!bitset.set(10, true).await.unwrap());
    assert!(!bitset.set(31, true).await.unwrap());
    assert!(!bitset.get(0).await.unwrap());
    assert!(bitset.get(31).await.unwrap());
    assert!(bitset.get(10).await.unwrap());
    assert_eq!(bitset.count_ones().await.unwrap(), 2);
    assert_eq!(bitset.len().await.unwrap(), 32);
}

// Redisson: testSetRange
#[tokio::test]
async fn set_range_sets_every_bit_in_the_half_open_range() {
    let bs = client().await.bit_set(unique("testbitset"));
    bs.set_range(3..10, true).await.unwrap();
    assert_eq!(bs.count_ones().await.unwrap(), 7);
    assert_eq!(bs.len().await.unwrap(), 16);
}

// Redisson: testAsBitSet
#[tokio::test]
async fn ones_and_to_bytes_reflect_the_set_bits() {
    let client = client().await;
    let bs = client.bit_set(unique("testbitset"));
    bs.set(3, true).await.unwrap();
    bs.set(41, true).await.unwrap();
    assert_eq!(bs.len().await.unwrap(), 48);

    assert_eq!(bs.ones().await.unwrap(), [3, 41]);
    assert_eq!(bs.count_ones().await.unwrap(), 2);
    assert_eq!(bs.to_bytes().await.unwrap().len(), 6);

    let empty = client.bit_set(unique("emptybitset"));
    assert!(empty.ones().await.unwrap().is_empty());
    assert!(empty.to_bytes().await.unwrap().is_empty());
}

// Redisson: testAnd
#[tokio::test]
async fn and_keeps_only_the_bits_set_in_both_sets() {
    let client = client().await;
    let tag = unique("bits");
    let bs1 = client.bit_set(format!("{{{tag}}}:testbitset1"));
    bs1.set_range(3..5, true).await.unwrap();
    assert_eq!(bs1.count_ones().await.unwrap(), 2);
    assert_eq!(bs1.len().await.unwrap(), 8);

    let name2 = format!("{{{tag}}}:testbitset2");
    let bs2 = client.bit_set(name2.clone());
    bs2.set(4, true).await.unwrap();
    bs2.set(10, true).await.unwrap();
    assert_eq!(bs1.and(&[name2.as_str()]).await.unwrap(), 2);
    assert!(!bs1.get(3).await.unwrap());
    assert!(bs1.get(4).await.unwrap());
    assert!(!bs1.get(5).await.unwrap());
    assert!(bs2.get(10).await.unwrap());

    assert_eq!(bs1.count_ones().await.unwrap(), 1);
    assert_eq!(bs1.len().await.unwrap(), 16);
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

// Redisson: testGetWithIndexes
#[tokio::test]
async fn get_many_reads_the_bit_at_each_index() {
    let bitset = client().await.bit_set(unique("testbitset"));
    bitset.set_range(4..10, true).await.unwrap();
    assert_eq!(
        bitset.get_many(&[2, 4, 7, 8]).await.unwrap(),
        [false, true, true, true]
    );
}
