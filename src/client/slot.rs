//! The Redis Cluster hash slot of a key.

const SLOTS: u16 = 16384;

/// The hash slot of `key`: CRC16/XMODEM of the key, or of the part between the first `{` and the next `}` when that part is not empty, modulo 16384.
pub(crate) fn keyslot(key: &[u8]) -> u16 {
    crc16(hashed_part(key)) % SLOTS
}

/// The indexes of `keys` grouped by hash slot, in slot order and in the order of the keys within a slot.
pub(crate) fn group_by_slot<'a>(keys: impl IntoIterator<Item = &'a [u8]>) -> Vec<Vec<usize>> {
    let mut slots = std::collections::BTreeMap::<u16, Vec<usize>>::new();
    for (index, key) in keys.into_iter().enumerate() {
        slots.entry(keyslot(key)).or_default().push(index);
    }
    slots.into_values().collect()
}

fn hashed_part(key: &[u8]) -> &[u8] {
    let Some(open) = key.iter().position(|byte| *byte == b'{') else {
        return key;
    };
    match key[open + 1..].iter().position(|byte| *byte == b'}') {
        Some(0) | None => key,
        Some(length) => &key[open + 1..open + 1 + length],
    }
}

fn crc16(bytes: &[u8]) -> u16 {
    bytes.iter().fold(0u16, |crc, byte| {
        (0..8).fold(crc ^ (u16::from(*byte) << 8), |crc, _| {
            if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::{group_by_slot, keyslot};

    #[test]
    fn keys_of_one_slot_are_grouped_in_their_order() {
        let keys: [&[u8]; 4] = [b"{a}1", b"bar", b"{a}2", b"hello"];
        // hello is slot 866, {a} slot 15495, bar slot 5061.
        assert_eq!(group_by_slot(keys), vec![vec![3], vec![1], vec![0, 2]]);
    }

    #[test]
    fn keys_land_in_the_slots_redis_gives_them() {
        // From `CLUSTER KEYSLOT` on Redis 7.4.
        assert_eq!(keyslot(b"bar"), 5061);
        assert_eq!(keyslot(b"hello"), 866);
        assert_eq!(keyslot(b""), 0);
    }

    #[test]
    fn a_hash_tag_puts_keys_in_the_slot_of_the_tag() {
        assert_eq!(keyslot(b"{user1000}.following"), keyslot(b"user1000"));
        assert_eq!(keyslot(b"{user1000}.followers"), keyslot(b"user1000"));
        assert_eq!(keyslot(b"{a}{b}"), keyslot(b"a"));
        // An empty tag or an unclosed brace hashes the whole key.
        assert_ne!(keyslot(b"{}x"), keyslot(b"x"));
        assert_eq!(keyslot(b"a{b"), keyslot(b"a{b"));
    }

    #[test]
    fn foo_is_in_slot_12182() {
        assert_eq!(keyslot(b"foo"), 12182);
    }
}
