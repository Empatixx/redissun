//! The Redis Cluster hash slot of a key.

const SLOTS: u16 = 16384;

/// The hash slot of `key`: CRC16/XMODEM of the key, or of the part between the first `{` and the next `}` when that part is not empty, modulo 16384.
pub(crate) fn keyslot(key: &[u8]) -> u16 {
    crc16(hashed_part(key)) % SLOTS
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
    use super::keyslot;

    #[test]
    fn keyslot_matches_fred_for_plain_and_tagged_keys() {
        let mut keys: Vec<Vec<u8>> = [
            "foo",
            "bar",
            "{user1000}.following",
            "{}x",
            "x{}",
            "{a}{b}",
            "a{b",
            "",
        ]
        .iter()
        .map(|key| key.as_bytes().to_vec())
        .collect();
        keys.extend(
            (0..200u32).map(|i| uuid::Uuid::new_v4().as_bytes()[..(i % 16) as usize].to_vec()),
        );
        for key in keys {
            assert_eq!(keyslot(&key), fred::util::redis_keyslot(&key), "{key:?}");
        }
    }

    #[test]
    fn foo_is_in_slot_12182() {
        assert_eq!(keyslot(b"foo"), 12182);
    }
}
