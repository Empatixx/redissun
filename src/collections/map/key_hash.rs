//! The names of the objects Redisson keeps per map key (`RMap.getLock(key)` and friends).

use crate::object::suffix_name;
use highway::{HighwayHash, HighwayHasher};

/// The HighwayHash key of Redisson's `misc.Hash`, shared with the bloom filter.
pub(crate) const HASH_KEY: highway::Key = highway::Key([
    0x9e37_79b9_7f4a_7c15,
    0xf39c_c060_5ced_c834,
    0x1082_276b_f3a2_7251,
    0xf86c_6a11_d0c1_8e95,
]);

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Redisson's `Hash.hash128toBase64`: the 128-bit HighwayHash as 16 big-endian bytes in standard base64, without the trailing `==`.
pub(crate) fn hash128_base64(bytes: &[u8]) -> String {
    let [first, second] = HighwayHasher::new(HASH_KEY).hash128(bytes);
    let mut raw = [0u8; 16];
    raw[..8].copy_from_slice(&first.to_be_bytes());
    raw[8..].copy_from_slice(&second.to_be_bytes());
    let mut out = String::with_capacity(22);
    let (chunks, rest) = raw.as_chunks::<3>();
    for &[a, b, c] in chunks {
        let n = (u32::from(a) << 16) | (u32::from(b) << 8) | u32::from(c);
        for shift in [18, 12, 6, 0] {
            out.push(char::from(ALPHABET[((n >> shift) & 63) as usize]));
        }
    }
    // 16 bytes leave one byte, which base64 writes as two characters before the `==` Redisson cuts off.
    if let [a] = rest {
        let n = u32::from(*a) << 16;
        for shift in [18, 12] {
            out.push(char::from(ALPHABET[((n >> shift) & 63) as usize]));
        }
    }
    out
}

/// The name of the object Redisson keeps for one key of the map `map_name`, `RedissonObject.getLockByMapKey`: `{map}:<hash of the encoded key>:<suffix>`.
pub(crate) fn map_key_name(map_name: &str, encoded_key: &[u8], suffix: &str) -> String {
    suffix_name(
        map_name,
        &format!("{}:{suffix}", hash128_base64(encoded_key)),
    )
}

/// The objects Redisson keeps per map key, for a map type with the fields `key: Key` and `codec: C`: `fn per_key` and the public `lock`, `fair_lock`, `rw_lock`, `semaphore`, `count_down_latch` and `permit_expirable_semaphore`. Invoke it inside an `impl` block whose `K` and `C` are the map's key type and codec.
macro_rules! per_key_objects {
    () => {
        /// The key of the object Redisson keeps for map key `k` under `suffix`.
        fn per_key<Q>(&self, k: &Q, suffix: &str) -> $crate::error::Result<$crate::object::Key>
        where
            K: ::std::borrow::Borrow<Q>,
            Q: ::serde::Serialize + ?Sized,
        {
            let encoded = self.codec.encode(k)?;
            Ok($crate::object::Key::new(
                self.key.core.clone(),
                $crate::collections::map::key_hash::map_key_name(self.key.name(), &encoded, suffix),
            ))
        }

        /// The [`Lock`](crate::Lock) of one key of the map, Redisson's `getLock(key)`. A Redisson client locking the same key of the same map takes the same lock.
        #[doc(alias = "getLock")]
        pub fn lock<Q>(&self, k: &Q) -> $crate::error::Result<$crate::locks::Lock>
        where
            K: ::std::borrow::Borrow<Q>,
            Q: ::serde::Serialize + ?Sized,
        {
            Ok($crate::locks::Lock::new(self.per_key(k, "lock")?))
        }

        /// The [`FairLock`](crate::FairLock) of one key of the map, Redisson's `getFairLock(key)`.
        #[doc(alias = "getFairLock")]
        pub fn fair_lock<Q>(
            &self,
            k: &Q,
        ) -> $crate::error::Result<$crate::locks::fair_lock::FairLock>
        where
            K: ::std::borrow::Borrow<Q>,
            Q: ::serde::Serialize + ?Sized,
        {
            Ok($crate::locks::fair_lock::FairLock::new(
                self.per_key(k, "fairlock")?,
            ))
        }

        /// The [`RwLock`](crate::RwLock) of one key of the map, Redisson's `getReadWriteLock(key)`.
        #[doc(alias = "getReadWriteLock")]
        pub fn rw_lock<Q>(&self, k: &Q) -> $crate::error::Result<$crate::locks::rw_lock::RwLock>
        where
            K: ::std::borrow::Borrow<Q>,
            Q: ::serde::Serialize + ?Sized,
        {
            Ok($crate::locks::rw_lock::RwLock::new(
                self.per_key(k, "rw_lock")?,
            ))
        }

        /// The [`Semaphore`](crate::Semaphore) of one key of the map, Redisson's `getSemaphore(key)`.
        #[doc(alias = "getSemaphore")]
        pub fn semaphore<Q>(
            &self,
            k: &Q,
        ) -> $crate::error::Result<$crate::coordination::semaphore::Semaphore>
        where
            K: ::std::borrow::Borrow<Q>,
            Q: ::serde::Serialize + ?Sized,
        {
            Ok($crate::coordination::semaphore::Semaphore::new(
                self.per_key(k, "semaphore")?,
            ))
        }

        /// The [`CountDownLatch`](crate::CountDownLatch) of one key of the map, Redisson's `getCountDownLatch(key)`.
        #[doc(alias = "getCountDownLatch")]
        pub fn count_down_latch<Q>(
            &self,
            k: &Q,
        ) -> $crate::error::Result<$crate::coordination::latch::CountDownLatch>
        where
            K: ::std::borrow::Borrow<Q>,
            Q: ::serde::Serialize + ?Sized,
        {
            Ok($crate::coordination::latch::CountDownLatch::new(
                self.per_key(k, "countdownlatch")?,
            ))
        }

        /// The [`PermitExpirableSemaphore`](crate::PermitExpirableSemaphore) of one key of the map, Redisson's `getPermitExpirableSemaphore(key)`.
        #[doc(alias = "getPermitExpirableSemaphore")]
        pub fn permit_expirable_semaphore<Q>(
            &self,
            k: &Q,
        ) -> $crate::error::Result<$crate::coordination::permit_semaphore::PermitExpirableSemaphore>
        where
            K: ::std::borrow::Borrow<Q>,
            Q: ::serde::Serialize + ?Sized,
        {
            Ok(
                $crate::coordination::permit_semaphore::PermitExpirableSemaphore::new(
                    self.per_key(k, "permitexpirablesemaphore")?,
                ),
            )
        }
    };
}
pub(crate) use per_key_objects;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_matches_redisson() {
        assert_eq!(hash128_base64(b""), "t6rZwibGo2uy1OSmNVe8pg");
        assert_eq!(hash128_base64(b"a"), "qwvAS12pR0CgRitaypEjUw");
        assert_eq!(hash128_base64(b"\"key\""), "IKm9EBQhlzpVIBxUXDHBWw");
        let big: std::vec::Vec<u8> = (0u8..100).collect();
        assert_eq!(hash128_base64(&big), "AuNncGCUwMfMuTk0WEL10g");
    }

    #[test]
    fn per_key_name_uses_the_hash_tag() {
        assert_eq!(
            map_key_name("m", b"a", "lock"),
            "{m}:qwvAS12pR0CgRitaypEjUw:lock"
        );
        assert_eq!(
            map_key_name("{m}", b"a", "lock"),
            "{m}:qwvAS12pR0CgRitaypEjUw:lock"
        );
    }
}
