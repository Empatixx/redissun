use crate::error::{Error, Result};
use crate::object::{HasKey, Key};
use crate::reply::{array, int, malformed, number};
use bytes::Bytes;
use fred::interfaces::KeysInterface;
use fred::types::scripts::Script;
use fred::types::Value;
use std::fmt;
use std::ops::Range;
use std::sync::LazyLock;

const MAX_INDEX: u64 = (1 << 32) - 1;

pub(crate) static COUNT: LazyLock<Script> =
    LazyLock::new(|| Script::from_lua("return redis.call('BITCOUNT', KEYS[1])"));

static LENGTH: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "local i = redis.call('bitpos', KEYS[1], 1, -1)
        local pos = i < 0 and redis.call('bitpos', KEYS[1], 0, -1) or math.floor(i / 8) * 8
        while (pos >= 0) do
            i = redis.call('bitpos', KEYS[1], 1, math.floor(pos / 8), math.floor(pos / 8))
            if i < 0 then
                pos = pos - 8
            else
                for j = pos + 7, pos, -1 do
                    if redis.call('getbit', KEYS[1], j) == 1 then
                        return j + 1
                    end
                end
            end
        end
        return 0",
    )
});

static SET_RANGE: LazyLock<Script> = LazyLock::new(|| {
    Script::from_lua(
        "for i = tonumber(ARGV[1]), tonumber(ARGV[2]) - 1, 1 do
            redis.call('SETBIT', KEYS[1], i, ARGV[3])
        end
        return 1",
    )
});

fn checked(index: u64) -> Result<u64> {
    if index > MAX_INDEX {
        return Err(Error::Config(
            "bit index is too large, the maximum is 2^32 - 1".into(),
        ));
    }
    Ok(index)
}

fn index_value(index: u64) -> Result<Value> {
    Ok(int(checked(index)?))
}

fn bit(value: bool) -> Value {
    Value::Integer(i64::from(value))
}

fn field(signed: bool, size: u32) -> Result<Value> {
    let limit = if signed { 64 } else { 63 };
    if size == 0 || size > limit {
        return Err(Error::Config(format!(
            "a {} field must be 1 to {limit} bits wide",
            if signed { "signed" } else { "unsigned" }
        )));
    }
    Ok(Value::from(format!(
        "{}{size}",
        if signed { 'i' } else { 'u' }
    )))
}

fn to_bytes(indexes: &[u64]) -> Result<Vec<u8>> {
    let Some(highest) = indexes.iter().copied().max() else {
        return Ok(Vec::new());
    };
    let mut out = vec![0u8; (checked(highest)? / 8 + 1) as usize];
    for &index in indexes {
        out[(index / 8) as usize] |= 1 << (7 - index % 8);
    }
    Ok(out)
}

/// A distributed array of bits stored in a Redis string, as Redisson's `RBitSet`. A missing bit reads as `false`. The largest index is 2^32 - 1. Bit 0 is the most significant bit of the first byte, as in `SETBIT`.
#[derive(Clone)]
pub struct BitSet {
    key: Key,
}

impl fmt::Debug for BitSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.key.describe(f, "BitSet")
    }
}

impl HasKey for BitSet {
    fn key(&self) -> &Key {
        &self.key
    }
}

impl BitSet {
    pub(crate) fn new(key: Key) -> Self {
        Self { key }
    }

    fn name(&self) -> Value {
        Value::from(self.key.redis_key())
    }

    /// Returns the bit at `index` (`GETBIT`).
    pub async fn get(&self, index: u64) -> Result<bool> {
        let reply = self
            .key
            .command("GETBIT", vec![self.name(), index_value(index)?], 0)
            .await?;
        Ok(number(&reply) == Some(1))
    }

    /// Returns the bits at several indexes in one call (`BITFIELD GET u1`), in the order of `indexes`.
    pub async fn get_many(&self, indexes: &[u64]) -> Result<Vec<bool>> {
        if indexes.is_empty() {
            return Ok(Vec::new());
        }
        let mut args = vec![self.name()];
        for &index in indexes {
            args.extend([Value::from("GET"), Value::from("u1"), index_value(index)?]);
        }
        let reply = self.key.command("BITFIELD", args, 0).await?;
        Ok(array(&reply)
            .iter()
            .map(|value| number(value) == Some(1))
            .collect())
    }

    /// Sets the bit at `index` to `value` and returns its previous value (`SETBIT`). The string grows when needed.
    pub async fn set(&self, index: u64, value: bool) -> Result<bool> {
        let reply = self
            .key
            .command(
                "SETBIT",
                vec![self.name(), index_value(index)?, bit(value)],
                0,
            )
            .await?;
        Ok(number(&reply) == Some(1))
    }

    /// Sets the bits at several indexes to `value` in one call (`BITFIELD SET u1`).
    pub async fn set_many(&self, indexes: &[u64], value: bool) -> Result<()> {
        if indexes.is_empty() {
            return Ok(());
        }
        let mut args = vec![self.name()];
        for &index in indexes {
            args.extend([
                Value::from("SET"),
                Value::from("u1"),
                index_value(index)?,
                bit(value),
            ]);
        }
        self.key.command("BITFIELD", args, 0).await?;
        Ok(())
    }

    /// Sets every bit in `range` to `value`. The end is exclusive, like Redisson's `set(from, to, value)` and `clear(from, to)`.
    pub async fn set_range(&self, range: Range<u64>, value: bool) -> Result<()> {
        if range.start >= range.end {
            return Ok(());
        }
        checked(range.end - 1)?;
        let _: i64 = self
            .key
            .core
            .eval(
                &SET_RANGE,
                vec![self.key.redis_key()],
                vec![
                    Bytes::from(range.start.to_string()),
                    Bytes::from(range.end.to_string()),
                    Bytes::from(if value { "1" } else { "0" }),
                ],
            )
            .await?;
        Ok(())
    }

    /// Number of bits that are set (`BITCOUNT`), Redisson's `cardinality`.
    pub async fn count(&self) -> Result<u64> {
        let reply = self.key.command("BITCOUNT", vec![self.name()], 0).await?;
        Ok(number(&reply).unwrap_or(0) as u64)
    }

    /// Index of the lowest set bit, or `None` when no bit is set (`BITPOS 1`).
    pub async fn first_set(&self) -> Result<Option<u64>> {
        let reply = self
            .key
            .command("BITPOS", vec![self.name(), Value::Integer(1)], 0)
            .await?;
        Ok(number(&reply).and_then(|position| u64::try_from(position).ok()))
    }

    /// Index of the highest set bit plus one, or 0 when no bit is set, like `java.util.BitSet::length` and Redisson's `length`.
    pub async fn length(&self) -> Result<u64> {
        let length: u64 = self
            .key
            .core
            .eval(&LENGTH, vec![self.key.redis_key()], Vec::new())
            .await?;
        Ok(length)
    }

    /// Size of the underlying string in bits, always a multiple of 8, like Redisson's `size`.
    pub async fn size(&self) -> Result<u64> {
        let reply = self.key.command("STRLEN", vec![self.name()], 0).await?;
        Ok(number(&reply).unwrap_or(0) as u64 * 8)
    }

    /// Returns the raw bytes of the string, empty when the key is missing, like Redisson's `toByteArray`.
    pub async fn to_bytes(&self) -> Result<Vec<u8>> {
        let raw: Option<Bytes> = self.key.core.redis().get(self.key.redis_key()).await?;
        Ok(raw.map(|raw| raw.to_vec()).unwrap_or_default())
    }

    /// Indexes of all set bits in ascending order, like Redisson's `asBitSet`.
    pub async fn ones(&self) -> Result<Vec<u64>> {
        let raw = self.to_bytes().await?;
        Ok(raw
            .iter()
            .enumerate()
            .flat_map(|(byte, value)| {
                (0..8)
                    .filter(move |offset| value & (1 << (7 - offset)) != 0)
                    .map(move |offset| byte as u64 * 8 + offset)
            })
            .collect())
    }

    /// Replaces the whole bit set with one where exactly the bits at `indexes` are set, like Redisson's `set(BitSet)`.
    pub async fn replace(&self, indexes: &[u64]) -> Result<()> {
        let raw = to_bytes(indexes)?;
        if raw.is_empty() {
            return self.clear().await;
        }
        self.key
            .core
            .redis()
            .set::<(), _, _>(self.key.redis_key(), Bytes::from(raw), None, None, false)
            .await?;
        Ok(())
    }

    /// Deletes the bit set, so every bit reads as `false`, like Redisson's `clear()`.
    pub async fn clear(&self) -> Result<()> {
        self.key.del_all(Vec::new()).await?;
        Ok(())
    }

    async fn op(&self, op: &'static str, others: &[&str]) -> Result<u64> {
        let mut args = vec![Value::from(op), self.name(), self.name()];
        args.extend(others.iter().map(|name| Value::from(name.to_string())));
        let reply = self.key.command("BITOP", args, 1).await?;
        number(&reply).map(|n| n as u64).ok_or_else(malformed)
    }

    /// Stores the bitwise AND of this set and the sets named in `others` in this set (`BITOP AND`); returns the new size in bytes.
    pub async fn and(&self, others: &[&str]) -> Result<u64> {
        self.op("AND", others).await
    }

    /// Stores the bitwise OR of this set and the sets named in `others` in this set (`BITOP OR`); returns the new size in bytes.
    pub async fn or(&self, others: &[&str]) -> Result<u64> {
        self.op("OR", others).await
    }

    /// Stores the bitwise XOR of this set and the sets named in `others` in this set (`BITOP XOR`); returns the new size in bytes.
    pub async fn xor(&self, others: &[&str]) -> Result<u64> {
        self.op("XOR", others).await
    }

    /// Flips every bit of the string in place (`BITOP NOT`); returns the size in bytes.
    pub async fn not(&self) -> Result<u64> {
        self.op("NOT", &[]).await
    }

    async fn bitfield(&self, signed: bool, size: u32, args: Vec<Value>) -> Result<i64> {
        let mut all = vec![self.name()];
        let mut args = args.into_iter();
        all.push(args.next().ok_or_else(malformed)?);
        all.push(field(signed, size)?);
        all.extend(args);
        let reply = self.key.command("BITFIELD", all, 0).await?;
        array(&reply).first().and_then(number).ok_or_else(malformed)
    }

    /// Reads a signed integer of `size` bits (1 to 64) starting at bit `offset` (`BITFIELD GET i<size>`).
    pub async fn get_signed(&self, size: u32, offset: u64) -> Result<i64> {
        self.bitfield(true, size, vec![Value::from("GET"), int(offset)])
            .await
    }

    /// Writes a signed integer of `size` bits (1 to 64) at bit `offset` and returns the previous one (`BITFIELD SET i<size>`).
    pub async fn set_signed(&self, size: u32, offset: u64, value: i64) -> Result<i64> {
        self.bitfield(
            true,
            size,
            vec![Value::from("SET"), int(offset), Value::Integer(value)],
        )
        .await
    }

    /// Adds `increment` to the signed integer of `size` bits (1 to 64) at bit `offset`, wrapping on overflow, and returns the result (`BITFIELD INCRBY i<size>`).
    pub async fn increment_and_get_signed(
        &self,
        size: u32,
        offset: u64,
        increment: i64,
    ) -> Result<i64> {
        self.bitfield(
            true,
            size,
            vec![
                Value::from("INCRBY"),
                int(offset),
                Value::Integer(increment),
            ],
        )
        .await
    }

    /// Reads an unsigned integer of `size` bits (1 to 63) starting at bit `offset` (`BITFIELD GET u<size>`).
    pub async fn get_unsigned(&self, size: u32, offset: u64) -> Result<u64> {
        let value = self
            .bitfield(false, size, vec![Value::from("GET"), int(offset)])
            .await?;
        Ok(value as u64)
    }

    /// Writes an unsigned integer of `size` bits (1 to 63) at bit `offset` and returns the previous one (`BITFIELD SET u<size>`).
    pub async fn set_unsigned(&self, size: u32, offset: u64, value: u64) -> Result<u64> {
        let previous = self
            .bitfield(
                false,
                size,
                vec![Value::from("SET"), int(offset), int(value)],
            )
            .await?;
        Ok(previous as u64)
    }

    /// Adds `increment` to the unsigned integer of `size` bits (1 to 63) at bit `offset`, wrapping on overflow, and returns the result (`BITFIELD INCRBY u<size>`).
    pub async fn increment_and_get_unsigned(
        &self,
        size: u32,
        offset: u64,
        increment: i64,
    ) -> Result<u64> {
        let value = self
            .bitfield(
                false,
                size,
                vec![
                    Value::from("INCRBY"),
                    int(offset),
                    Value::Integer(increment),
                ],
            )
            .await?;
        Ok(value as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::to_bytes;

    #[test]
    fn indexes_become_bytes_with_bit_zero_first() {
        assert_eq!(to_bytes(&[]).unwrap(), Vec::<u8>::new());
        assert_eq!(to_bytes(&[0]).unwrap(), vec![0x80]);
        assert_eq!(to_bytes(&[1, 10]).unwrap(), vec![0x40, 0x20]);
    }
}
