//! The key and channel names of a [`HashMapCache`](super::HashMapCache), built with Redisson's `prefixName`/`suffixName` from the raw name, plus the names redissun 0.23 and earlier used, for the migration.

use super::EntryEventKind;
use crate::object::{prefix_name, suffix_name, tagged};

/// The sorted set of the time-to-live deadlines, Redisson's `getTimeoutSetName`.
pub(crate) fn timeout(name: &str) -> String {
    prefix_name("redissun__timeout__set", name)
}

/// The sorted set of the idle deadlines, Redisson's `getIdleSetName`.
pub(crate) fn idle(name: &str) -> String {
    prefix_name("redissun__idle__set", name)
}

/// The sorted set of the last uses (LRU) or use counts (LFU), Redisson's `getLastAccessTimeSetName`.
pub(crate) fn last_access(name: &str) -> String {
    prefix_name("redissun__map_cache__last_access__set", name)
}

/// The hash of the settings (`max-size`, `mode`, `has-listeners`), Redisson's `getOptionsName`.
pub(crate) fn options(name: &str) -> String {
    suffix_name(name, "redissun_options")
}

/// The prefix of the per-key lease strings, with the trailing `:`; Redisson's `getLeaseName`.
pub(crate) fn lease(name: &str) -> String {
    format!("{}:", prefix_name("redissun__map__lease", name))
}

/// The channel of one kind of entry event, Redisson's `get*ChannelName`.
pub(crate) fn channel(kind: EntryEventKind, name: &str) -> String {
    let prefix = match kind {
        EntryEventKind::Created => "redissun_map_cache_created",
        EntryEventKind::Updated => "redissun_map_cache_updated",
        EntryEventKind::Removed => "redissun_map_cache_removed",
        EntryEventKind::Expired => "redissun_map_cache_expired",
    };
    prefix_name(prefix, name)
}

/// The latch that lets one client clean the cache per run, `MapCacheEvictionTask`'s `executeTaskOnceLatchName`.
pub(crate) fn latch(name: &str) -> String {
    prefix_name("redissun__execute_task_once_latch", name)
}

/// The main hash of redissun 0.23 and earlier: the name with a hash tag.
#[allow(dead_code)]
pub(crate) fn old_main(name: &str) -> String {
    tagged(name)
}

/// The last-access set of redissun 0.23 and earlier.
#[allow(dead_code)]
pub(crate) fn old_last_access(name: &str) -> String {
    format!("redissun__map_cache_last_access__set:{}", tagged(name))
}

/// The options hash of redissun 0.23 and earlier.
#[allow(dead_code)]
pub(crate) fn old_options(name: &str) -> String {
    format!("redissun__map_cache_options:{}", tagged(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collections::hash_map_cache::EntryEventKind;

    #[test]
    fn names_match_redisson() {
        assert_eq!(timeout("m"), "redissun__timeout__set:{m}");
        assert_eq!(idle("m"), "redissun__idle__set:{m}");
        assert_eq!(
            last_access("m"),
            "redissun__map_cache__last_access__set:{m}"
        );
        assert_eq!(options("m"), "{m}:redissun_options");
        assert_eq!(lease("m"), "redissun__map__lease:{m}:");
        assert_eq!(latch("m"), "redissun__execute_task_once_latch:{m}");
        assert_eq!(
            channel(EntryEventKind::Created, "m"),
            "redissun_map_cache_created:{m}"
        );
        assert_eq!(
            channel(EntryEventKind::Updated, "m"),
            "redissun_map_cache_updated:{m}"
        );
        assert_eq!(
            channel(EntryEventKind::Removed, "m"),
            "redissun_map_cache_removed:{m}"
        );
        assert_eq!(
            channel(EntryEventKind::Expired, "m"),
            "redissun_map_cache_expired:{m}"
        );
        assert_eq!(old_main("m"), "{m}");
        assert_eq!(
            old_last_access("m"),
            "redissun__map_cache_last_access__set:{m}"
        );
        assert_eq!(old_options("m"), "redissun__map_cache_options:{m}");
    }

    #[test]
    fn names_with_a_hash_tag_are_used_as_they_are() {
        assert_eq!(timeout("{t}:m"), "redissun__timeout__set:{t}:m");
        assert_eq!(idle("{t}:m"), "redissun__idle__set:{t}:m");
        assert_eq!(
            last_access("{t}:m"),
            "redissun__map_cache__last_access__set:{t}:m"
        );
        assert_eq!(options("{t}:m"), "{t}:m:redissun_options");
        assert_eq!(lease("{t}:m"), "redissun__map__lease:{t}:m:");
        assert_eq!(
            channel(EntryEventKind::Created, "{t}:m"),
            "redissun_map_cache_created:{t}:m"
        );
        assert_eq!(old_main("{t}:m"), "{t}:m");
        assert_eq!(
            old_last_access("{t}:m"),
            "redissun__map_cache_last_access__set:{t}:m"
        );
        assert_eq!(old_options("{t}:m"), "redissun__map_cache_options:{t}:m");
    }
}
