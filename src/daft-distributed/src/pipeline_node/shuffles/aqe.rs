use std::ops::Range;

/// Record why an exchange exists before internal partition counts are resolved.
/// `Some(n)` alone cannot distinguish a user's request from a planner default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ShuffleOrigin {
    UserRepartition,
    HashJoin,
    Aggregate,
    Distinct,
    Window,
}

impl ShuffleOrigin {
    pub(crate) fn skip_reason(
        self,
        enabled: bool,
        flight: bool,
        in_join: bool,
    ) -> Option<&'static str> {
        if !enabled {
            Some("disabled")
        } else if self == Self::HashJoin || in_join {
            Some("join_input")
        } else if self == Self::UserRepartition {
            Some("user_partition_contract")
        } else if !flight {
            Some("unsupported_backend")
        } else if !matches!(self, Self::Aggregate | Self::Distinct) {
            Some("unsupported_operator")
        } else {
            None
        }
    }
}

/// Greedy contiguous groups, bounded by advisory uncompressed input bytes.
/// Oversized buckets stay intact; splitting a key group would change results.
/// Keep at least one group for an all-empty exchange.
pub(crate) fn coalesce_partitions(sizes: &[usize], target: usize) -> Vec<Range<usize>> {
    assert!(target > 0);
    // Also bound per-task map-ref reconstruction and serialization for empty
    // or tiny buckets; a byte limit alone would allow unbounded fan-in.
    const MAX_BUCKETS_PER_TASK: usize = 64;
    let mut groups = Vec::new();
    let mut start = 0;
    let mut bytes = 0usize;
    for (idx, &size) in sizes.iter().enumerate() {
        if idx > start
            && (size > target.saturating_sub(bytes) || idx - start == MAX_BUCKETS_PER_TASK)
        {
            groups.push(start..idx);
            start = idx;
            bytes = 0;
        }
        bytes = bytes.saturating_add(size);
    }
    if start < sizes.len() {
        groups.push(start..sizes.len());
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalescing_preserves_coverage_and_large_buckets() {
        assert_eq!(
            coalesce_partitions(&[20, 30, 0, 80, 200, 10], 100),
            vec![0..3, 3..4, 4..5, 5..6]
        );
        assert_eq!(coalesce_partitions(&[0, 0, 0], 100), vec![0..3]);
        assert_eq!(coalesce_partitions(&[usize::MAX, 1], 100), vec![0..1, 1..2]);
        assert!(coalesce_partitions(&[], 100).is_empty());
        assert_eq!(
            coalesce_partitions(&[0; 130], 100),
            vec![0..64, 64..128, 128..130]
        );
    }

    #[test]
    fn experimental_gate_and_partition_contracts() {
        for origin in [
            ShuffleOrigin::Aggregate,
            ShuffleOrigin::Distinct,
            ShuffleOrigin::HashJoin,
            ShuffleOrigin::UserRepartition,
            ShuffleOrigin::Window,
        ] {
            assert_eq!(origin.skip_reason(false, true, false), Some("disabled"));
            assert_eq!(origin.skip_reason(true, true, true), Some("join_input"));
        }
        assert_eq!(
            ShuffleOrigin::HashJoin.skip_reason(true, true, false),
            Some("join_input")
        );
        assert_eq!(
            ShuffleOrigin::UserRepartition.skip_reason(true, true, false),
            Some("user_partition_contract")
        );
        assert_eq!(
            ShuffleOrigin::Aggregate.skip_reason(true, false, false),
            Some("unsupported_backend")
        );
        assert_eq!(
            ShuffleOrigin::Aggregate.skip_reason(true, true, false),
            None
        );
        assert_eq!(ShuffleOrigin::Distinct.skip_reason(true, true, false), None);
    }
}
