use std::{collections::BinaryHeap, ops::Range};

use common_error::{DaftError, DaftResult};

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
pub(crate) fn coalesce_partitions(
    sizes: &[usize],
    target: usize,
    min_partitions: usize,
) -> DaftResult<(Vec<Range<usize>>, usize)> {
    if target == 0 || min_partitions == 0 {
        return Err(DaftError::ValueError(
            "Shuffle AQE target bytes and minimum partitions must be greater than 0".into(),
        ));
    }
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
    // Split only at original bucket boundaries to retain both byte/width caps.
    // Splitting the widest group first avoids leaving a long tail of singletons.
    let before_floor = groups.len();
    let minimum = min_partitions.min(sizes.len());
    if groups.len() < minimum {
        let mut heap: BinaryHeap<_> = groups
            .into_iter()
            .map(|r| (r.len(), r.start, r.end))
            .collect();
        while heap.len() < minimum {
            let (len, start, end) = heap.pop().unwrap();
            let mid = start + len / 2;
            heap.push((mid - start, start, mid));
            heap.push((end - mid, mid, end));
        }
        groups = heap.into_iter().map(|(_, start, end)| start..end).collect();
        groups.sort_unstable_by_key(|r| r.start);
    }
    Ok((groups, before_floor))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floor_status_requires_a_real_change_to_greedy_groups() {
        let (large, before) = coalesce_partitions(&[500; 4], 100, 20).unwrap();
        assert_eq!(large.len(), before);
        let (small, before) = coalesce_partitions(&[1; 4], 100, 20).unwrap();
        assert_eq!(small.len(), 4);
        assert_eq!(before, 1);
    }

    #[test]
    fn coalescing_preserves_coverage_and_large_buckets() {
        assert_eq!(
            coalesce_partitions(&[20, 30, 0, 80, 200, 10], 100, 1)
                .unwrap()
                .0,
            vec![0..3, 3..4, 4..5, 5..6]
        );
        assert_eq!(
            coalesce_partitions(&[0, 0, 0], 100, 1).unwrap().0,
            vec![0..3]
        );
        assert_eq!(
            coalesce_partitions(&[usize::MAX, 1], 100, 1).unwrap().0,
            vec![0..1, 1..2]
        );
        assert!(coalesce_partitions(&[], 100, 1).unwrap().0.is_empty());
        assert_eq!(
            coalesce_partitions(&[0; 130], 100, 1).unwrap().0,
            vec![0..64, 64..128, 128..130]
        );
    }

    #[test]
    fn invalid_config_returns_error_and_floor_preserves_coverage() {
        assert!(coalesce_partitions(&[1], 0, 1).is_err());
        assert!(coalesce_partitions(&[1], 100, 0).is_err());
        for floor in [1, 2, 6, 10, 20, usize::MAX] {
            let groups = coalesce_partitions(&[1; 10], 100, floor).unwrap().0;
            assert_eq!(groups.len(), floor.min(10));
            assert_eq!(
                groups.into_iter().flatten().collect::<Vec<_>>(),
                (0..10).collect::<Vec<_>>()
            );
        }
        assert_eq!(coalesce_partitions(&[0; 130], 100, 20).unwrap().0.len(), 20);
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
