use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use daft_local_plan::{
    FlightMapOutput, FlightShuffleReadInput, LocalNodeContext, LocalPhysicalPlan,
    SharedShuffleSpec, ShuffleReadBackend,
};
use daft_logical_plan::stats::StatsState;
use daft_partition_refs::FlightPartitionRef;
use daft_schema::schema::SchemaRef;
use futures::{Stream, StreamExt};

use crate::{
    pipeline_node::{
        MaterializedOutput, NodeID, PipelineNodeImpl, shuffles::aqe::coalesce_partitions,
    },
    plan::PlanExecutionContext,
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::Sender,
};

#[derive(Clone, Default)]
pub(crate) struct FlightShuffleBackendConfig {
    pub(crate) shuffle_id: u64,
    pub(crate) shuffle_dirs: Vec<String>,
    pub(crate) compression: Option<String>,
    pub(crate) shared: Option<SharedShuffleSpec>,
}

pub(crate) fn register_cleanup(
    backend: &FlightShuffleBackendConfig,
    plan_context: &mut PlanExecutionContext,
) {
    let shuffle_dirs_to_register: Vec<String> = backend
        .shuffle_dirs
        .iter()
        .map(|base_dir| format!("{}/daft_shuffle/{}", base_dir, backend.shuffle_id))
        .collect();
    plan_context.register_shuffle_dirs(shuffle_dirs_to_register);
    plan_context.register_shuffle_id(backend.shuffle_id);

    // Registered separately because a shared directory is one tree visible to
    // every node, not one tree per node: fanning the same delete out to the whole
    // cluster would have every worker racing to remove the same files.
    if let Some(shared_root) = backend.shared.as_ref().map(|shared| shared.root.as_str()) {
        plan_context.register_shared_shuffle_dirs(vec![daft_shuffles::store::shared_shuffle_dir(
            shared_root,
            backend.shuffle_id,
        )]);
    }
}

/// Whether `partition` came out of a flight write, i.e. whether the flight read
/// path can address it.
pub(crate) fn is_flight_ref(partition: &PartitionRef) -> bool {
    partition.as_any().is::<FlightPartitionRef>()
}

/// View `partition` as a `FlightPartitionRef`.
///
/// Returns an error rather than panicking: the refs reaching the flight read path
/// come from whatever the upstream stage materialized, and mixing in a plain
/// in-memory ref is a bug in the calling node, not something to abort the process
/// over. The panic this replaces surfaced in Python as a bare
/// `RayTaskError(DaftCoreException)` with no indication of which node was at fault.
fn as_flight_ref(partition: &PartitionRef) -> DaftResult<&FlightPartitionRef> {
    partition
        .as_any()
        .downcast_ref::<FlightPartitionRef>()
        .ok_or_else(|| {
            DaftError::InternalError(
                "Flight shuffle read expected a flight partition ref, got a partition ref that \
                 did not come from a flight write."
                    .to_string(),
            )
        })
}

/// `partition_ref_id` layout: `(input_id << 32) | partition_idx`.
fn input_id_from_ref(flight_ref: &FlightPartitionRef) -> u32 {
    (flight_ref.partition_ref_id >> 32) as u32
}

/// `partition_ref_id` layout: `(input_id << 32) | partition_idx`.
fn partition_idx_from_ref(flight_ref: &FlightPartitionRef) -> u32 {
    (flight_ref.partition_ref_id & 0xFFFF_FFFF) as u32
}

fn map_output_from_ref(flight_ref: &FlightPartitionRef) -> FlightMapOutput {
    FlightMapOutput {
        input_id: input_id_from_ref(flight_ref),
        attempt: flight_ref.attempt,
    }
}

/// Fold map outputs into original-bucket read inputs, grouped by AQE when eligible.
///
/// Each map task emits one `FlightPartitionRef` per output partition, so collecting them
/// all (as the generic transpose does) holds O(map_tasks x num_partitions) refs on the
/// coordinator — e.g. 10k map tasks x 8k partitions is ~82M refs, tens of GB of heap.
/// But the refs are structured (`partition_ref_id = (input_id << 32) | partition_idx`,
/// one per partition per map input), so the matrix is recoverable from just the set of
/// input ids per server — O(map_tasks) total, shared across all partitions via `Arc`.
/// The reduce side reconstructs the exact refs, issuing the same requests as if the
/// full matrix had been kept. Each output is recorded with the attempt that
/// produced it, so a stale registration or file left by another attempt of the
/// same task is never addressed.
///
/// Exactly one output per map input is accepted. The dispatcher delivers one
/// result per task, so a second output for an input cannot happen in normal
/// operation; if it ever did, folding both in would have every reducer read that
/// input twice. That is a wrong answer, so it is refused rather than tolerated.
pub(crate) async fn fold_outputs_from_stream(
    mut materialized_stream: impl Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin,
    num_partitions: usize,
    shuffle_id: u64,
    shared_root: Option<&str>,
    aqe_skip_reason: Option<&str>,
    target_bytes: usize,
) -> DaftResult<Vec<Vec<FlightShuffleReadInput>>> {
    let started = std::time::Instant::now();
    let mut sizes = vec![0usize; num_partitions];
    let mut rows = 0usize;
    let mut nonempty_fragments = 0usize;
    let mut small_fragments = 0usize;
    let mut inputs_by_server: BTreeMap<String, Vec<FlightMapOutput>> = BTreeMap::new();
    let mut seen_inputs: HashSet<u32> = HashSet::new();

    while let Some(output) = materialized_stream.next().await {
        let partitions = output?.into_inner().0;
        let Some(partition) = partitions.first() else {
            continue;
        };
        let flight_ref = as_flight_ref(partition)?;
        let map_output = map_output_from_ref(flight_ref);
        if !seen_inputs.insert(map_output.input_id) {
            return Err(DaftError::InternalError(format!(
                "shuffle {} received two outputs for map input {} (second from {} attempt {:#x}); \
                 refusing to fold both, which would read that input twice",
                shuffle_id, map_output.input_id, flight_ref.server_address, map_output.attempt
            )));
        }
        // Accumulate O(partitions) statistics while discarding each map's refs.
        if partitions.len() != num_partitions {
            return Err(DaftError::InternalError(
                "Flight map output partition count mismatch".into(),
            ));
        }
        for (idx, partition) in partitions.iter().enumerate() {
            let part = as_flight_ref(partition)?;
            if partition_idx_from_ref(part) as usize != idx
                || map_output_from_ref(part) != map_output
                || part.shuffle_id != shuffle_id
                || part.server_address != flight_ref.server_address
            {
                return Err(DaftError::InternalError(
                    "Inconsistent Flight map output identity or partition order".into(),
                ));
            }
            sizes[idx] = sizes[idx].saturating_add(part.size_bytes);
            rows = rows.saturating_add(part.num_rows);
            if part.num_rows > 0 {
                nonempty_fragments += 1;
                small_fragments += usize::from(part.size_bytes < 64 * 1024);
            }
        }
        inputs_by_server
            .entry(flight_ref.server_address.clone())
            .or_default()
            .push(map_output);
    }

    let groups = if aqe_skip_reason.is_none() {
        coalesce_partitions(&sizes, target_bytes)
    } else {
        (0..num_partitions).map(|idx| idx..idx + 1).collect()
    };
    let total_bytes: usize = sizes.iter().copied().fold(0usize, usize::saturating_add);
    let mut sorted_sizes = sizes.clone();
    sorted_sizes.sort_unstable();
    tracing::info!(
        shuffle_id,
        map_tasks = seen_inputs.len(),
        original_partitions = num_partitions,
        reduce_tasks = groups.len(),
        rows,
        uncompressed_bytes = total_bytes,
        nonempty_fragments,
        fragments_under_64k_uncompressed = small_fragments,
        partition_p50_bytes = sorted_sizes
            .get((sorted_sizes.len() * 50).div_ceil(100).saturating_sub(1))
            .copied()
            .unwrap_or(0),
        partition_p95_bytes = sorted_sizes
            .get((sorted_sizes.len() * 95).div_ceil(100).saturating_sub(1))
            .copied()
            .unwrap_or(0),
        partition_max_bytes = sorted_sizes.last().copied().unwrap_or(0),
        map_stage_wait_ms = started.elapsed().as_millis() as u64,
        aqe_status = aqe_skip_reason.unwrap_or(if groups.len() < num_partitions {
            "coalesced"
        } else {
            "no_small_partitions"
        }),
        target_bytes,
        "Shuffle map statistics and AQE decision"
    );
    let inputs_by_server = Arc::new(inputs_by_server);
    let shared_root: Option<Arc<str>> = shared_root.map(Arc::from);
    Ok(groups
        .into_iter()
        .map(|group| {
            group
                .map(|partition_idx| FlightShuffleReadInput {
                    shuffle_id,
                    partition_idx: partition_idx as u32,
                    coalesce_ranges: aqe_skip_reason.is_none(),
                    inputs_by_server: inputs_by_server.clone(),
                    shared_root: shared_root.clone(),
                })
                .collect()
        })
        .collect())
}

/// Express an arbitrary set of flight partition refs as read inputs, grouped by
/// (shuffle, partition idx).
pub(crate) fn read_inputs_from_refs(
    partition_refs: Vec<PartitionRef>,
    shared_root: Option<&str>,
) -> DaftResult<Vec<FlightShuffleReadInput>> {
    let mut groups: BTreeMap<(u64, u32), BTreeMap<String, Vec<FlightMapOutput>>> = BTreeMap::new();
    for partition in partition_refs {
        let flight_ref = as_flight_ref(&partition)?;
        groups
            .entry((flight_ref.shuffle_id, partition_idx_from_ref(flight_ref)))
            .or_default()
            .entry(flight_ref.server_address.clone())
            .or_default()
            .push(map_output_from_ref(flight_ref));
    }

    let shared_root: Option<Arc<str>> = shared_root.map(Arc::from);
    Ok(groups
        .into_iter()
        .map(
            |((shuffle_id, partition_idx), inputs_by_server)| FlightShuffleReadInput {
                shuffle_id,
                partition_idx,
                coalesce_ranges: false,
                inputs_by_server: Arc::new(inputs_by_server),
                shared_root: shared_root.clone(),
            },
        )
        .collect())
}

pub(crate) async fn emit_read_tasks(
    node_id: NodeID,
    schema: SchemaRef,
    read_inputs: Vec<Vec<FlightShuffleReadInput>>,
    node: &dyn PipelineNodeImpl,
    result_tx: Sender<SwordfishTaskBuilder>,
) -> DaftResult<()> {
    for read_input in read_inputs {
        // Fresh plan per task: `SwordfishTaskBuilder::build` mutates the plan in
        // place and requires sole ownership of its Arc.
        let shuffle_read_plan = LocalPhysicalPlan::shuffle_read(
            node_id,
            schema.clone(),
            ShuffleReadBackend::Flight,
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(node_id as usize)),
        );
        let task = SwordfishTaskBuilder::new(shuffle_read_plan, node, node_id)
            .with_flight_shuffle_reads(node_id, read_input);

        let _ = result_tx.send(task).await;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn output(input_id: u32, sizes: &[usize]) -> MaterializedOutput {
        let refs = sizes
            .iter()
            .enumerate()
            .map(|(idx, &size_bytes)| {
                Arc::new(FlightPartitionRef {
                    shuffle_id: 7,
                    server_address: "worker".into(),
                    partition_ref_id: ((input_id as u64) << 32) | idx as u64,
                    attempt: 99,
                    num_rows: usize::from(size_bytes > 0),
                    size_bytes,
                }) as PartitionRef
            })
            .collect();
        MaterializedOutput::new(refs, Arc::from("worker"), "127.0.0.1".into(), input_id)
    }

    #[tokio::test]
    async fn fold_statistics_coalesce_only_when_allowed() -> DaftResult<()> {
        for reason in [
            None,
            Some("disabled"),
            Some("join_input"),
            Some("user_partition_contract"),
        ] {
            let stream =
                futures::stream::iter(vec![Ok(output(0, &[64; 4])), Ok(output(1, &[64; 4]))]);
            let groups =
                fold_outputs_from_stream(stream, 4, 7, Some("/shared"), reason, 256).await?;
            assert_eq!(groups.len(), if reason.is_none() { 2 } else { 4 });
            let refs: Vec<_> = groups.iter().flatten().collect();
            assert_eq!(
                refs.iter().map(|r| r.partition_idx).collect::<Vec<_>>(),
                vec![0, 1, 2, 3]
            );
            for r in &refs {
                assert_eq!(r.coalesce_ranges, reason.is_none());
                assert_eq!(
                    r.inputs_by_server["worker"],
                    vec![
                        FlightMapOutput {
                            input_id: 0,
                            attempt: 99
                        },
                        FlightMapOutput {
                            input_id: 1,
                            attempt: 99
                        }
                    ]
                );
                assert!(Arc::ptr_eq(&refs[0].inputs_by_server, &r.inputs_by_server));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn fold_empty_buckets_and_reject_duplicate_attempts() -> DaftResult<()> {
        let groups = fold_outputs_from_stream(
            futures::stream::iter(vec![Ok(output(0, &[0; 4]))]),
            4,
            7,
            None,
            None,
            256,
        )
        .await?;
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 4);
        let result = fold_outputs_from_stream(
            futures::stream::iter(vec![Ok(output(0, &[1; 4])), Ok(output(0, &[1; 4]))]),
            4,
            7,
            None,
            None,
            256,
        )
        .await;
        assert!(result.is_err());
        let result = fold_outputs_from_stream(
            futures::stream::iter(vec![Ok(output(0, &[1; 3]))]),
            4,
            7,
            None,
            None,
            256,
        )
        .await;
        assert!(result.is_err());
        Ok(())
    }
}
