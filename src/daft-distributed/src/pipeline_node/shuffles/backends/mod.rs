use common_error::{DaftError, DaftResult};
use common_partitioning::PartitionRef;
use daft_local_plan::{
    LocalNodeContext, LocalPhysicalPlan, LocalPhysicalPlanRef,
    ShuffleBackend as LocalShuffleBackend, ShuffleReadBackend,
};
use daft_logical_plan::stats::StatsState;
use daft_schema::schema::SchemaRef;

use crate::{
    pipeline_node::{MaterializedOutput, NodeID, PipelineNodeContext, PipelineNodeImpl},
    plan::PlanExecutionContext,
    scheduling::task::SwordfishTaskBuilder,
    utils::channel::Sender,
};

mod flight;
mod ray;

pub(crate) use flight::FlightShuffleBackendConfig;

/// Mint the identity under which one shuffle's files and registrations live.
///
/// Random rather than derived from `(query_idx, node_id)`: those counters are
/// local to one driver process, so two drivers sharing a cluster — or a shared
/// filesystem — would produce the same id for their first query's first shuffle
/// and then write into, and clean up, each other's directories. Sixty-four random
/// bits make that collision negligible. The id is logged against the plan
/// coordinates it stands for so a directory on disk can still be traced back.
fn make_shuffle_id(context: &PipelineNodeContext) -> u64 {
    let shuffle_id = rand::random::<u64>();
    tracing::info!(
        shuffle_id = shuffle_id,
        query_idx = context.query_idx,
        node_id = context.node_id,
        "Assigned flight shuffle id"
    );
    shuffle_id
}

/// Which map-side writer a shuffle node uses.
///
/// This decides whether shared placement is available, because only the combined
/// file carries an index a peer can resolve byte ranges from. The per-partition
/// layout is addressable only through the writing worker's in-memory cache, so
/// putting it on a shared mount would buy nothing and would leave the read side
/// looking for files in the wrong place.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ShuffleWriteKind {
    /// One combined, self-indexing file per map task (`RepartitionWrite`).
    CombinedFile,
    /// One directory per output partition (`GatherWrite`, `IntoPartitions`).
    PerPartition,
}

/// A shuffle node's resolved backend: the plan-level [`ShuffleBackend`] with this
/// node's `shuffle_id` stamped in, plus the node handles task building needs.
#[derive(Clone)]
pub(crate) enum DistributedShuffleBackend {
    Ray,
    Flight(FlightShuffleBackendConfig),
}

impl DistributedShuffleBackend {
    fn shared_root(&self) -> Option<&str> {
        match self {
            Self::Ray => None,
            Self::Flight(config) => config.shared.as_ref().map(|shared| shared.root.as_str()),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ShuffleBackend {
    backend: DistributedShuffleBackend,
    schema: SchemaRef,
    node_id: NodeID,
}

impl ShuffleBackend {
    pub(crate) fn new(
        context: &PipelineNodeContext,
        schema: SchemaRef,
        backend: DistributedShuffleBackend,
        write_kind: ShuffleWriteKind,
    ) -> Self {
        Self {
            schema,
            node_id: context.node_id,
            backend: match backend {
                DistributedShuffleBackend::Ray => DistributedShuffleBackend::Ray,
                DistributedShuffleBackend::Flight(mut backend) => {
                    backend.shuffle_id = make_shuffle_id(context);
                    if write_kind == ShuffleWriteKind::PerPartition {
                        backend.shared = None;
                    }
                    DistributedShuffleBackend::Flight(backend)
                }
            },
        }
    }

    pub(crate) fn backend(&self) -> &DistributedShuffleBackend {
        &self.backend
    }

    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) fn node_id(&self) -> NodeID {
        self.node_id
    }

    pub(crate) fn register_cleanup(&self, plan_context: &mut PlanExecutionContext) {
        match &self.backend {
            DistributedShuffleBackend::Ray => {}
            DistributedShuffleBackend::Flight(backend) => {
                flight::register_cleanup(backend, plan_context);
            }
        }
    }

    /// The local-plan `ShuffleBackend` matching this distributed shuffle
    /// backend, for use when building local ops like `IntoPartitions`,
    /// `GatherWrite`, or `RepartitionWrite`.
    pub(crate) fn local_shuffle_backend(&self) -> LocalShuffleBackend {
        match self.backend.clone() {
            DistributedShuffleBackend::Ray => LocalShuffleBackend::Ray,
            DistributedShuffleBackend::Flight(cfg) => LocalShuffleBackend::Flight {
                shuffle_id: cfg.shuffle_id,
                shuffle_dirs: cfg.shuffle_dirs,
                compression: cfg.compression,
                shared: cfg.shared,
            },
        }
    }

    /// Build a `SwordfishTaskBuilder` whose plan reads from already-materialized
    /// partition refs (`in_memory_scan` for plain refs, `shuffle_read(Flight)` for
    /// flight refs) and then applies `wrap_plan` on top. The partition refs are
    /// attached to the task via the matching API (`with_psets` /
    /// `with_flight_shuffle_reads`).
    ///
    /// The read path is chosen from what the refs *are*, not from what the backend
    /// is configured to be. A flight-configured node can legitimately hold plain
    /// in-memory refs: only refs produced by a flight write (`gather_write`,
    /// `repartition_write`, a Flight-backed local `into_partitions`) are addressable
    /// over flight, and a node that materializes its child's output directly — as
    /// `IntoPartitionsNode`'s coalesce branch does — gets ordinary Ray object refs.
    /// Dispatching on the config instead used to hand those to the flight reader and
    /// panic on the downcast.
    pub(crate) fn build_refs_task_builder<F>(
        &self,
        partition_refs: Vec<PartitionRef>,
        node: &dyn PipelineNodeImpl,
        wrap_plan: F,
    ) -> DaftResult<SwordfishTaskBuilder>
    where
        F: FnOnce(LocalPhysicalPlanRef) -> LocalPhysicalPlanRef,
    {
        let node_id = self.node_id;
        let num_flight_refs = partition_refs
            .iter()
            .filter(|p| flight::is_flight_ref(p))
            .count();
        // Empty falls to the in-memory path: there is nothing to read back over
        // flight, and `in_memory_scan` with no psets yields an empty partition.
        if num_flight_refs == 0 {
            let total_size_bytes = partition_refs.iter().map(|p| p.size_bytes()).sum::<usize>();
            let in_memory_scan = LocalPhysicalPlan::in_memory_scan(
                node_id,
                self.schema.clone(),
                total_size_bytes,
                StatsState::NotMaterialized,
                LocalNodeContext::new(Some(node_id as usize)),
            );
            let plan = wrap_plan(in_memory_scan);
            return Ok(
                SwordfishTaskBuilder::new(plan, node, node_id).with_psets(node_id, partition_refs)
            );
        }
        if num_flight_refs != partition_refs.len() {
            return Err(DaftError::InternalError(format!(
                "Shuffle read for node {} got a mix of flight and in-memory partition refs \
                 ({} of {} are flight refs); a stage's outputs must be all one or the other.",
                node_id,
                num_flight_refs,
                partition_refs.len(),
            )));
        }

        let read_inputs =
            flight::read_inputs_from_refs(partition_refs, self.backend.shared_root())?;
        let shuffle_read = LocalPhysicalPlan::shuffle_read(
            node_id,
            self.schema.clone(),
            ShuffleReadBackend::Flight,
            StatsState::NotMaterialized,
            LocalNodeContext::new(Some(node_id as usize)),
        );
        let plan = wrap_plan(shuffle_read);
        Ok(SwordfishTaskBuilder::new(plan, node, node_id)
            .with_flight_shuffle_reads(node_id, read_inputs))
    }

    /// Group a stream of map-task outputs into per-partition read tasks.
    ///
    /// The Ray backend transposes the full (tasks x partitions) matrix of object refs.
    /// The flight backend folds the stream into per-server map-input lists shared by
    /// all reduce tasks, keeping coordinator memory O(map_tasks + partitions) instead
    /// of O(map_tasks x partitions).
    pub(crate) async fn emit_read_tasks_from_stream(
        &self,
        materialized_stream: impl futures::Stream<Item = DaftResult<MaterializedOutput>> + Send + Unpin,
        num_partitions: usize,
        node: &dyn PipelineNodeImpl,
        result_tx: Sender<SwordfishTaskBuilder>,
    ) -> DaftResult<()> {
        match &self.backend {
            DistributedShuffleBackend::Ray => {
                let partition_groups =
                    crate::utils::transpose::transpose_materialized_outputs_from_stream(
                        materialized_stream,
                        num_partitions,
                    )
                    .await?;
                ray::emit_read_tasks(
                    self.node_id,
                    self.schema.clone(),
                    partition_groups,
                    node,
                    result_tx,
                )
                .await
            }
            DistributedShuffleBackend::Flight(cfg) => {
                let read_inputs = flight::fold_outputs_from_stream(
                    materialized_stream,
                    num_partitions,
                    cfg.shuffle_id,
                    self.backend.shared_root(),
                )
                .await?;
                flight::emit_read_tasks(
                    self.node_id,
                    self.schema.clone(),
                    read_inputs,
                    node,
                    result_tx,
                )
                .await
            }
        }
    }
}
