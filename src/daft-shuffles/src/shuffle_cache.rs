use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use common_error::{DaftError, DaftResult};
use common_runtime::{RuntimeTask, get_io_runtime};
use daft_io::{SourceType, parse_url};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;
use daft_schema::schema::SchemaRef;
use daft_writers::{AsyncFileWriter, make_ipc_writer_with_tracking};
use tokio::sync::Mutex;

fn get_shuffle_dirs(shuffle_dirs: &[String], shuffle_id: u64) -> Vec<String> {
    shuffle_dirs
        .iter()
        .map(|dir| format!("{}/daft_shuffle/{}", dir, shuffle_id))
        .collect()
}

/// Directory for one attempt's spill files of one output partition.
///
/// The attempt is part of the path because two attempts of the same task can be
/// alive on one node at once (see `store::shared_map_file`); the IPC writer names
/// its files by sequence number, so a shared directory would have them overwrite
/// each other.
fn get_partition_dir(shuffle_dirs: &[String], partition_ref_id: u64, attempt: u64) -> String {
    let dir = &shuffle_dirs[(partition_ref_id as usize) % shuffle_dirs.len()];
    format!(
        "{}/partition_ref_{}_{:016x}",
        dir, partition_ref_id, attempt
    )
}

pub fn partition_ref_id(input_id: u32, partition_idx: usize) -> u64 {
    ((input_id as u64) << 32) | partition_idx as u64
}

/// IPC batch chunk target.
///
/// Shared by the oneshot writer and the read-side concat path so write emits at the
/// size read wants.
pub const CHUNK_TARGET_BYTES: usize = 4 * 1024 * 1024;

// Group ready IPC batches into a single blocking operation without changing
// individual IPC messages or waiting for more input.
const READY_WRITE_GROUP_BYTES: usize = 8 * CHUNK_TARGET_BYTES;

// A small first input may also be the last. Keep it in the existing bounded
// queue until another input or close, avoiding a separate async forwarding task.
// Start large inputs immediately so streaming can overlap with their producer.
const DEFER_FIRST_WRITE_BYTES: usize = 8 * 1024;

// Result of a writer task
struct WriterTaskResult {
    bytes_per_file: Vec<usize>,
    total_rows_written: usize,
    total_bytes_written: usize,
    file_paths: Vec<String>,
}
type WriterTask = RuntimeTask<DaftResult<WriterTaskResult>>;

struct PendingWriterTask {
    rx: async_channel::Receiver<MicroPartition>,
    writer: Box<dyn AsyncFileWriter<Input = MicroPartition, Result = Vec<RecordBatch>>>,
    active: Option<Arc<daft_io::shuffle_file::ActiveShuffleWrite>>,
    first_input: bool,
}

impl PendingWriterTask {
    async fn run(self) -> DaftResult<WriterTaskResult> {
        // Also cover the deferred queue. IPCWriter transfers a clone of this
        // registration into blocking operations, preserving drain on cancel.
        let _active = self.active;
        writer_task(self.rx, self.writer).await
    }
}

struct InProgressShuffleCacheState {
    writer_sender: Option<async_channel::Sender<MicroPartition>>,
    writer_task: Option<WriterTask>,
    // Receiver is !Unpin; keep it behind a box so sink states containing this
    // cache remain movable, without promising structural pinning of the receiver.
    pending_writer: Option<Box<PendingWriterTask>>,
    error: Option<Arc<DaftError>>,
}

pub struct InProgressShuffleCache {
    state: Mutex<InProgressShuffleCacheState>,
    writer_sender_weak: async_channel::WeakSender<MicroPartition>,
    writer_started: AtomicBool,
    partition_ref_id: u64,
    schema: SchemaRef,
    write_path: String,
}

impl InProgressShuffleCache {
    pub fn try_new(
        partition_ref_id: u64,
        attempt: u64,
        schema: SchemaRef,
        dirs: &[String],
        shuffle_id: u64,
        target_filesize: usize,
        compression: Option<&str>,
    ) -> DaftResult<Self> {
        // Create the directories
        let active = Arc::new(daft_io::shuffle_file::ActiveShuffleWrite::for_shuffle(
            Some(shuffle_id),
        ));
        // TODO: Add checks here, as well as periodic checks to ensure that the dirs are not too full. If so, we switch to directories with more space.
        // And raise an error if we can't find any directories with space.
        let shuffle_dirs = get_shuffle_dirs(dirs, shuffle_id);
        for dir in &shuffle_dirs {
            // Check that the dir is a file
            let (source_type, _) = parse_url(dir)?;
            if source_type != SourceType::File {
                return Err(DaftError::ValueError(format!(
                    "ShuffleCache only supports file paths, got: {}",
                    dir
                )));
            }

            // If the directory doesn't exist, create it
            if !std::path::Path::new(dir).exists() {
                std::fs::create_dir_all(dir).map_err(|e| {
                    DaftError::IoError(e).with_shuffle_io_context("create directory", dir)
                })?;
            }
        }

        // Create the partition writer
        let partition_dir = get_partition_dir(&shuffle_dirs, partition_ref_id, attempt);
        std::fs::create_dir_all(&partition_dir).map_err(|e| {
            DaftError::IoError(e).with_shuffle_io_context("create directory", &partition_dir)
        })?;

        let writer = make_ipc_writer_with_tracking(
            &partition_dir,
            target_filesize,
            compression,
            crate::local_io::policy(shuffle_id),
            Some(shuffle_id),
            Some(active.clone()),
        )?;

        let mut cache =
            Self::try_new_with_writer_tracked(writer, partition_ref_id, schema, Some(active))?;
        cache.write_path = partition_dir;
        Ok(cache)
    }

    #[cfg(test)]
    fn try_new_with_writer(
        writer: Box<dyn AsyncFileWriter<Input = MicroPartition, Result = Vec<RecordBatch>>>,
        partition_ref_id: u64,
        schema: SchemaRef,
    ) -> DaftResult<Self> {
        Self::try_new_with_writer_tracked(writer, partition_ref_id, schema, None)
    }

    fn try_new_with_writer_tracked(
        writer: Box<dyn AsyncFileWriter<Input = MicroPartition, Result = Vec<RecordBatch>>>,
        partition_ref_id: u64,
        schema: SchemaRef,
        active: Option<Arc<daft_io::shuffle_file::ActiveShuffleWrite>>,
    ) -> DaftResult<Self> {
        let num_cpus = std::thread::available_parallelism().unwrap().get();
        let (tx, rx) = async_channel::bounded(num_cpus * 2);
        let writer_sender_weak = tx.downgrade();

        Ok(Self {
            state: Mutex::new(InProgressShuffleCacheState {
                writer_sender: Some(tx),
                writer_task: None,
                pending_writer: Some(Box::new(PendingWriterTask {
                    rx,
                    writer,
                    active,
                    first_input: true,
                })),
                error: None,
            }),
            writer_sender_weak,
            writer_started: AtomicBool::new(false),
            partition_ref_id,
            schema,
            write_path: String::new(),
        })
    }

    /// Push single partition data to the writer.
    pub async fn push_partition_data(&self, partition: MicroPartition) -> DaftResult<()> {
        if !self.writer_started.load(Ordering::Acquire) {
            // Do not hold a Sender while waiting for state: close holds state
            // while draining the channel, and must be able to close that channel.
            let mut state = self.state.lock().await;
            if let Some(pending) = &mut state.pending_writer {
                if pending.first_input && partition.size_bytes() <= DEFER_FIRST_WRITE_BYTES {
                    pending.first_input = false;
                } else {
                    let pending = state.pending_writer.take().unwrap();
                    state.writer_task = Some(get_io_runtime(true).spawn((*pending).run()));
                    self.writer_started.store(true, Ordering::Release);
                }
            }
        }
        let send_future = async move {
            match self.writer_sender_weak.upgrade() {
                Some(sender) => sender.send(partition).await.map_err(|e| e.to_string()),
                None => Err("Shuffle cache has been closed".to_string()),
            }
        };

        if let Err(e) = send_future.await {
            self.close().await?;
            return Err(DaftError::InternalError(e));
        }

        Ok(())
    }

    pub async fn close(&self) -> DaftResult<PartitionCache> {
        let mut state = self.state.lock().await;
        // If there was an error from a previous close, return it
        if let Some(error) = &state.error {
            return Err(DaftError::Shared(error.clone()));
        }

        let writer_sender = state.writer_sender.take();
        let writer_task = std::mem::take(&mut state.writer_task);
        let pending_writer = state.pending_writer.take();

        // Close the writer tasks
        let close_result = Self::close_internal(writer_sender, writer_task, pending_writer).await;
        if let Err(err) = close_result {
            let err = Arc::new(err.with_shuffle_io_context("write or close", &self.write_path));
            state.error = Some(err.clone());
            return Err(DaftError::Shared(err));
        }

        // All good, get the schema and results
        let writer_result = close_result.unwrap();

        match writer_result {
            Some(result) => Ok(PartitionCache {
                partition_ref_id: self.partition_ref_id,
                schema: self.schema.clone(),
                bytes_per_file: result.bytes_per_file,
                file_paths: result.file_paths,
                num_rows: result.total_rows_written,
                size_bytes: result.total_bytes_written,
                byte_ranges: None,
                crc32s: None,
            }),
            None => Err(DaftError::InternalError(
                "No writer result found".to_string(),
            )),
        }
    }

    async fn close_internal(
        writer_sender: Option<async_channel::Sender<MicroPartition>>,
        writer_task: Option<WriterTask>,
        pending_writer: Option<Box<PendingWriterTask>>,
    ) -> DaftResult<Option<WriterTaskResult>> {
        // Drop the writer senders so that the writer tasks can exit
        drop(writer_sender);

        // Wait for the writer tasks to exit
        if let Some(writer_task) = writer_task {
            let result = writer_task.await??;
            Ok(Some(result))
        } else if let Some(pending) = pending_writer {
            // IPCWriter still offloads encoding and file operations to the
            // blocking pool. Only the otherwise redundant async task is removed.
            Ok(Some((*pending).run().await?))
        } else {
            Ok(None)
        }
    }
}

// Writer task that takes a partition from a writer sender, writes them to a file, and returns the schema and file path
async fn writer_task(
    rx: async_channel::Receiver<MicroPartition>,
    mut writer: Box<dyn AsyncFileWriter<Input = MicroPartition, Result = Vec<RecordBatch>>>,
) -> DaftResult<WriterTaskResult> {
    let mut total_rows_written = 0;
    let mut total_bytes_written = 0;
    let mut completed = None;
    while let Ok(partition) = rx.recv().await {
        let mut bytes = partition.size_bytes();
        let mut rows = partition.len();
        // Drain only ready input: never wait for a batch to fill. Reuse the
        // existing bounded queue, and bound work between await points even if
        // producers refill it. concat keeps record batches without copying data.
        let partition = if bytes < READY_WRITE_GROUP_BYTES
            && let Ok(next) = rx.try_recv()
        {
            bytes += next.size_bytes();
            rows += next.len();
            let mut parts = vec![partition, next];
            while parts.len() < 32 && bytes < READY_WRITE_GROUP_BYTES {
                let Ok(next) = rx.try_recv() else { break };
                bytes += next.size_bytes();
                rows += next.len();
                parts.push(next);
            }
            MicroPartition::concat(parts)?
        } else {
            partition
        };
        total_rows_written += rows;
        total_bytes_written += bytes;
        // This task already runs on the IO runtime. IPCWriter still offloads
        // blocking syscalls and owns their cancellation/drain guards.
        if rx.is_closed() && rx.is_empty() {
            let (_, paths) = writer.write_and_close(partition).await?;
            completed = Some(paths);
            break;
        }
        writer.write(partition).await?;
    }
    let file_path_tables = match completed {
        Some(paths) => paths,
        None => writer.close().await?,
    };

    let file_paths = file_path_tables
        .into_iter()
        .map(|file_path_table| {
            assert!(file_path_table.num_columns() > 0);
            assert!(file_path_table.num_rows() == 1);
            // IPC writer should always return a RecordBatch of one path column
            let path = file_path_table
                .get_column(0)
                .utf8()?
                .get(0)
                .expect("path column should have one path");
            Ok(path.to_string())
        })
        .collect::<DaftResult<Vec<String>>>()?;

    let bytes_per_file = writer.bytes_per_file();
    assert!(bytes_per_file.len() == file_paths.len());
    Ok(WriterTaskResult {
        bytes_per_file,
        total_rows_written,
        total_bytes_written,
        file_paths,
    })
}

#[derive(Debug, Clone)]
pub struct PartitionCache {
    pub partition_ref_id: u64,
    pub schema: SchemaRef,
    pub bytes_per_file: Vec<usize>,
    pub file_paths: Vec<String>,
    pub num_rows: usize,
    pub size_bytes: usize,
    /// `(start, end)` per file. Set when one file holds multiple output partitions
    /// (combined-file shuffle); `None` means read the whole file.
    pub byte_ranges: Option<Vec<(u64, u64)>>,
    /// CRC-32 of each entry of `byte_ranges`, so a reader can tell a range that is
    /// merely the right length from one that is also the right bytes.
    ///
    /// `None` for the per-partition layout, whose writer records no checksum. Those
    /// files are read whole, where a truncated stream is caught by the absence of
    /// the writer's end-of-stream marker instead.
    pub crc32s: Option<Vec<u32>>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use daft_core::datatypes::DataType;
    use daft_schema::{field::Field, schema::Schema};
    use daft_writers::test::{
        DummyWriterFactory, FailingWriterFactory, make_dummy_mp,
        make_dummy_target_file_size_writer_factory,
    };

    use super::*;

    fn dummy_schema() -> SchemaRef {
        // Matches the schema produced by `make_dummy_mp` in daft-writers tests.
        Arc::new(Schema::new(vec![Field::new("ints", DataType::UInt8)]))
    }

    #[tokio::test]
    async fn deferred_first_batch_and_concurrent_producers_preserve_all_values() -> DaftResult<()> {
        let shuffle_id = rand::random::<u64>();
        let root = std::env::temp_dir().join(format!("daft-cache-deferred-{shuffle_id}"));
        let cache = Arc::new(InProgressShuffleCache::try_new(
            0,
            1,
            dummy_schema(),
            &[root.to_string_lossy().into_owned()],
            shuffle_id,
            1024 * 1024,
            None,
        )?);
        cache.push_partition_data(make_dummy_mp(64)).await?;
        // The small first input is queued and tracked, without an output file.
        assert_eq!(std::fs::read_dir(&cache.write_path)?.count(), 0);
        assert_eq!(
            daft_io::shuffle_file::active_shuffle_writes_for(&[shuffle_id]),
            1
        );
        let mut expected: Vec<u8> = (0..64).collect();
        let mut tasks = Vec::new();
        for producer in 0..4 {
            let cache = cache.clone();
            tasks.push(tokio::spawn(async move {
                for batch in 0..16 {
                    cache
                        .push_partition_data(make_dummy_mp(100 + producer * 16 + batch))
                        .await?;
                }
                DaftResult::Ok(())
            }));
        }
        for size in 100..164 {
            expected.extend((0..size).map(|i| i as u8));
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            for task in tasks {
                task.await.unwrap().unwrap();
            }
        })
        .await
        .unwrap();
        let result = cache.close().await?;
        assert_eq!(result.num_rows, expected.len());
        let mut actual = Vec::new();
        for path in result.file_paths {
            for batch in arrow_ipc::reader::StreamReader::try_new(std::fs::File::open(path)?, None)?
            {
                let batch = batch?;
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::UInt8Array>()
                    .unwrap();
                actual.extend_from_slice(values.values());
            }
        }
        actual.sort_unstable();
        expected.sort_unstable();
        assert_eq!(actual, expected);
        drop(cache);
        assert_eq!(
            daft_io::shuffle_file::active_shuffle_writes_for(&[shuffle_id]),
            0
        );
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn close_racing_a_deferred_push_does_not_deadlock_or_lose_accepted_input()
    -> DaftResult<()> {
        for _ in 0..16 {
            let shuffle_id = rand::random::<u64>();
            let root = std::env::temp_dir().join(format!("daft-cache-close-race-{shuffle_id}"));
            let cache = Arc::new(InProgressShuffleCache::try_new(
                0,
                1,
                dummy_schema(),
                &[root.to_string_lossy().into_owned()],
                shuffle_id,
                1024 * 1024,
                None,
            )?);
            let barrier = Arc::new(tokio::sync::Barrier::new(3));
            let push = tokio::spawn({
                let cache = cache.clone();
                let barrier = barrier.clone();
                async move {
                    barrier.wait().await;
                    cache.push_partition_data(make_dummy_mp(64)).await
                }
            });
            let close = tokio::spawn({
                let cache = cache.clone();
                let barrier = barrier.clone();
                async move {
                    barrier.wait().await;
                    cache.close().await
                }
            });
            barrier.wait().await;
            let (pushed, closed) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                (push.await.unwrap(), close.await.unwrap())
            })
            .await
            .unwrap();
            assert_eq!(closed?.num_rows, if pushed.is_ok() { 64 } else { 0 });
            drop(cache);
            std::fs::remove_dir_all(root)?;
        }
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires LD_PRELOAD shuffle EIO injector"]
    async fn deferred_close_keeps_write_tracking_until_cancelled_io_finishes() -> DaftResult<()> {
        let root = std::path::PathBuf::from(std::env::var("DAFT_TEST_SHUFFLE_IO_ROOT").unwrap());
        let shuffle_id = 0xe10_009;
        crate::local_io::configure(
            shuffle_id,
            daft_io::shuffle_file::EioRetryPolicy {
                max_retries: 6,
                initial_backoff_ms: 32_000,
                max_backoff_ms: 32_000,
            },
        );
        let cache = InProgressShuffleCache::try_new(
            0,
            1,
            dummy_schema(),
            &[root.to_string_lossy().into_owned()],
            shuffle_id,
            1024 * 1024,
            None,
        )?;
        cache.push_partition_data(make_dummy_mp(4096)).await?;
        let mut closing = Box::pin(cache.close());
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::select! {
                result = &mut closing => panic!("close finished before cancellation: {}", result.is_ok()),
                () = async {
                    while !root.join("fault-0").exists() {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                } => {}
            }
        }).await.unwrap();
        assert_eq!(
            daft_io::shuffle_file::active_shuffle_writes_for(&[shuffle_id]),
            1
        );
        drop(closing);
        assert!(
            cache.close().await.is_err(),
            "cancelled close published a result"
        );
        drop(cache);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while daft_io::shuffle_file::active_shuffle_writes_for(&[shuffle_id]) != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(!root.join("fault-1").exists());
        std::fs::remove_dir_all(root.join("daft_shuffle"))?;
        crate::store::forget_shuffle(shuffle_id);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires LD_PRELOAD shuffle EIO injector"]
    async fn shared_write_tracking_survives_cancellation() -> DaftResult<()> {
        let root = std::path::PathBuf::from(std::env::var("DAFT_TEST_SHUFFLE_IO_ROOT").unwrap());
        let shuffle_id = 0xe10_008;
        crate::local_io::configure(
            shuffle_id,
            daft_io::shuffle_file::EioRetryPolicy {
                max_retries: 6,
                initial_backoff_ms: 32_000,
                max_backoff_ms: 32_000,
            },
        );
        let cache = InProgressShuffleCache::try_new(
            0,
            1,
            dummy_schema(),
            &[root.to_string_lossy().into_owned()],
            shuffle_id,
            1024 * 1024,
            None,
        )?;
        cache.push_partition_data(make_dummy_mp(16 * 1024)).await?;
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !root.join("fault-0").exists() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        // Queue, factory and blocking operation share a single registration.
        assert_eq!(
            daft_io::shuffle_file::active_shuffle_writes_for(&[shuffle_id]),
            1
        );
        drop(cache);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while daft_io::shuffle_file::active_shuffle_writes_for(&[shuffle_id]) != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(!root.join("fault-1").exists());
        std::fs::remove_dir_all(root.join("daft_shuffle"))?;
        crate::store::forget_shuffle(shuffle_id);
        Ok(())
    }

    #[tokio::test]
    async fn streaming_ready_batches_preserve_order_across_file_rotation() -> DaftResult<()> {
        let shuffle_id = rand::random::<u64>();
        let root = std::env::temp_dir().join(format!("daft-cache-batching-{shuffle_id}"));
        let cache = InProgressShuffleCache::try_new(
            0,
            1,
            dummy_schema(),
            &[root.to_string_lossy().into_owned()],
            shuffle_id,
            8192,
            None,
        )?;
        let mut expected = Vec::new();
        for i in 0..97 {
            let size = [0, 257, 4096, 65][i % 4];
            expected.extend((0..size).map(|i| i as u8));
            cache.push_partition_data(make_dummy_mp(size)).await?;
        }
        let result = cache.close().await?;
        assert!(result.file_paths.len() > 1);
        assert_eq!(result.num_rows, expected.len());
        assert_eq!(result.size_bytes, expected.len());
        let mut actual = Vec::new();
        for path in result.file_paths {
            let reader =
                arrow_ipc::reader::StreamReader::try_new(std::fs::File::open(path)?, None)?;
            for batch in reader {
                let batch = batch?;
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::UInt8Array>()
                    .unwrap();
                actual.extend_from_slice(values.values());
            }
        }
        assert_eq!(actual, expected);
        std::fs::remove_dir_all(root)?;
        crate::store::forget_shuffle(shuffle_id);
        Ok(())
    }

    #[tokio::test]
    async fn streaming_write_tracking_covers_idle_queues_and_cancellation() -> DaftResult<()> {
        for close in [true, false] {
            let shuffle_id = rand::random::<u64>();
            let root = std::env::temp_dir().join(format!("daft-cache-write-drain-{shuffle_id}"));
            let cache = InProgressShuffleCache::try_new(
                0,
                1,
                dummy_schema(),
                &[root.to_string_lossy().into_owned()],
                shuffle_id,
                1024,
                None,
            )?;
            // The writer is still waiting for input, with no blocking operation
            // yet. Cleanup must not mistake this queue for a completed writer.
            assert_eq!(
                daft_io::shuffle_file::active_shuffle_writes_for(&[shuffle_id]),
                1
            );
            crate::store::forget_shuffle(shuffle_id);
            assert_eq!(
                daft_io::shuffle_file::active_shuffle_writes_for(&[shuffle_id]),
                1
            );
            cache.push_partition_data(make_dummy_mp(10)).await?;
            if close {
                assert_eq!(cache.close().await?.num_rows, 10);
            }
            drop(cache);
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while daft_io::shuffle_file::active_shuffle_writes_for(&[shuffle_id]) != 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            std::fs::remove_dir_all(root)?;
            crate::store::forget_shuffle(shuffle_id);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_shuffle_cache_basic() -> DaftResult<()> {
        // Create dummy writer for testing
        let dummy_writer_factory = DummyWriterFactory {};
        let dummy_writer_factory =
            make_dummy_target_file_size_writer_factory(100, 1.0, Arc::new(dummy_writer_factory));
        let writer = dummy_writer_factory.create_writer(0, None)?;

        // Create the cache with dummy writers
        let cache = InProgressShuffleCache::try_new_with_writer(writer, 0, dummy_schema())?;

        // Create and push some partitions
        // Since we have 1 partition, all data goes to partition 0
        let mp1 = make_dummy_mp(100);
        let mp2 = make_dummy_mp(200);

        cache.push_partition_data(mp1).await?;
        cache.push_partition_data(mp2).await?;

        // Close the cache and verify results
        let partition_cache = cache.close().await?;

        // We should have 3 file paths because we wrote 300 bytes and the target filesize is 100
        assert_eq!(partition_cache.file_paths.len(), 3);

        // Check that bytes were distributed
        let total_bytes: usize = partition_cache.bytes_per_file.iter().sum();

        // We should have recorded bytes for our two micropartitions
        assert!(total_bytes == 300);

        Ok(())
    }

    #[tokio::test]
    async fn test_shuffle_cache_with_empty_partitions() -> DaftResult<()> {
        let dummy_writer_factory = DummyWriterFactory {};
        let dummy_writer_factory =
            make_dummy_target_file_size_writer_factory(100, 1.0, Arc::new(dummy_writer_factory));
        let writer = dummy_writer_factory.create_writer(0, None)?;

        let cache = InProgressShuffleCache::try_new_with_writer(writer, 0, dummy_schema())?;

        // Push 1000 empty partitions
        for _ in 0..1000 {
            let empty_partition = make_dummy_mp(0);
            cache.push_partition_data(empty_partition).await?;
        }

        let partition_cache = cache.close().await?;

        // Even though we pushed empty partitions, we should still have the schema
        assert!(partition_cache.schema.names() == vec!["ints"]);
        assert_eq!(partition_cache.file_paths.len(), 0);
        assert!(
            partition_cache
                .file_paths
                .iter()
                .all(|paths| paths.is_empty()),
            "All partitions should have no file paths: {:?}",
            partition_cache.file_paths
        );

        Ok(())
    }
    #[tokio::test]
    async fn test_shuffle_cache_with_failing_writer() -> DaftResult<()> {
        // Create failing writers for testing
        // First writer fails on write
        let failing_writer_factory = FailingWriterFactory::new_fail_on_write();
        let failing_writer_factory =
            make_dummy_target_file_size_writer_factory(100, 1.0, Arc::new(failing_writer_factory));
        let writer = failing_writer_factory.create_writer(0, None)?;

        // Create the cache with writers
        let cache = InProgressShuffleCache::try_new_with_writer(writer, 0, dummy_schema())?;

        let mut found_failure = false;
        // Technically, we can calculate the max number of iterations before failure, based on number of tasks and channel sizes,
        // but 100 should be good enough based on our testing environment.
        let num_iterations = 100;
        for _ in 0..num_iterations {
            let partition = make_dummy_mp(100);
            if let Err(err) = cache.push_partition_data(partition).await {
                // Verify the error message
                let error_message = err.to_string();
                assert!(
                    error_message.contains("Intentional failure in FailingWriter::write"),
                    "Error message should mention write failure: {}",
                    error_message
                );
                found_failure = true;
                break;
            }
        }

        // Assert that the loop did not complete
        assert!(
            found_failure,
            "Expected failure before completing all pushes, num_iterations: {}",
            num_iterations
        );

        // Assert that another push will fail
        let partition = make_dummy_mp(100);
        let result = cache.push_partition_data(partition).await;
        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("Intentional failure in FailingWriter::write"),
            "Error message should mention write failure: {}",
            error_message
        );

        // Try that closing the cache will fail
        let result = cache.close().await;
        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("Intentional failure in FailingWriter::write"),
            "Error message should mention write failure: {}",
            error_message
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_shuffle_cache_with_failing_writer_on_close() -> DaftResult<()> {
        // Create failing writers for testing

        // First writer fails on close
        let failing_writer_factory = FailingWriterFactory::new_fail_on_close();
        let failing_writer_factory =
            make_dummy_target_file_size_writer_factory(100, 1.0, Arc::new(failing_writer_factory));
        let writer = failing_writer_factory.create_writer(0, None)?;

        // Create the cache with writers
        let cache = InProgressShuffleCache::try_new_with_writer(writer, 0, dummy_schema())?;

        // Create and push a partition
        let partitions = vec![make_dummy_mp(100), make_dummy_mp(100)];

        for partition in partitions {
            // This should succeed since the failure happens on close
            cache.push_partition_data(partition).await?;
        }

        // When we close, we should get an error
        let result = cache.close().await;
        assert!(result.is_err());

        // Verify the error message
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("Intentional failure in FailingWriter::close"),
            "Error message should mention close failure: {}",
            error_message
        );

        // Try that another push will fail
        let partition = make_dummy_mp(100);
        let result = cache.push_partition_data(partition).await;
        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("Intentional failure in FailingWriter::close"),
            "Error message should mention close failure: {}",
            error_message
        );

        // Try that closing the cache will fail
        let result = cache.close().await;
        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(
            error_message.contains("Intentional failure in FailingWriter::close"),
            "Error message should mention close failure: {}",
            error_message
        );

        Ok(())
    }
}
