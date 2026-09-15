use std::{fs::File, sync::Arc};

use async_trait::async_trait;
use common_error::{DaftError, DaftResult};
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_io::shuffle_file::{
    ActiveShuffleWrite, EioRetryBudget, EioRetryPolicy, RetryWriter, WriteCancellation,
};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::{AsyncFileWriter, RETURN_PATHS_COLUMN_NAME, WriteResult, WriterFactory};

pub struct IPCWriter {
    is_closed: bool,
    error: Option<Arc<DaftError>>,
    bytes_written: usize,
    file_path: String,
    compression: Option<arrow_ipc::CompressionType>,
    retry_policy: EioRetryPolicy,
    shuffle_id: Option<u64>,
    writer: Option<arrow_ipc::writer::StreamWriter<RetryWriter>>,
}

impl IPCWriter {
    pub fn new(file_path: &str, compression: Option<arrow_ipc::CompressionType>) -> Self {
        Self {
            is_closed: false,
            error: None,
            bytes_written: 0,
            file_path: file_path.to_string(),
            compression,
            writer: None,
            retry_policy: EioRetryPolicy::default(),
            shuffle_id: None,
        }
    }

    fn check_error(&self) -> DaftResult<()> {
        match &self.error {
            Some(error) => Err(DaftError::Shared(error.clone())),
            None => Ok(()),
        }
    }

    fn remember_error(&mut self, error: DaftError) -> DaftError {
        let error = Arc::new(error);
        self.error = Some(error.clone());
        DaftError::Shared(error)
    }

    fn mark_incomplete(&mut self) {
        self.error = Some(Arc::new(DaftError::InternalError(
            "IPC write or close was cancelled before completion".into(),
        )));
    }

    fn get_or_create_writer(
        &mut self,
        schema: &Schema,
        cancellation: &WriteCancellation,
    ) -> DaftResult<&mut arrow_ipc::writer::StreamWriter<RetryWriter>> {
        if self.writer.is_none() {
            let mut budget = EioRetryBudget::new(self.retry_policy, &self.file_path)
                .with_cancellation(cancellation.clone());
            let file = budget.run("create", || {
                File::options()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&self.file_path)
            })?;
            let file = RetryWriter::new(file, budget)?;

            let arrow_schema = schema.to_arrow()?;
            let write_options = arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(self.compression)?;

            let writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
                file,
                &arrow_schema,
                write_options,
            )?;
            self.writer = Some(writer);
        }
        let writer = self.writer.as_mut().unwrap();
        writer.get_mut().set_cancellation(cancellation.clone());
        Ok(writer)
    }
}

#[async_trait]
impl AsyncFileWriter for IPCWriter {
    type Input = MicroPartition;
    type Result = Option<RecordBatch>;

    async fn write(&mut self, data: Self::Input) -> DaftResult<WriteResult> {
        self.check_error()?;
        if self.is_closed {
            return Err(DaftError::ValueError("IPC writer is closed".into()));
        }
        let size_bytes = data.size_bytes();
        let rows_written = data.len();
        let cancellation = WriteCancellation::default();
        let _cancel_on_drop = cancellation.guard();
        let active = ActiveShuffleWrite::for_shuffle(self.shuffle_id);
        // Poison before yielding: dropping the future must not leave a reusable
        // writer that truncates the previous file or reports a successful close.
        self.mark_incomplete();
        let mut state = IPCWriter::new(&self.file_path, self.compression);
        state.retry_policy = self.retry_policy;
        state.shuffle_id = self.shuffle_id;
        state.writer = self.writer.take();
        let result = common_runtime::get_io_runtime(true)
            .spawn_blocking(move || -> DaftResult<_> {
                let _active = active;
                cancellation.check()?;
                let writer = state.get_or_create_writer(&data.schema(), &cancellation)?;
                for table in data.record_batches() {
                    cancellation.check()?;
                    let arrow_batch: arrow_array::RecordBatch = table.clone().try_into()?;
                    writer.write(&arrow_batch)?;
                }
                cancellation.check()?;
                Ok(state.writer)
            })
            .await
            .and_then(|result| result);
        match result {
            Ok(writer) => {
                self.writer = writer;
                self.error = None;
            }
            Err(error) => return Err(self.remember_error(error)),
        }

        // Track bytes written (approximate, since we can't easily get exact bytes from arrow-ipc)
        self.bytes_written += size_bytes;
        Ok(WriteResult {
            bytes_written: size_bytes,
            rows_written,
        })
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        self.check_error()?;
        if let Some(mut writer) = self.writer.take() {
            self.mark_incomplete();
            let cancellation = WriteCancellation::default();
            let _cancel_on_drop = cancellation.guard();
            let active = ActiveShuffleWrite::for_shuffle(self.shuffle_id);
            writer.get_mut().set_cancellation(cancellation.clone());
            let result = common_runtime::get_io_runtime(true)
                .spawn_blocking(move || -> DaftResult<()> {
                    let _active = active;
                    cancellation.check()?;
                    writer.finish()?;
                    writer.into_inner()?.finish()?;
                    cancellation.check()?;
                    Ok(())
                })
                .await
                .and_then(|result| result);
            if let Err(error) = result {
                return Err(self.remember_error(error));
            }
            self.error = None;
        }
        self.is_closed = true;
        let path_col = Series::from_arrow(
            Arc::new(Field::new(RETURN_PATHS_COLUMN_NAME, DataType::Utf8)),
            Arc::new(arrow_array::LargeStringArray::from_iter_values(
                std::iter::once(self.file_path.clone()),
            )),
        )?;
        let res = RecordBatch::from_nonempty_columns(vec![path_col])?;
        Ok(Some(res))
    }

    fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    fn bytes_per_file(&self) -> Vec<usize> {
        vec![self.bytes_written]
    }
}

pub struct IPCWriterFactory {
    retry_policy: EioRetryPolicy,
    shuffle_id: Option<u64>,
    dir: String,
    compression: Option<arrow_ipc::CompressionType>,
}

impl IPCWriterFactory {
    pub fn new(dir: String, compression: Option<arrow_ipc::CompressionType>) -> Self {
        Self {
            dir,
            compression,
            retry_policy: EioRetryPolicy::default(),
            shuffle_id: None,
        }
    }
}

impl IPCWriterFactory {
    pub fn with_retry_policy(mut self, policy: EioRetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    pub fn with_shuffle_id(mut self, shuffle_id: Option<u64>) -> Self {
        self.shuffle_id = shuffle_id;
        self
    }
}

impl WriterFactory for IPCWriterFactory {
    type Input = MicroPartition;
    type Result = Option<RecordBatch>;

    fn create_writer(
        &self,
        file_idx: usize,
        _partition_values: Option<&RecordBatch>,
    ) -> DaftResult<Box<dyn AsyncFileWriter<Input = Self::Input, Result = Self::Result>>> {
        let file_path = format!("{}/{}.arrow", self.dir, file_idx);
        let mut writer = IPCWriter::new(&file_path, self.compression);
        writer.retry_policy = self.retry_policy;
        writer.shuffle_id = self.shuffle_id;
        Ok(Box::new(writer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::make_dummy_mp;

    #[tokio::test]
    async fn failed_write_keeps_its_error_on_close_and_later_writes() {
        let path = std::env::temp_dir().join(format!(
            "daft-missing-ipc-parent-{}/child/0.arrow",
            uuid::Uuid::new_v4()
        ));
        let mut writer = IPCWriter::new(path.to_str().unwrap(), None);
        let error = writer
            .write(make_dummy_mp(10))
            .await
            .err()
            .unwrap()
            .to_string();
        assert_eq!(writer.close().await.err().unwrap().to_string(), error);
        assert_eq!(
            writer
                .write(make_dummy_mp(20))
                .await
                .err()
                .unwrap()
                .to_string(),
            error
        );
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn successful_close_rejects_later_writes_without_truncating() {
        let path =
            std::env::temp_dir().join(format!("daft-ipc-close-{}.arrow", uuid::Uuid::new_v4()));
        let mut writer = IPCWriter::new(path.to_str().unwrap(), None);
        writer.write(make_dummy_mp(10)).await.unwrap();
        writer.close().await.unwrap();
        let data = std::fs::read(&path).unwrap();
        assert!(writer.write(make_dummy_mp(20)).await.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), data);
        std::fs::remove_file(path).unwrap();
    }
    #[tokio::test]
    #[ignore = "requires LD_PRELOAD shuffle EIO injector"]
    async fn cancelled_write_poisoning_survives_close_and_reuse() {
        let root = std::path::PathBuf::from(std::env::var("DAFT_TEST_SHUFFLE_IO_ROOT").unwrap());
        assert!(!root.join("fault-0").exists());
        std::fs::create_dir_all(root.join("daft_shuffle")).unwrap();
        let path = root.join("daft_shuffle/0.arrow");
        let mut writer = IPCWriter::new(path.to_str().unwrap(), None);
        let shuffle_id = 0xe10_005;
        writer.shuffle_id = Some(shuffle_id);
        writer.retry_policy = EioRetryPolicy {
            max_retries: 6,
            initial_backoff_ms: 32_000,
            max_backoff_ms: 32_000,
        };
        let mut writing = Box::pin(writer.write(make_dummy_mp(1024)));
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::select! {
                result = &mut writing => panic!("write completed before cancellation: {}", result.is_ok()),
                () = async {
                    while !root.join("fault-0").exists() {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                } => {}
            }
        }).await.unwrap();
        drop(writing);
        let error = writer.close().await.err().unwrap().to_string();
        assert!(error.contains("cancelled"));
        assert_eq!(
            writer
                .write(make_dummy_mp(20))
                .await
                .err()
                .unwrap()
                .to_string(),
            error
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while daft_io::shuffle_file::active_shuffle_writes_for(&[shuffle_id]) != 0 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(!root.join("fault-1").exists());
        std::fs::remove_dir_all(root.join("daft_shuffle")).unwrap();
    }

    /// Isolate per-batch scheduling and checksum cost without a Ray startup or
    /// source scan. Run optimized for throughput comparisons; debug runs are
    /// useful only as local diagnostics. Includes IPC decode/row-count checks.
    #[tokio::test]
    #[ignore = "manual streaming IPC scheduling/CRC benchmark"]
    async fn bench_streaming_ipc_write_overheads() {
        for (size, batches) in [(4096, 512), (4 * 1024 * 1024, 32)] {
            let data = make_dummy_mp(size);
            for repeat in 0..3 {
                for mode in ["inline", "inline_crc", "offload_crc"] {
                    let path = std::env::temp_dir()
                        .join(format!("daft-ipc-bench-{}.arrow", uuid::Uuid::new_v4()));
                    let mut writer = IPCWriter::new(path.to_str().unwrap(), None);
                    writer.shuffle_id = Some(0xe10_006);
                    if mode != "inline" {
                        writer.retry_policy.max_retries = 6;
                    }
                    let cancellation = WriteCancellation::default();
                    let started = std::time::Instant::now();
                    for _ in 0..batches {
                        if mode == "offload_crc" {
                            writer.write(data.clone()).await.unwrap();
                        } else {
                            let stream = writer
                                .get_or_create_writer(&data.schema(), &cancellation)
                                .unwrap();
                            for table in data.record_batches() {
                                let batch: arrow_array::RecordBatch =
                                    table.clone().try_into().unwrap();
                                stream.write(&batch).unwrap();
                            }
                        }
                    }
                    writer.close().await.unwrap();
                    let elapsed = started.elapsed();
                    let reader =
                        arrow_ipc::reader::StreamReader::try_new(File::open(&path).unwrap(), None)
                            .unwrap();
                    let rows: usize = reader.map(|batch| batch.unwrap().num_rows()).sum();
                    assert_eq!(rows, size * batches);
                    eprintln!(
                        "IPC_BENCH mode={mode} batch_bytes={size} batches={batches} repeat={repeat} elapsed_us={}",
                        elapsed.as_micros()
                    );
                    std::fs::remove_file(path).unwrap();
                }
            }
        }
    }
}
