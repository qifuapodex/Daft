use std::{fs::File, sync::Arc};

use async_trait::async_trait;
use common_error::DaftResult;
use daft_core::{
    prelude::{DataType, Field, Schema},
    series::Series,
};
use daft_io::shuffle_file::{EioRetryBudget, EioRetryPolicy, RetryWriter};
use daft_micropartition::MicroPartition;
use daft_recordbatch::RecordBatch;

use crate::{AsyncFileWriter, RETURN_PATHS_COLUMN_NAME, WriteResult, WriterFactory};

pub struct IPCWriter {
    is_closed: bool,
    bytes_written: usize,
    file_path: String,
    compression: Option<arrow_ipc::CompressionType>,
    retry_policy: EioRetryPolicy,
    writer: Option<arrow_ipc::writer::StreamWriter<RetryWriter>>,
}

impl IPCWriter {
    pub fn new(file_path: &str, compression: Option<arrow_ipc::CompressionType>) -> Self {
        Self {
            is_closed: false,
            bytes_written: 0,
            file_path: file_path.to_string(),
            compression,
            writer: None,
            retry_policy: EioRetryPolicy::default(),
        }
    }

    fn get_or_create_writer(
        &mut self,
        schema: &Schema,
    ) -> DaftResult<&mut arrow_ipc::writer::StreamWriter<RetryWriter>> {
        if self.writer.is_none() {
            let mut budget = EioRetryBudget::new(self.retry_policy, &self.file_path);
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
        Ok(self.writer.as_mut().unwrap())
    }
}

#[async_trait]
impl AsyncFileWriter for IPCWriter {
    type Input = MicroPartition;
    type Result = Option<RecordBatch>;

    async fn write(&mut self, data: Self::Input) -> DaftResult<WriteResult> {
        assert!(!self.is_closed, "Writer is closed");

        let size_bytes = data.size_bytes();
        let rows_written = data.len();
        // Encoding and bounded synchronous I/O retries must not block an async
        // executor thread. Move the private writer into the blocking operation.
        let mut state = IPCWriter::new(&self.file_path, self.compression);
        state.retry_policy = self.retry_policy;
        state.writer = self.writer.take();
        self.writer = common_runtime::get_io_runtime(true)
            .spawn_blocking(move || -> DaftResult<_> {
                let writer = state.get_or_create_writer(&data.schema())?;
                for table in data.record_batches() {
                    let arrow_batch: arrow_array::RecordBatch = table.clone().try_into()?;
                    writer.write(&arrow_batch)?;
                }
                Ok(state.writer)
            })
            .await??;

        // Track bytes written (approximate, since we can't easily get exact bytes from arrow-ipc)
        self.bytes_written += size_bytes;
        Ok(WriteResult {
            bytes_written: size_bytes,
            rows_written,
        })
    }

    async fn close(&mut self) -> DaftResult<Self::Result> {
        if let Some(mut writer) = self.writer.take() {
            common_runtime::get_io_runtime(true)
                .spawn_blocking(move || -> DaftResult<()> {
                    writer.finish()?;
                    writer.into_inner()?.finish()?;
                    Ok(())
                })
                .await??;
        }
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
    dir: String,
    compression: Option<arrow_ipc::CompressionType>,
}

impl IPCWriterFactory {
    pub fn new(dir: String, compression: Option<arrow_ipc::CompressionType>) -> Self {
        Self {
            dir,
            compression,
            retry_policy: EioRetryPolicy::default(),
        }
    }
}

impl IPCWriterFactory {
    pub fn with_retry_policy(mut self, policy: EioRetryPolicy) -> Self {
        self.retry_policy = policy;
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
        Ok(Box::new(writer))
    }
}
