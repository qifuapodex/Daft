//! Opt-in, in-process EIO recovery for immutable shuffle reads and private writes.
//! No file-format changes or reopen/resume of published streams. Recovered writes
//! require a same-descriptor data sync before returning an output for publication.
use std::{
    fs::File,
    future::Future,
    io::{self, Read, Seek, SeekFrom, Write},
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, ready},
    time::Duration,
};

use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

// Amortize recovery I/O calls on shared filesystems while bounding temporary
// memory per recovering writer. This does not affect the on-disk layout.
const WRITE_RECOVERY_BUFFER_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EioRetryPolicy {
    pub max_retries: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl EioRetryPolicy {
    pub fn backoff(self, retries: u32) -> Duration {
        Duration::from_millis(
            self.initial_backoff_ms
                .saturating_mul(1u64.checked_shl(retries).unwrap_or(u64::MAX))
                .min(self.max_backoff_ms),
        )
    }
}

#[derive(Debug, Default)]
struct WriteCancellationState {
    cancelled: AtomicBool,
    lock: Mutex<()>,
    wake: Condvar,
}

/// Cancellation for synchronous file operations. A syscall already in progress
/// cannot be interrupted here; its owner must remain tracked until it returns.
#[derive(Clone, Debug, Default)]
pub struct WriteCancellation(Arc<WriteCancellationState>);

pub struct CancelWriteOnDrop(WriteCancellation);

impl Drop for CancelWriteOnDrop {
    fn drop(&mut self) {
        let _lock = self.0.0.lock.lock().unwrap();
        self.0.0.cancelled.store(true, Ordering::Release);
        self.0.0.wake.notify_all();
    }
}

impl WriteCancellation {
    pub fn guard(&self) -> CancelWriteOnDrop {
        CancelWriteOnDrop(self.clone())
    }

    pub fn check(&self) -> io::Result<()> {
        if self.0.cancelled.load(Ordering::Acquire) {
            // Interrupted would make Write::write_all retry cancellation forever.
            Err(io::Error::other("Shuffle write cancelled"))
        } else {
            Ok(())
        }
    }

    fn wait(&self, delay: Duration) -> io::Result<()> {
        let lock = self.0.lock.lock().unwrap();
        let _wait = self
            .0
            .wake
            .wait_timeout_while(lock, delay, |_| !self.0.cancelled.load(Ordering::Acquire))
            .unwrap();
        self.check()
    }
}

static ACTIVE_WRITES: AtomicUsize = AtomicUsize::new(0);

/// Move this guard into the blocking closure, so cancellation of its async
/// caller cannot make cleanup mistake an outstanding write for a drained one.
pub struct ActiveShuffleWrite {
    _private: (),
}

impl ActiveShuffleWrite {
    pub fn track() -> Self {
        ACTIVE_WRITES.fetch_add(1, Ordering::AcqRel);
        Self { _private: () }
    }
}

impl Drop for ActiveShuffleWrite {
    fn drop(&mut self) {
        ACTIVE_WRITES.fetch_sub(1, Ordering::AcqRel);
    }
}

pub fn active_shuffle_writes() -> usize {
    ACTIVE_WRITES.load(Ordering::Acquire)
}

/// The budget is cumulative for a file handle, not reset on every successful I/O.
pub struct EioRetryBudget {
    policy: EioRetryPolicy,
    used: u32,
    path: String,
    cancellation: Option<WriteCancellation>,
}

impl EioRetryBudget {
    pub fn new(policy: EioRetryPolicy, path: &str) -> Self {
        Self {
            policy,
            used: 0,
            path: path.into(),
            cancellation: None,
        }
    }

    pub fn with_cancellation(mut self, cancellation: WriteCancellation) -> Self {
        self.cancellation = Some(cancellation);
        self
    }

    fn check_cancelled(&self) -> io::Result<()> {
        self.cancellation
            .as_ref()
            .map_or(Ok(()), WriteCancellation::check)
    }

    fn retry(&mut self, error: io::Error, operation: &str) -> io::Result<()> {
        match self.delay(&error, operation) {
            Some(delay) => match &self.cancellation {
                Some(cancellation) => cancellation.wait(delay),
                None => {
                    std::thread::sleep(delay);
                    Ok(())
                }
            },
            None => Err(error),
        }
    }

    fn delay(&mut self, error: &io::Error, operation: &str) -> Option<Duration> {
        if !cfg!(unix) || error.raw_os_error() != Some(5) || self.used >= self.policy.max_retries {
            return None;
        }
        let delay = self.policy.backoff(self.used);
        self.used += 1;
        tracing::warn!(target: "daft_shuffle_io_retry", path = %self.path,
            operation, retry = self.used, max_retries = self.policy.max_retries,
            error = %error, errno = error.raw_os_error(), backoff_secs = delay.as_secs_f64(),
            backoff_ms = delay.as_millis(), "Retrying shuffle file I/O in process");
        Some(delay)
    }

    /// Only use for operations safe to repeat against the same private file.
    pub fn run<T>(
        &mut self,
        operation: &str,
        mut f: impl FnMut() -> io::Result<T>,
    ) -> io::Result<T> {
        loop {
            self.check_cancelled()?;
            match f() {
                Ok(value) => return Ok(value),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => self.retry(error, operation)?,
            }
        }
    }
}

enum ReadState {
    Ready,
    Backoff(Pin<Box<tokio::time::Sleep>>),
    Seeking,
    ExternalSeeking,
}

/// Retries below the IPC parser and checksum reader. Only successfully returned
/// bytes advance the cursor; a failed read's bytes never reach either consumer.
pub struct RetryReader<R> {
    inner: R,
    offset: u64,
    budget: EioRetryBudget,
    state: ReadState,
}

impl RetryReader<tokio::fs::File> {
    pub async fn open(path: &str, policy: EioRetryPolicy) -> io::Result<Self> {
        let mut budget = EioRetryBudget::new(policy, path);
        let file = loop {
            match tokio::fs::File::open(path).await {
                Ok(file) => break file,
                Err(error) => match budget.delay(&error, "open") {
                    Some(delay) => tokio::time::sleep(delay).await,
                    None => return Err(error),
                },
            }
        };
        Ok(Self {
            inner: file,
            offset: 0,
            budget,
            state: ReadState::Ready,
        })
    }
}

impl<R: AsyncRead + AsyncSeek + Unpin> AsyncRead for RetryReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            match &mut this.state {
                ReadState::ExternalSeeking => {
                    ready!(Pin::new(&mut *this).poll_complete(cx))?;
                }
                ReadState::Backoff(delay) => {
                    ready!(delay.as_mut().poll(cx));
                    // Keep the same FD/attempt; never reopen or switch producer output.
                    Pin::new(&mut this.inner).start_seek(SeekFrom::Start(this.offset))?;
                    this.state = ReadState::Seeking;
                }
                ReadState::Seeking => {
                    let offset = ready!(Pin::new(&mut this.inner).poll_complete(cx))?;
                    if offset != this.offset {
                        return Poll::Ready(Err(io::Error::other(
                            "Shuffle retry seek changed position",
                        )));
                    }
                    this.state = ReadState::Ready;
                }
                ReadState::Ready => {
                    // Reuse the caller's allocation, but publish progress only on success.
                    let mut scratch = ReadBuf::new(buf.initialize_unfilled());
                    match ready!(Pin::new(&mut this.inner).poll_read(cx, &mut scratch)) {
                        Ok(()) => {
                            let n = scratch.filled().len();
                            this.offset += n as u64;
                            buf.advance(n);
                            return Poll::Ready(Ok(()));
                        }
                        Err(error) => match this.budget.delay(&error, "read") {
                            Some(delay) => {
                                this.state = ReadState::Backoff(Box::pin(tokio::time::sleep(delay)))
                            }
                            None => return Poll::Ready(Err(error)),
                        },
                    }
                }
            }
        }
    }
}

impl<R: AsyncSeek + Unpin> AsyncSeek for RetryReader<R> {
    fn start_seek(self: Pin<&mut Self>, pos: SeekFrom) -> io::Result<()> {
        let this = self.get_mut();
        // A caller cancelling a read may discard the retry, but not silently use
        // the underlying descriptor's uncertain position for a relative seek.
        if !matches!(this.state, ReadState::Ready) {
            return Err(io::Error::other(
                "Seek during an unfinished shuffle read retry",
            ));
        }
        Pin::new(&mut this.inner).start_seek(pos)?;
        this.state = ReadState::ExternalSeeking;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let this = self.get_mut();
        match this.state {
            ReadState::Ready => return Poll::Ready(Ok(this.offset)),
            ReadState::Backoff(_) | ReadState::Seeking => {
                return Poll::Ready(Err(io::Error::other(
                    "Seek during an unfinished shuffle read retry",
                )));
            }
            ReadState::ExternalSeeking => {}
        }
        let offset = ready!(Pin::new(&mut this.inner).poll_complete(cx))?;
        this.offset = offset;
        this.state = ReadState::Ready;
        Poll::Ready(Ok(offset))
    }
}

/// Sequential writer for an unpublished, exclusively owned file. Positioned
/// writes make short writes and an EIO's unspecified file offset unambiguous.
/// The caller's bytes remain borrowed until the write succeeds or exhausts its
/// budget; no whole-partition copy or extra steady-state disk pass is needed.
/// Owns the file's tail from its initial position through EOF. Existing prefixes
/// are allowed; retaining an existing tail or pre-extending past the final write
/// is not. Recovery validates that the final file length matches this region.
pub struct RetryWriter {
    file: File,
    start: u64,
    offset: u64,
    hasher: crc32fast::Hasher,
    budget: EioRetryBudget,
    recovered_write: bool,
    terminal_error: Option<io::Error>,
}

impl RetryWriter {
    pub fn new(mut file: File, budget: EioRetryBudget) -> io::Result<Self> {
        let offset = file.stream_position()?;
        Ok(Self {
            file,
            start: offset,
            offset,
            hasher: crc32fast::Hasher::new(),
            budget,
            recovered_write: false,
            terminal_error: None,
        })
    }

    /// Replace the cancellation scope when a streaming writer moves to a new
    /// async write/close operation. Only the current operation owns this writer.
    pub fn set_cancellation(&mut self, cancellation: WriteCancellation) {
        self.budget.cancellation = Some(cancellation);
    }

    /// A write EIO may report earlier failed writeback. Re-read and validate the
    /// whole owned region, re-dirty it, then sync it on the original descriptor.
    /// A failed recovery I/O restarts the pass within the remaining file budget;
    /// checksum mismatches and sync failures are terminal, never synced away.
    pub fn finish(mut self) -> io::Result<File> {
        if let Some(error) = self.terminal_error.take() {
            return Err(error);
        }
        self.budget.check_cancelled()?;
        if self.recovered_write {
            let mut buf = vec![
                0u8;
                (self.offset - self.start).min(WRITE_RECOVERY_BUFFER_BYTES as u64)
                    as usize
            ];
            let hasher = loop {
                match self.rewrite_region(&mut buf) {
                    Ok(hasher) => break hasher,
                    Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                        // Integrity loss after a write EIO still needs task
                        // replay, rather than retrying an incomplete file.
                        return Err(io::Error::from_raw_os_error(5));
                    }
                    Err(error) => self.budget.retry(error, "recover read/write")?,
                }
            };
            if hasher.finalize() != self.hasher.clone().finalize()
                || self.file.metadata()?.len() != self.offset
            {
                return Err(io::Error::from_raw_os_error(5));
            }
            self.budget.check_cancelled()?;
            // A fresh FD may miss an earlier writeback error. Do not retry an
            // EIO from sync: a later successful sync alone cannot repair data.
            loop {
                self.budget.check_cancelled()?;
                match self.file.sync_data() {
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    result => {
                        result?;
                        break;
                    }
                }
            }
            self.budget.check_cancelled()?;
            tracing::info!(target: "daft_shuffle_io_retry", path = %self.budget.path,
                verified_bytes = self.offset - self.start,
                "Verified, rewrote and synced shuffle data after write EIO recovery");
        }
        self.file.seek(SeekFrom::Start(self.offset))?;
        Ok(self.file)
    }

    fn rewrite_region(&mut self, buf: &mut [u8]) -> io::Result<crc32fast::Hasher> {
        self.budget.check_cancelled()?;
        self.file.seek(SeekFrom::Start(self.start))?;
        let mut remaining = self.offset - self.start;
        let mut hasher = crc32fast::Hasher::new();
        while remaining > 0 {
            self.budget.check_cancelled()?;
            let size = remaining.min(buf.len() as u64) as usize;
            let n = match self.file.read(&mut buf[..size]) {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                result => result?,
            };
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Shuffle recovery read ended before the written data",
                ));
            }
            hasher.update(&buf[..n]);
            let offset = self.offset - remaining;
            let mut written = 0;
            while written < n {
                self.budget.check_cancelled()?;
                #[cfg(unix)]
                let result = std::os::unix::fs::FileExt::write_at(
                    &self.file,
                    &buf[written..n],
                    offset + written as u64,
                );
                #[cfg(not(unix))]
                let result = {
                    self.file.seek(SeekFrom::Start(offset + written as u64))?;
                    self.file.write(&buf[written..n])
                };
                let count = match result {
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    result => result?,
                };
                if count == 0 {
                    return Err(io::ErrorKind::WriteZero.into());
                }
                written += count;
            }
            #[cfg(not(unix))]
            self.file.seek(SeekFrom::Start(offset + n as u64))?;
            remaining -= n as u64;
        }
        Ok(hasher)
    }
}

impl Write for RetryWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if let Some(error) = &self.terminal_error {
            return Err(copy_io_error(error));
        }
        let before = self.budget.used;
        let file = &mut self.file;
        let offset = self.offset;
        let result = self.budget.run("write", || {
            #[cfg(unix)]
            {
                std::os::unix::fs::FileExt::write_at(file, buf, offset)
            }
            #[cfg(not(unix))]
            {
                file.seek(SeekFrom::Start(offset))?;
                file.write(buf)
            }
        });
        self.recovered_write |= self.budget.used != before;
        let n = match result {
            Ok(n) => n,
            Err(error) => {
                // In particular, BufWriter::drop must not issue more syscalls
                // after this file's retry budget has already been exhausted.
                self.terminal_error = Some(copy_io_error(&error));
                return Err(error);
            }
        };
        self.offset += n as u64;
        if self.budget.policy.max_retries > 0 {
            self.hasher.update(&buf[..n]);
        }
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(error) = &self.terminal_error {
            return Err(copy_io_error(error));
        }
        self.file.flush()
    }
}

fn copy_io_error(error: &io::Error) -> io::Error {
    error.raw_os_error().map_or_else(
        || io::Error::new(error.kind(), error.to_string()),
        io::Error::from_raw_os_error,
    )
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    use super::*;

    struct FaultReader {
        data: Vec<u8>,
        pos: u64,
        calls: usize,
        fail_calls: Vec<usize>,
        errno: i32,
    }

    impl AsyncRead for FaultReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            self.calls += 1;
            if self.fail_calls.contains(&self.calls) {
                // A failed syscall may leave its cursor uncertain. Even bytes
                // placed into the failed read's buffer must not be published.
                buf.put_slice(&[255]);
                self.pos += 2;
                return Poll::Ready(Err(io::Error::from_raw_os_error(self.errno)));
            }
            let pos = self.pos as usize;
            let n = 3
                .min(buf.remaining())
                .min(self.data.len().saturating_sub(pos));
            buf.put_slice(&self.data[pos..pos + n]);
            self.pos += n as u64;
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncSeek for FaultReader {
        fn start_seek(mut self: Pin<&mut Self>, pos: SeekFrom) -> io::Result<()> {
            self.pos = match pos {
                SeekFrom::Start(n) => n,
                SeekFrom::Current(n) => (self.pos as i64 + n) as u64,
                SeekFrom::End(n) => (self.data.len() as i64 + n) as u64,
            };
            Ok(())
        }
        fn poll_complete(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<u64>> {
            Poll::Ready(Ok(self.pos))
        }
    }

    fn reader(errno: i32, retries: u32) -> RetryReader<FaultReader> {
        RetryReader {
            inner: FaultReader {
                data: (0..32).collect(),
                pos: 0,
                calls: 0,
                fail_calls: vec![2, 5],
                errno,
            },
            offset: 0,
            budget: EioRetryBudget::new(
                EioRetryPolicy {
                    max_retries: retries,
                    ..Default::default()
                },
                "test",
            ),
            state: ReadState::Ready,
        }
    }

    #[tokio::test]
    async fn partial_reads_retry_without_repeating_or_dropping_bytes() {
        let mut reader = reader(5, 2);
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await.unwrap();
        assert_eq!(data, (0..32).collect::<Vec<_>>());
        assert_eq!(reader.budget.used, 2);
        reader.seek(SeekFrom::Start(10)).await.unwrap();
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).await.unwrap();
        assert_eq!(tail, (10..32).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn read_budget_is_cumulative_and_permanent_errors_are_not_retried() {
        for (errno, retries, used) in [(5, 1, 1), (5, 0, 0), (13, 3, 0), (28, 3, 0)] {
            let mut reader = reader(errno, retries);
            let error = reader.read_to_end(&mut Vec::new()).await.unwrap_err();
            assert_eq!(error.raw_os_error(), Some(errno));
            assert_eq!(reader.budget.used, used);
        }
    }

    #[tokio::test]
    async fn cancelling_a_backoff_does_not_deliver_partial_bytes() {
        let mut reader = reader(5, 2);
        reader.budget.policy.initial_backoff_ms = 1000;
        reader.budget.policy.max_backoff_ms = 1000;
        let mut data = [0; 3];
        reader.read_exact(&mut data).await.unwrap();
        assert_eq!(data, [0, 1, 2]);
        let mut next = [42; 3];
        assert!(
            tokio::time::timeout(Duration::from_millis(10), reader.read_exact(&mut next))
                .await
                .is_err()
        );
        assert_eq!(reader.offset, 3);
        // The caller must discard the cancelled buffer; no progress was reported.
        assert_eq!(reader.budget.used, 1);
    }

    #[test]
    fn backoff_is_bounded_and_zero_can_disable_waiting() {
        let policy = EioRetryPolicy {
            max_retries: 6,
            initial_backoff_ms: 1000,
            max_backoff_ms: 32000,
        };
        for (attempt, expected) in [
            (0, 1),
            (1, 2),
            (2, 4),
            (3, 8),
            (4, 16),
            (5, 32),
            (6, 32),
            (u32::MAX, 32),
        ] {
            assert_eq!(policy.backoff(attempt), Duration::from_secs(expected));
        }
    }

    #[test]
    fn interrupted_syscalls_do_not_poison_or_consume_the_eio_budget() {
        let mut budget = EioRetryBudget::new(EioRetryPolicy::default(), "test");
        let mut calls = 0;
        let result = budget
            .run("write", || {
                calls += 1;
                if calls == 1 {
                    Err(io::ErrorKind::Interrupted.into())
                } else {
                    Ok(7)
                }
            })
            .unwrap();
        assert_eq!(result, 7);
        assert_eq!(calls, 2);
        assert_eq!(budget.used, 0);
    }

    #[test]
    fn cancellation_wakes_a_long_backoff_without_another_syscall() {
        let cancellation = WriteCancellation::default();
        let guard = cancellation.guard();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            let mut budget = EioRetryBudget::new(
                EioRetryPolicy {
                    max_retries: 6,
                    initial_backoff_ms: 32_000,
                    max_backoff_ms: 32_000,
                },
                "test",
            )
            .with_cancellation(cancellation);
            let mut calls = 0;
            let result: io::Result<()> = budget.run("write", || {
                calls += 1;
                started_tx.send(()).unwrap();
                Err(io::Error::from_raw_os_error(5))
            });
            done_tx.send((calls, result.unwrap_err().kind())).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        drop(guard);
        let (calls, kind) = done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        handle.join().unwrap();
        assert_eq!(calls, 1);
        assert_ne!(kind, io::ErrorKind::Interrupted);
    }

    #[tokio::test]
    async fn pending_external_seek_is_completed_before_read_recovery() {
        let mut reader = reader(5, 1);
        reader.inner.fail_calls = vec![1];
        Pin::new(&mut reader)
            .start_seek(SeekFrom::Start(10))
            .unwrap();
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).await.unwrap();
        assert_eq!(tail, (10..32).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn rejected_seek_does_not_change_a_cancelled_reads_recovery_offset() {
        let mut reader = reader(5, 1);
        reader.inner.fail_calls = vec![2];
        reader.budget.policy.initial_backoff_ms = 30;
        reader.budget.policy.max_backoff_ms = 30;
        let mut prefix = [0; 3];
        reader.read_exact(&mut prefix).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(1), reader.read_exact(&mut [0; 3]))
                .await
                .is_err()
        );
        assert!(reader.seek(SeekFrom::Start(0)).await.is_err());
        assert_eq!(reader.offset, 3);
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).await.unwrap();
        assert_eq!(tail, (3..32).collect::<Vec<_>>());
    }

    #[test]
    fn write_recovery_preserves_reserved_prefix_and_detects_earlier_corruption() {
        for corrupt in [false, true] {
            let mut file = tempfile::tempfile().unwrap();
            file.write_all(b"index").unwrap();
            let mut writer = RetryWriter::new(
                file,
                EioRetryBudget::new(
                    EioRetryPolicy {
                        max_retries: 1,
                        ..Default::default()
                    },
                    "test",
                ),
            )
            .unwrap();
            writer.write_all(b"first chunk, second chunk").unwrap();
            writer.recovered_write = true;
            if corrupt {
                writer.file.seek(SeekFrom::Start(5)).unwrap();
                writer.file.write_all(b"!").unwrap();
            }
            let result = writer.finish();
            if corrupt {
                assert_eq!(result.unwrap_err().raw_os_error(), Some(5));
            } else {
                let mut file = result.unwrap();
                file.rewind().unwrap();
                let mut data = Vec::new();
                file.read_to_end(&mut data).unwrap();
                assert_eq!(data, b"indexfirst chunk, second chunk");
            }
        }
    }
}
