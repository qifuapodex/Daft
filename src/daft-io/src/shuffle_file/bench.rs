//! Normal-path reads through Take, which gives its inner reader an uninitialized
//! view on every poll, as in the Flight CheckedRange path.
//!
//! DAFT_SHUFFLE_BENCH_DIR=/path/to/mount cargo test -p daft-io --release \
//!     bench_normal_shuffle_reads -- --ignored --nocapture --test-threads=1

use std::{path::Path, time::Instant};

use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::*;

const READS: usize = 256;

async fn measure(reader: &mut (impl AsyncRead + AsyncSeek + Unpin), expected: &[u8]) -> Duration {
    let mut buf = vec![0; expected.len()];
    let mut elapsed = Duration::ZERO;
    for _ in 0..READS {
        reader.rewind().await.unwrap();
        buf.fill(255);
        let start = Instant::now();
        // Match the bounded reads below Flight's checksum adapter. A plain
        // read_exact on this initialized Vec would miss Take's initialization
        // behavior and would not reproduce the extra clearing.
        reader
            .take(expected.len() as u64)
            .read_exact(&mut buf)
            .await
            .unwrap();
        elapsed += start.elapsed();
        assert_eq!(buf, expected);
    }
    elapsed
}

async fn sample(path: &Path, retry: bool, expected: &[u8]) -> Duration {
    if retry {
        let mut reader = RetryReader::open(
            path.to_str().unwrap(),
            EioRetryPolicy {
                max_retries: 6,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        measure(&mut reader, expected).await
    } else {
        let mut reader = tokio::fs::File::open(path).await.unwrap();
        measure(&mut reader, expected).await
    }
}

#[tokio::test]
#[ignore = "manual read microbenchmark; retain every measured sample"]
async fn bench_normal_shuffle_reads() {
    let root = std::env::var_os("DAFT_SHUFFLE_BENCH_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = tempfile::tempdir_in(root).unwrap();
    let path = dir.path().join("input.arrow");
    println!("case,mode,warmup,sample,seconds,logical_bytes");
    for (case, size) in [("small_reads", 4096), ("wide_reads", 4 * 1024 * 1024)] {
        let expected: Vec<_> = (0..size).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &expected).unwrap();
        // One warmup per mode, then three ABBA blocks: six samples per mode.
        // This is a warm-cache measurement; never clear global caches or
        // silently remove a slow sample. Preparation, seeks, buffer resets,
        // verification, and file cleanup are all outside the timed interval.
        for (index, retry) in [false, true]
            .into_iter()
            .chain([false, true, true, false].into_iter().cycle().take(12))
            .enumerate()
        {
            let seconds = sample(&path, retry, &expected).await.as_secs_f64();
            println!(
                "{case},{},{},{index},{seconds:.9},{}",
                if retry { "RetryReader" } else { "File" },
                index < 2,
                READS * size
            );
        }
    }
}
