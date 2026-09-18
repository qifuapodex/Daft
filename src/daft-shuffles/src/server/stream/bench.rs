//! Manual, warm-cache IPC message read benchmark. Run in matching release
//! builds before/after in ABBA process order; retain all samples and warmups.
//! cargo test -p daft-shuffles --release bench_flight_messages -- \
//!     --ignored --nocapture --test-threads=1

use std::{io::Cursor, time::Instant};

use daft_io::shuffle_file::{EioRetryPolicy, RetryReader};
use tokio::io::AsyncSeekExt;

use super::{tests::fixture, *};
use crate::store::verify::CheckedRange;

async fn sample(
    reader: &mut (impl AsyncRead + AsyncSeek + Unpin),
    len: usize,
    expected: &[FlightData],
    crc: u32,
    reads: usize,
) -> (f64, f64) {
    let mut elapsed = std::time::Duration::ZERO;
    let mut seek_elapsed = std::time::Duration::ZERO;
    for _ in 0..reads {
        let seek_start = Instant::now();
        reader.rewind().await.unwrap();
        let start = Instant::now();
        seek_elapsed += start.duration_since(seek_start);
        let mut range = CheckedRange::new(&mut *reader, len as u64, Some(crc), "bench".into());
        let mut messages = Vec::with_capacity(expected.len());
        while let Some(message) = range.next().await.unwrap() {
            messages.push(message);
        }
        range.finish().unwrap();
        elapsed += start.elapsed();
        assert_eq!(messages, expected);
        // Include message deallocation, but exclude the oracle comparison.
        let start = Instant::now();
        drop(messages);
        elapsed += start.elapsed();
    }
    (
        elapsed.as_secs_f64(),
        (elapsed + seek_elapsed).as_secs_f64(),
    )
}

#[tokio::test]
#[ignore = "manual IPC allocation benchmark; use release and retain all samples"]
async fn bench_flight_messages() {
    let root = std::env::var_os("DAFT_SHUFFLE_BENCH_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let dir = root.join(format!("daft-ipc-bench-{}", std::process::id()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("input.arrow");
    // Keep the original read-only metric and also include seek waiting in a
    // companion metric. Tiny async reads can shift scheduler waiting between
    // the excluded seek and the included read; report both without subtraction.
    let selected_case = std::env::var("DAFT_SHUFFLE_BENCH_CASE").ok();
    assert!(matches!(
        selected_case.as_deref(),
        None | Some("small" | "wide" | "oversized")
    ));
    println!("case,reader,warmup,sample,seconds,logical_bytes,seek_read_seconds");
    for (case, size, reads) in [
        ("small", 4096, 4096),
        ("wide", 4 * 1024 * 1024, 256),
        ("oversized", 16 * 1024 * 1024, 64),
    ] {
        if selected_case
            .as_ref()
            .is_some_and(|selected| selected != case)
        {
            continue;
        }
        let (bytes, expected) = fixture(size);
        std::fs::write(&path, &bytes).unwrap();
        let crc = crc32fast::hash(&bytes);
        let mut memory = Cursor::new(&bytes);
        let mut file = RetryReader::open(
            path.to_str().unwrap(),
            EioRetryPolicy {
                max_retries: 6,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        for index in 0..7 {
            for source in ["memory", "file"] {
                let (seconds, seek_read_seconds) = if source == "memory" {
                    sample(&mut memory, bytes.len(), &expected, crc, reads).await
                } else {
                    sample(&mut file, bytes.len(), &expected, crc, reads).await
                };
                println!(
                    "{case},{source},{},{index},{seconds:.9},{},{seek_read_seconds:.9}",
                    index == 0,
                    bytes.len() * reads
                );
            }
        }
    }
    std::fs::remove_file(path).unwrap();
    std::fs::remove_dir(dir).unwrap();
}
