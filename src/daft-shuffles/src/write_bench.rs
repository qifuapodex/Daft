//! Manual, real-writer regression benchmark; all validation is outside timing.
use std::{fs::File, time::Instant};

use crate::{
    oneshot_writer::{OneShotTarget, write_partitions_one_shot},
    shuffle_cache::{InProgressShuffleCache, PartitionCache},
};

fn configure(id: u64, retries: u32) {
    // PRE_EIO_BASELINE: this function is a no-op in the historical test copy.
    crate::local_io::configure(
        id,
        daft_io::shuffle_file::EioRetryPolicy {
            max_retries: retries,
            ..Default::default()
        },
    );
}

fn validate(caches: &[PartitionCache], expected_rows: usize) {
    let mut paths: Vec<_> = caches.iter().flat_map(|c| c.file_paths.iter()).collect();
    paths.sort();
    paths.dedup();
    let mut rows = 0;
    for path in paths {
        let reader =
            arrow_ipc::reader::StreamReader::try_new(File::open(path).unwrap(), None).unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::UInt8Array>()
                .unwrap();
            for (i, value) in values.values().iter().enumerate() {
                assert_eq!(*value, i as u8);
            }
            rows += batch.num_rows();
        }
    }
    assert_eq!(rows, expected_rows);
    assert_eq!(
        caches.iter().map(|c| c.num_rows).sum::<usize>(),
        expected_rows
    );
}

#[tokio::test]
#[ignore = "manual release writer benchmark; retain all samples"]
async fn bench_shuffle_writes() {
    let case = std::env::var("DAFT_WRITE_CASE").unwrap_or("stream-tiny".into());
    let samples: usize = std::env::var("DAFT_WRITE_SAMPLES")
        .unwrap_or("3".into())
        .parse()
        .unwrap();
    let concurrency: usize = std::env::var("DAFT_WRITE_CONCURRENCY")
        .unwrap_or("1".into())
        .parse()
        .unwrap();
    let retries: u32 = std::env::var("DAFT_WRITE_RETRIES")
        .unwrap_or("6".into())
        .parse()
        .unwrap();
    let compression = std::env::var("DAFT_WRITE_COMPRESSION").unwrap_or("none".into());
    let ipc_compression = match compression.as_str() {
        "none" => None,
        "lz4" => Some(arrow_ipc::CompressionType::LZ4_FRAME),
        _ => panic!("unsupported compression"),
    };
    let (oneshot, size, batches, groups) = match case.as_str() {
        "oneshot-small" => (true, 4096, 128, 16),
        "oneshot-wide" => (true, 4 * 1024 * 1024, 32, 2),
        "stream-single" => (false, 4096, 1, 128),
        "stream-tiny" => (false, 4096, 256, 8),
        "stream-wide" => (false, 4 * 1024 * 1024, 16, 2),
        _ => panic!("unknown case"),
    };
    assert!(concurrency > 0 && samples > 0);
    let data = daft_writers::test::make_dummy_mp(size);
    let root = std::env::temp_dir().join(format!("daft-write-bench-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    println!(
        "WRITE_ENV case={case} concurrency={concurrency} retries={retries} compression={compression}"
    );
    for sample in 0..=samples {
        let dir = root.join(sample.to_string());
        std::fs::create_dir(&dir).unwrap();
        // Configuration and data creation are outside timing. File creation,
        // encoding, all writes, close/drain, and task scheduling are included.
        let id = 0xbe00 + sample as u64;
        configure(id, retries);
        let start = Instant::now();
        let mut all_caches = Vec::new();
        for group in (0..groups).step_by(concurrency) {
            let futures = (group..(group + concurrency).min(groups)).map(|group| {
                let data = data.clone();
                let dir = dir.to_string_lossy().into_owned();
                let compression = compression.clone();
                async move {
                    if oneshot {
                        write_partitions_one_shot(
                            group as u32,
                            id,
                            1,
                            OneShotTarget::Local {
                                shuffle_dirs: vec![dir],
                            },
                            data.schema(),
                            ipc_compression,
                            vec![data; batches],
                        )
                        .await
                        .unwrap()
                    } else {
                        let cache = InProgressShuffleCache::try_new(
                            group as u64,
                            1,
                            data.schema(),
                            &[dir],
                            id,
                            512 * 1024 * 1024,
                            if compression == "none" {
                                None
                            } else {
                                Some(compression.as_str())
                            },
                        )
                        .unwrap();
                        for _ in 0..batches {
                            cache.push_partition_data(data.clone()).await.unwrap();
                        }
                        vec![cache.close().await.unwrap()]
                    }
                }
            });
            for caches in futures::future::join_all(futures).await {
                all_caches.extend(caches);
            }
        }
        let seconds = start.elapsed().as_secs_f64();
        validate(&all_caches, size * batches * groups);
        println!("WRITE_SAMPLE {case} {} {sample} {seconds:.9}", sample == 0);
        std::fs::remove_dir_all(dir).unwrap();
    }
    std::fs::remove_dir(root).unwrap();
}
