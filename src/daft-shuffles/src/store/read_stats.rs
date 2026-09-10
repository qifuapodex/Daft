//! One summary per shared read stream, never one log record per map range.
//! Durations are summed operation waits, not stage wall time or storage-only time.
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

#[derive(Default)]
pub(super) struct ReadStats {
    pub enabled: bool,
    pub shuffle_id: u64,
    pub map_files: usize,
    pub coalesced: bool,
    pub started: Option<Instant>,
    pub opens: AtomicU64,
    pub completed_files: AtomicU64,
    pub index_hits: AtomicU64,
    pub index_misses: AtomicU64,
    pub ranges: AtomicU64,
    pub empty_ranges: AtomicU64,
    pub ranges_under_64k: AtomicU64,
    pub ranges_under_1m: AtomicU64,
    pub indexed_bytes: AtomicU64,
    pub verified_bytes: AtomicU64,
    pub messages: AtomicU64,
    pub slot_wait_us: AtomicU64,
    pub open_us: AtomicU64,
    pub index_us: AtomicU64,
    pub read_poll_us: AtomicU64,
}

impl ReadStats {
    pub fn add(&self, counter: &AtomicU64, value: u64) {
        if self.enabled {
            counter.fetch_add(value, Ordering::Relaxed);
        }
    }
}

pub(super) fn elapsed_us(started: Option<Instant>) -> u64 {
    started
        .map(|t| t.elapsed().as_micros().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

impl Drop for ReadStats {
    fn drop(&mut self) {
        if !self.enabled {
            return;
        }
        let get = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        tracing::info!(
            shuffle_id = self.shuffle_id,
            route = "shared",
            coalesced = self.coalesced,
            map_files = self.map_files,
            file_opens = get(&self.opens),
            completed_files = get(&self.completed_files),
            index_cache_hits = get(&self.index_hits),
            index_cache_misses = get(&self.index_misses),
            nonempty_ranges = get(&self.ranges),
            empty_ranges = get(&self.empty_ranges),
            ranges_under_64k = get(&self.ranges_under_64k),
            ranges_under_1m = get(&self.ranges_under_1m),
            indexed_bytes = get(&self.indexed_bytes),
            verified_bytes = get(&self.verified_bytes),
            ipc_messages = get(&self.messages),
            slot_wait_us = get(&self.slot_wait_us),
            open_us = get(&self.open_us),
            index_us = get(&self.index_us),
            read_poll_us = get(&self.read_poll_us),
            stream_wall_us = elapsed_us(self.started),
            complete = get(&self.completed_files) == self.map_files as u64,
            "Shuffle shared read statistics"
        );
    }
}
