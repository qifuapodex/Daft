//! Per-shuffle runtime policy, shared with the serving worker without changing
//! Flight tickets or the file index. Its lifetime matches other shuffle caches.
use std::{
    collections::HashMap,
    sync::{LazyLock, Mutex},
};

use common_daft_config::DaftExecutionConfig;
use daft_io::shuffle_file::EioRetryPolicy;

static POLICIES: LazyLock<Mutex<HashMap<u64, EioRetryPolicy>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub fn from_config(config: &DaftExecutionConfig) -> EioRetryPolicy {
    EioRetryPolicy {
        max_retries: config.flight_shuffle_eio_local_max_retries,
        initial_backoff_ms: config.flight_shuffle_eio_local_initial_backoff_ms,
        max_backoff_ms: config.flight_shuffle_eio_local_max_backoff_ms,
    }
}

pub fn configure(shuffle_id: u64, policy: EioRetryPolicy) {
    POLICIES.lock().unwrap().insert(shuffle_id, policy);
}

pub(crate) fn policy(shuffle_id: u64) -> EioRetryPolicy {
    POLICIES
        .lock()
        .unwrap()
        .get(&shuffle_id)
        .copied()
        .unwrap_or_else(|| {
            tracing::debug!(target: "daft_shuffle_io_retry", shuffle_id,
                "No shuffle I/O policy registered; local retries disabled");
            EioRetryPolicy::default()
        })
}

pub(crate) fn forget(shuffle_id: u64) {
    POLICIES.lock().unwrap().remove(&shuffle_id);
}
