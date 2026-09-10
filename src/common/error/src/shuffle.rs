use serde::{Deserialize, Serialize};

use crate::DaftError;

/// Identity of an unavailable physical map output. This is a recovery request,
/// not a transient error: retrying the consumer with the same inputs cannot fix it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error(
    "Shuffle {shuffle_id} map {input_id} attempt {attempt:#x} partition {partition_idx}: {message} ({path})"
)]
pub struct ShuffleFetchFailure {
    pub shuffle_id: u64,
    pub input_id: u32,
    pub attempt: u64,
    pub partition_idx: u32,
    pub path: String,
    pub message: String,
}

impl ShuffleFetchFailure {
    /// Only ENOENT identifies missing output. Permission and mount errors must
    /// retain their I/O classification instead of triggering reconstruction.
    pub fn open_error(mut self, error: std::io::Error) -> DaftError {
        if error.kind() == std::io::ErrorKind::NotFound {
            self.message = error.to_string();
            DaftError::ShuffleFetchFailure(Box::new(self))
        } else {
            DaftError::IoError(error)
        }
    }
}

impl DaftError {
    pub fn shuffle_fetch_failure(&self) -> Option<ShuffleFetchFailure> {
        match self {
            Self::Shared(error) => error.shuffle_fetch_failure(),
            Self::ShuffleFetchFailure(failure) => Some((**failure).clone()),
            #[cfg(feature = "python")]
            Self::PyO3Error(error) => pyo3::Python::attach(|py| {
                use pyo3::types::PyAnyMethods;
                let mut value = error.value(py).as_any().clone();
                for _ in 0..=4 {
                    if value.is_instance_of::<crate::python::DaftShuffleFetchError>() {
                        let identity = (|| {
                            Some(ShuffleFetchFailure {
                                shuffle_id: value.getattr("shuffle_id").ok()?.extract().ok()?,
                                input_id: value.getattr("input_id").ok()?.extract().ok()?,
                                attempt: value.getattr("attempt").ok()?.extract().ok()?,
                                partition_idx: value
                                    .getattr("partition_idx")
                                    .ok()?
                                    .extract()
                                    .ok()?,
                                path: value.getattr("path").ok()?.extract().ok()?,
                                message: value.getattr("message").ok()?.extract().ok()?,
                            })
                        })();
                        if identity.is_some() {
                            return identity;
                        }
                    }
                    value = value
                        .getattr("cause")
                        .ok()
                        .filter(|v| !v.is_none())
                        .or_else(|| value.getattr("__cause__").ok().filter(|v| !v.is_none()))?;
                }
                None
            }),
            _ => None,
        }
    }
}
