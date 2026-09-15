use serde::{Deserialize, Serialize};

use crate::DaftError;

/// A POSIX shuffle file failure. Kept separate from generic transient errors:
/// retrying it needs the task's EIO budget and restart-safety policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("Shuffle {operation} failed at {path} (errno {errno}): {message}")]
pub struct ShuffleIoError {
    pub operation: String,
    pub path: String,
    pub errno: i32,
    pub message: String,
}

impl ShuffleIoError {
    pub fn is_eio(&self) -> bool {
        // EIO is 5 on the POSIX platforms supported by Flight's file backend.
        cfg!(unix) && self.errno == 5
    }
}

#[cfg(test)]
mod io_tests {
    use super::*;

    #[test]
    fn shuffle_io_preserves_typed_errno_through_arrow_and_shared_errors() {
        for errno in [5, 13, 28, 30] {
            let error = DaftError::ArrowRsError(arrow_schema::ArrowError::from(
                std::io::Error::from_raw_os_error(errno),
            ))
            .with_shuffle_io_context("write", "/shuffle/map.arrow");
            let failure = error.shuffle_io_error().unwrap();
            assert_eq!(failure.errno, errno);
            assert_eq!(failure.is_eio(), cfg!(unix) && errno == 5);
            let shared = DaftError::Shared(std::sync::Arc::new(error));
            assert_eq!(shared.shuffle_io_error(), Some(failure));
            assert!(!shared.is_transient(), "EIO uses its own task policy");
        }
    }

    #[test]
    fn generic_io_and_error_text_do_not_enable_shuffle_retries() {
        let generic = DaftError::IoError(std::io::Error::from_raw_os_error(5));
        assert!(generic.shuffle_io_error().is_none());
        let text = DaftError::InternalError("Input/output error (os error 5)".into())
            .with_shuffle_io_context("read", "/shuffle/map.arrow");
        assert!(text.shuffle_io_error().is_none());
    }

    #[test]
    fn explicit_transient_causes_keep_their_classification_except_for_eio() {
        for errno in [13, 28, 110] {
            let error =
                DaftError::MiscTransient(Box::new(std::io::Error::from_raw_os_error(errno)))
                    .with_shuffle_io_context("read", "/shuffle/map.arrow");
            assert!(error.is_transient());
            assert!(error.shuffle_io_error().is_none());
        }
        let error = DaftError::MiscTransient(Box::new(std::io::Error::from_raw_os_error(5)))
            .with_shuffle_io_context("read", "/shuffle/map.arrow");
        assert!(error.shuffle_io_error().unwrap().is_eio());
        assert!(!error.is_transient());
    }
}

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
    /// Call only at shuffle file boundaries. Inspect typed causes, including
    /// Arrow's I/O wrapper, before any conversion to strings or RPC statuses.
    pub fn with_shuffle_io_context(self, operation: &str, path: &str) -> Self {
        if self.shuffle_io_error().is_some() {
            return self;
        }
        let mut source: &(dyn std::error::Error + 'static) = &self;
        for _ in 0..32 {
            if let Some(errno) = source
                .downcast_ref::<std::io::Error>()
                .and_then(std::io::Error::raw_os_error)
            {
                // Preserve an explicit transport/transient classification for
                // non-EIO causes instead of replacing it with an errno-only type.
                if errno != 5 && self.is_transient() {
                    return self;
                }
                return Self::ShuffleIo(Box::new(ShuffleIoError {
                    operation: operation.into(),
                    path: path.into(),
                    errno,
                    message: self.to_string(),
                }));
            }
            match source.source() {
                Some(next) => source = next,
                None => break,
            }
        }
        self
    }

    pub fn shuffle_io_error(&self) -> Option<ShuffleIoError> {
        match self {
            Self::Shared(error) => error.shuffle_io_error(),
            Self::ShuffleIo(error) => Some((**error).clone()),
            #[cfg(feature = "python")]
            Self::PyO3Error(error) => pyo3::Python::attach(|py| {
                use pyo3::types::PyAnyMethods;
                let mut value = error.value(py).as_any().clone();
                for _ in 0..=4 {
                    if value.is_instance_of::<crate::python::DaftShuffleIoError>() {
                        return Some(ShuffleIoError {
                            operation: value.getattr("operation").ok()?.extract().ok()?,
                            path: value.getattr("path").ok()?.extract().ok()?,
                            errno: value.getattr("errno").ok()?.extract().ok()?,
                            message: value.getattr("message").ok()?.extract().ok()?,
                        });
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
