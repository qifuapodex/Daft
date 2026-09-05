use thiserror::Error;

pub type DaftResult<T> = std::result::Result<T, DaftError>;
pub type GenericError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Error)]
pub enum DaftError {
    #[error("DaftError::AmbiguousReference {0}")]
    AmbiguousReference(String),
    #[error("DaftError::FieldNotFound {0}")]
    FieldNotFound(String),
    #[error("DaftError::SchemaMismatch {0}")]
    SchemaMismatch(String),
    #[error("DaftError::TypeError {0}")]
    TypeError(String),
    #[error("DaftError::ComputeError {0}")]
    ComputeError(String),
    #[error("DaftError::ArrowRsError {0}")]
    ArrowRsError(#[from] arrow_schema::ArrowError),
    // TODO(desmond): We can't currently implement this as a From<parquet::errors::ParquetError>
    // because this results in infinite nesting of types in `fixed_size_binary_op` in arithmetic.rs.
    #[error("DaftError::ParquetError {0}")]
    ParquetError(String),
    /// Raised when a file is identified as corrupt or unreadable due to format/integrity
    /// failures (e.g. bad magic bytes, truncated footer, bad encoding, wrong field counts).
    /// Used by `is_parquet_corrupt` and `is_csv_corrupt` to identify files that should be
    /// skipped when `ignore_corrupt_files` is enabled.
    /// General operation errors (write failures, schema mismatches, etc.) are NOT routed
    /// here — they use format-specific variants or `External`.
    #[error("DaftError::CorruptFile {0}")]
    CorruptFile(String),
    #[error("DaftError::ValueError {0}")]
    ValueError(String),
    #[cfg(feature = "python")]
    #[error("DaftError::PyO3Error {0}")]
    PyO3Error(#[from] pyo3::PyErr),
    #[error("DaftError::IoError {0}")]
    IoError(#[from] std::io::Error),
    #[error("DaftError::FileNotFound {path} not found: {source}")]
    FileNotFound { path: String, source: GenericError },
    #[error("DaftError::InternalError {0}")]
    InternalError(String),
    #[error("ConnectTimeout {0}")]
    ConnectTimeout(#[source] GenericError),
    #[error("ReadTimeout {0}")]
    ReadTimeout(#[source] GenericError),
    #[error("ByteStreamError {0}")]
    ByteStreamError(#[source] GenericError),
    #[error("SocketError {0}")]
    SocketError(#[source] GenericError),
    #[error("ThrottledIo {0}")]
    ThrottledIo(#[source] GenericError),
    #[error("MiscTransient {0}")]
    MiscTransient(#[source] GenericError),
    #[error("DaftError::External {0}")]
    External(#[source] GenericError),
    #[error("DaftError::SerdeJsonError {0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("DaftError::FmtError {0}")]
    FmtError(#[from] std::fmt::Error),
    #[error("DaftError::RegexError {0}")]
    RegexError(#[from] regex::Error),
    #[error("DaftError::FromUtf8Error {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error("Not Yet Implemented: {0}")]
    NotImplemented(String),
    #[error("DaftError::CatalogError {0}")]
    CatalogError(String),
    #[error("DaftError::JoinError {0}")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("DaftError::InvalidArgumentError {0}")]
    InvalidArgumentError(String),
}

impl DaftError {
    pub fn not_implemented<T: std::fmt::Display>(msg: T) -> Self {
        Self::NotImplemented(msg.to_string())
    }
    pub fn type_error<T: std::fmt::Display>(msg: T) -> Self {
        Self::TypeError(msg.to_string())
    }

    /// Returns true if this error is transient and the operation should be retried.
    /// Uses a whitelist approach: only network/timeout errors are considered transient.
    /// Data errors (OOM, schema mismatches, casts, corrupt files) are never transient.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::ConnectTimeout(_)
            | Self::ReadTimeout(_)
            | Self::ByteStreamError(_)
            | Self::SocketError(_)
            | Self::ThrottledIo(_)
            | Self::MiscTransient(_) => true,
            // A worker-side error that crosses a Python boundary (e.g. a Ray task result
            // travelling back to the driver) arrives here as a bare `PyErr`, so the variant
            // above is gone and the Python exception class is the only type information left.
            // `daft.exceptions.DaftTransientError` is the base class of exactly the six
            // variants above -- see the `From<DaftError> for PyErr` mapping in `python.rs` --
            // so the two whitelists stay in sync by construction. Ray re-raises the original
            // exception via `as_instanceof_cause()`, which both makes the result an instance
            // of the original class and keeps the original on `.cause`; check both.
            #[cfg(feature = "python")]
            Self::PyO3Error(pyerr) => pyo3::Python::attach(|py| {
                use pyo3::types::PyAnyMethods;

                let value = pyerr.value(py);
                value.is_instance_of::<crate::python::DaftTransientError>()
                    || value
                        .getattr(pyo3::intern!(py, "cause"))
                        .is_ok_and(|cause| {
                            cause.is_instance_of::<crate::python::DaftTransientError>()
                        })
            }),
            _ => false,
        }
    }
}

#[macro_export]
macro_rules! ensure {
    ($cond:expr, $msg:expr) => {
        if !$cond {
            return Err($crate::DaftError::ComputeError($msg.to_string()));
        }
    };
    ($cond:expr, $variant:ident: $($msg:tt)*) => {
        if !$cond {
            return Err($crate::DaftError::$variant(format!($($msg)*)));
        }
    };
}

#[macro_export]
macro_rules! value_err {
    ($($arg:tt)*) => {
        return Err(common_error::DaftError::ValueError(format!($($arg)*)))
    };
}

#[cfg(feature = "python")]
impl<'py> From<pyo3::pyclass::PyClassGuardError<'_, 'py>> for DaftError {
    fn from(error: pyo3::pyclass::PyClassGuardError<'_, 'py>) -> Self {
        Self::PyO3Error(error.into())
    }
}

#[cfg(test)]
mod tests {
    use super::DaftError;

    /// The six variants `python.rs` maps onto `DaftTransientError` subclasses must all
    /// classify as transient. If a variant is added to that mapping, add it here too --
    /// the two lists are the same whitelist expressed on either side of the boundary.
    #[test]
    fn native_transient_variants_are_retryable() {
        let transient: Vec<DaftError> = vec![
            DaftError::ConnectTimeout("connect".into()),
            DaftError::ReadTimeout("read".into()),
            DaftError::ByteStreamError("stream".into()),
            DaftError::SocketError("socket".into()),
            DaftError::ThrottledIo("throttled".into()),
            DaftError::MiscTransient("misc".into()),
        ];
        for error in transient {
            assert!(error.is_transient(), "{error:?} should be transient");
        }
    }

    /// Data and logic errors must never be retried: retrying them burns the whole retry
    /// budget on an outcome that cannot change, and an OOM that keeps killing workers
    /// would take a node out of the cluster on every attempt.
    #[test]
    fn data_and_logic_errors_are_not_retryable() {
        let permanent: Vec<DaftError> = vec![
            DaftError::ComputeError("compute".to_string()),
            DaftError::ValueError("value".to_string()),
            DaftError::InternalError("internal".to_string()),
            DaftError::TypeError("type".to_string()),
            DaftError::SchemaMismatch("schema".to_string()),
            DaftError::CorruptFile("corrupt".to_string()),
            DaftError::External("external".into()),
        ];
        for error in permanent {
            assert!(!error.is_transient(), "{error:?} should not be transient");
        }
    }
}
