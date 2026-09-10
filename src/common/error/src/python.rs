use pyo3::{exceptions::PyFileNotFoundError, import_exception};

use crate::{DaftError, format::format_error_for_user};

import_exception!(daft.exceptions, DaftCoreException);
import_exception!(daft.exceptions, DaftShuffleFetchError);
import_exception!(daft.exceptions, DaftTypeError);
import_exception!(daft.exceptions, DaftTransientError);
import_exception!(daft.exceptions, ConnectTimeoutError);
import_exception!(daft.exceptions, ReadTimeoutError);
import_exception!(daft.exceptions, ByteStreamError);
import_exception!(daft.exceptions, SocketError);
import_exception!(daft.exceptions, ThrottleError);
import_exception!(daft.exceptions, MiscTransientError);

impl std::convert::From<DaftError> for pyo3::PyErr {
    fn from(err: DaftError) -> Self {
        to_pyerr(&err)
    }
}

fn to_pyerr(err: &DaftError) -> pyo3::PyErr {
    match err {
        DaftError::Shared(error) => to_pyerr(error),
        DaftError::ShuffleFetchFailure(failure) => DaftShuffleFetchError::new_err((
            failure.shuffle_id,
            failure.input_id,
            failure.attempt,
            failure.partition_idx,
            failure.path.clone(),
            failure.message.clone(),
        )),
        DaftError::PyO3Error(pyerr) => pyo3::Python::attach(|py| pyerr.clone_ref(py)),
        DaftError::TypeError(msg) => DaftTypeError::new_err(msg.clone()),
        other => {
            let formatted = format_error_for_user(other);
            match other {
                DaftError::FileNotFound { .. } => PyFileNotFoundError::new_err(formatted),
                DaftError::ConnectTimeout(_) => ConnectTimeoutError::new_err(formatted),
                DaftError::ReadTimeout(_) => ReadTimeoutError::new_err(formatted),
                DaftError::ByteStreamError(_) => ByteStreamError::new_err(formatted),
                DaftError::SocketError(_) => SocketError::new_err(formatted),
                DaftError::ThrottledIo(_) => ThrottleError::new_err(formatted),
                DaftError::MiscTransient(_) => MiscTransientError::new_err(formatted),
                _ => DaftCoreException::new_err(formatted),
            }
        }
    }
}
