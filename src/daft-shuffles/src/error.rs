//! Preserve shuffle recovery identity through both local and remote Flight streams.
use arrow_flight::error::FlightError;
use common_error::{DaftError, ShuffleFetchFailure};
use tonic::{Code, Status};

const FAILURE_METADATA: &str = "daft-shuffle-failure";

pub(crate) fn to_status(error: DaftError) -> Status {
    if let Some(failure) = error.shuffle_fetch_failure()
        && let Ok(details) = serde_json::to_vec(&failure)
    {
        let mut status = Status::with_details(Code::NotFound, error.to_string(), details.into());
        status
            .metadata_mut()
            .insert(FAILURE_METADATA, "1".parse().unwrap());
        status
    } else {
        Status::internal(error.to_string())
    }
}

pub(crate) fn from_flight(error: FlightError) -> DaftError {
    match error {
        FlightError::ExternalError(error) => match error.downcast::<DaftError>() {
            Ok(error) => *error,
            Err(error) => DaftError::External(error),
        },
        FlightError::Tonic(status) => {
            if status.code() == Code::NotFound
                && status
                    .metadata()
                    .get(FAILURE_METADATA)
                    .is_some_and(|v| v == "1")
                && let Ok(failure) = serde_json::from_slice::<ShuffleFetchFailure>(status.details())
            {
                DaftError::ShuffleFetchFailure(Box::new(failure))
            } else {
                DaftError::External(Box::new(status))
            }
        }
        error => DaftError::External(Box::new(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure() -> ShuffleFetchFailure {
        ShuffleFetchFailure {
            shuffle_id: 17,
            input_id: 9,
            attempt: u64::MAX,
            partition_idx: 3,
            path: "/test/map.arrow".into(),
            message: "missing".into(),
        }
    }

    #[test]
    fn local_and_rpc_errors_preserve_exact_output_identity() {
        let expected = failure();
        let local = from_flight(FlightError::ExternalError(Box::new(
            DaftError::ShuffleFetchFailure(Box::new(expected.clone())),
        )));
        assert_eq!(local.shuffle_fetch_failure(), Some(expected.clone()));
        let shared = DaftError::Shared(std::sync::Arc::new(local));
        assert_eq!(shared.shuffle_fetch_failure(), Some(expected.clone()));
        let status = to_status(shared);
        // Exercise tonic's header encoding/decoding, including binary status details.
        let received = Status::from_header_map(status.into_http::<()>().headers()).unwrap();
        let remote = from_flight(FlightError::Tonic(Box::new(received)));
        assert_eq!(remote.shuffle_fetch_failure(), Some(expected));
        assert!(!remote.is_transient());
    }

    #[test]
    fn ordinary_not_found_and_permissions_do_not_request_reconstruction() {
        let ordinary = from_flight(FlightError::Tonic(Box::new(Status::not_found(
            "not a shuffle output",
        ))));
        assert!(ordinary.shuffle_fetch_failure().is_none());
        let denied =
            failure().open_error(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
        assert!(denied.shuffle_fetch_failure().is_none());
    }
}
