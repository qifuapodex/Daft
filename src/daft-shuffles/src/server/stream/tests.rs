use std::{
    future::Future,
    io::{self, Cursor},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow_array::{RecordBatch, UInt8Array};
use arrow_flight::utils::batches_to_flight_data;
use tokio::io::ReadBuf;

use super::*;

pub(super) fn fixture(size: usize) -> (Vec<u8>, Vec<FlightData>) {
    let array = UInt8Array::from_iter_values((0..size).map(|i| (i % 251) as u8));
    let batch = RecordBatch::try_from_iter([("payload", Arc::new(array) as _)]).unwrap();
    let messages = batches_to_flight_data(&batch.schema(), [&batch]).unwrap();
    let mut bytes = Vec::new();
    for message in &messages {
        bytes.extend_from_slice(&CONTINUATION_MARKER.to_le_bytes());
        bytes.extend_from_slice(&(message.data_header.len() as i32).to_le_bytes());
        bytes.extend_from_slice(&message.data_header);
        bytes.extend_from_slice(&message.data_body);
    }
    (bytes, messages)
}

struct ShortReader {
    bytes: Cursor<Vec<u8>>,
    chunk: usize,
    pending: bool,
    fail_at: Option<usize>,
}

impl AsyncRead for ShortReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.pending {
            self.pending = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.pending = true;
        let position = self.bytes.position() as usize;
        if self.fail_at == Some(position) {
            // A failed reader may have modified its destination; the parser
            // must still return the error without yielding a partial message.
            buf.put_slice(&[0xee]);
            return Poll::Ready(Err(io::Error::from_raw_os_error(5)));
        }
        let end = (position + self.chunk.min(buf.remaining()))
            .min(self.bytes.get_ref().len())
            .min(self.fail_at.unwrap_or(usize::MAX));
        buf.put_slice(&self.bytes.get_ref()[position..end]);
        self.bytes.set_position(end as u64);
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn short_pending_reads_preserve_messages_and_owned_buffers() {
    for size in [
        0,
        1,
        4096,
        APPEND_READ_MIN_BYTES / 2,
        APPEND_READ_MIN_BYTES,
        4 * 1024 * 1024 + 17,
    ] {
        let (mut bytes, mut expected) = fixture(size);
        let (_, mut later) = fixture(129);
        let mut later = later.pop().unwrap();
        let mut body = later.data_body.to_vec();
        body.fill(0xa5);
        later.data_body = body.into();
        bytes.extend_from_slice(&CONTINUATION_MARKER.to_le_bytes());
        bytes.extend_from_slice(&(later.data_header.len() as i32).to_le_bytes());
        bytes.extend_from_slice(&later.data_header);
        bytes.extend_from_slice(&later.data_body);
        expected.push(later);
        let mut reader = ShortReader {
            bytes: Cursor::new(bytes),
            chunk: if size < 4096 { 3 } else { 8191 },
            pending: true,
            fail_at: None,
        };
        let mut retained = Vec::new();
        for _ in &expected {
            let FlightMessage::Data(message) = next_flight_data(&mut reader).await.unwrap() else {
                panic!("premature end of input");
            };
            retained.push(message);
        }
        assert!(matches!(
            next_flight_data(&mut reader).await.unwrap(),
            FlightMessage::EndOfInput
        ));
        // Holding earlier messages models downstream backpressure. Later reads
        // must not reuse or overwrite any of their allocations.
        assert_eq!(retained, expected);
    }
}

#[tokio::test]
async fn truncated_metadata_and_body_are_errors() {
    let (bytes, expected) = fixture(129);
    let schema_end = 8 + expected[0].data_header.len();
    for end in 4..bytes.len() {
        if end == schema_end || (schema_end + 1..schema_end + 4).contains(&end) {
            // The existing parser treats EOF in the first length word as
            // EndOfInput; CheckedRange separately rejects an incomplete range.
            continue;
        }
        let mut reader = &bytes[..end];
        if end > schema_end {
            assert!(matches!(
                next_flight_data(&mut reader).await.unwrap(),
                FlightMessage::Data(_)
            ));
        }
        assert!(next_flight_data(&mut reader).await.is_err(), "cut at {end}");
    }
}

#[tokio::test]
async fn eio_during_metadata_or_body_never_yields_partial_data() {
    let (bytes, expected) = fixture(APPEND_READ_MIN_BYTES * 2);
    let schema_end = 8 + expected[0].data_header.len();
    let body_start = schema_end + 8 + expected[1].data_header.len();
    for fail_at in [schema_end + 9, body_start + 13, bytes.len() - 1] {
        let mut reader = ShortReader {
            bytes: Cursor::new(bytes.clone()),
            chunk: 17,
            pending: true,
            fail_at: Some(fail_at),
        };
        assert!(matches!(
            next_flight_data(&mut reader).await.unwrap(),
            FlightMessage::Data(_)
        ));
        let Err(DaftError::IoError(error)) = next_flight_data(&mut reader).await else {
            panic!("expected EIO at {fail_at}");
        };
        assert_eq!(error.raw_os_error(), Some(5));
    }
}

#[tokio::test]
async fn cancellation_during_body_does_not_publish_a_message() {
    let (bytes, expected) = fixture(APPEND_READ_MIN_BYTES * 2);
    let body_start = 16 + expected[0].data_header.len() + expected[1].data_header.len();
    let (mut sender, mut reader) = tokio::io::duplex(bytes.len());
    use tokio::io::AsyncWriteExt;
    sender.write_all(&bytes[..body_start + 13]).await.unwrap();
    let FlightMessage::Data(schema) = next_flight_data(&mut reader).await.unwrap() else {
        panic!("missing schema");
    };
    let mut future = Box::pin(next_flight_data(&mut reader));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(future.as_mut().poll(&mut cx).is_pending());
    drop(future);
    // Cancellation discards this stream, as CheckedRange callers do. It is not
    // safe to resume parsing in the middle of an IPC body.
    drop(reader);
    assert_eq!(schema, expected[0]);
    assert!(sender.write_all(b"discarded").await.is_err());
}

#[tokio::test]
async fn exact_length_reads_do_not_probe_or_consume_the_next_message() {
    let mut reader = &b"firstnext"[..];
    assert_eq!(read_message_bytes(&mut reader, 5).await.unwrap(), b"first");
    assert_eq!(reader, b"next");
    assert!(read_message_bytes(&mut reader, 0).await.unwrap().is_empty());
    assert_eq!(reader, b"next");
}

#[tokio::test]
async fn message_allocation_does_not_initialize_unread_capacity() {
    struct InitializationProbe;
    impl AsyncRead for InitializationProbe {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            assert!(buf.initialized().is_empty());
            buf.put_slice(&[42]);
            Poll::Ready(Ok(()))
        }
    }
    let result = read_message_bytes(&mut InitializationProbe, 4096)
        .await
        .unwrap();
    assert_eq!(result, vec![42; 4096]);
    assert_eq!(result.capacity(), 4096);
}

#[tokio::test]
async fn explicit_eos_and_negative_length_keep_their_meaning() {
    let mut eos = Vec::from(CONTINUATION_MARKER.to_le_bytes());
    eos.extend_from_slice(&0i32.to_le_bytes());
    eos.extend_from_slice(b"next");
    let mut reader = eos.as_slice();
    assert!(matches!(
        next_flight_data(&mut reader).await.unwrap(),
        FlightMessage::EndOfStream
    ));
    assert_eq!(reader, b"next");
    assert!(
        next_flight_data(&mut (-2i32).to_le_bytes().as_slice())
            .await
            .is_err()
    );
}
