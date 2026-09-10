pub mod flight_client;

use std::{collections::HashMap, sync::Arc};

use common_error::DaftResult;
use daft_core::prelude::SchemaRef;
use daft_recordbatch::RecordBatch;
use futures::{StreamExt, stream::BoxStream};
use tokio::sync::Mutex;

use crate::client::flight_client::{MAX_TICKET_REFS, ShuffleFlightClient};

#[derive(Clone)]
pub struct FlightClientManager {
    clients: Arc<Mutex<HashMap<String, Arc<Mutex<ShuffleFlightClient>>>>>,
}

impl FlightClientManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fetch `(attempt, partition_ref_id)` pairs from one server as a single stream.
    pub async fn fetch_partition(
        &self,
        shuffle_id: u64,
        server_address: &str,
        refs: &[(u64, u64)],
        schema: SchemaRef,
    ) -> DaftResult<BoxStream<'static, DaftResult<RecordBatch>>> {
        let client = {
            let mut clients = self.clients.lock().await;
            clients
                .entry(server_address.to_string())
                .or_insert_with(|| {
                    Arc::new(Mutex::new(ShuffleFlightClient::new(
                        server_address.to_string(),
                    )))
                })
                .clone()
        };

        if refs.len() > MAX_TICKET_REFS {
            // Keep one RPC active at a time. A later chunk's error is delivered
            // through the same stream, so callers cannot replay already-yielded rows.
            let refs = refs.to_vec();
            return Ok(Box::pin(async_stream::try_stream! {
                for chunk in refs.chunks(MAX_TICKET_REFS) {
                    let mut stream = client.lock().await
                        .get_partition(shuffle_id, chunk, schema.clone()).await?;
                    while let Some(batch) = stream.next().await {
                        yield batch?;
                    }
                }
            }));
        }

        let stream = client
            .lock()
            .await
            .get_partition(shuffle_id, refs, schema)
            .await?
            .boxed();
        Ok(stream)
    }
}

impl Default for FlightClientManager {
    fn default() -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}
