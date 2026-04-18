use std::time::{Duration, Instant};

use eyre::Result;
use flume::Receiver;
use tokio::time::{sleep, timeout};
use tracing::{error, trace};

use crate::event::EventWrapper;

pub mod loki;
pub mod victorialogs;

pub(crate) const CAPACITY: usize = 1024;
const TIMEOUT: Duration = Duration::from_secs(5);

#[allow(async_fn_in_trait)]
pub trait Sender {
    type Encoded;

    fn encode(&self, events: Vec<EventWrapper>) -> Result<Self::Encoded>;

    async fn send(&mut self, data: &Self::Encoded) -> Result<()>;
}

pub(crate) async fn sender_task(receiver: Receiver<EventWrapper>, mut sender: impl Sender) {
    let mut buf = Vec::with_capacity(CAPACITY);
    let mut deadline = Instant::now() + TIMEOUT;
    loop {
        match timeout(Duration::from_secs(5), receiver.recv_async()).await {
            Ok(Ok(ev)) => buf.push(ev),
            Ok(Err(_)) => return, // no senders
            Err(_) => {}          // timeout
        }

        if buf.is_empty() {
            continue;
        }

        if deadline > Instant::now() && buf.len() < CAPACITY {
            continue;
        }

        trace!(len = buf.len(), "sending logs");
        match sender.encode(buf) {
            Ok(data) => {
                while let Err(err) = sender.send(&data).await {
                    error!(?err, "send logs");
                    sleep(Duration::from_secs(5)).await;
                }
            }
            Err(err) => {
                error!(?err, "failed to encode logs");
            }
        };

        buf = Vec::with_capacity(CAPACITY);
        deadline = Instant::now() + TIMEOUT;
    }
}
