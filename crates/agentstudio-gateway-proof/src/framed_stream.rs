use std::{
    future::{Future, poll_fn},
    pin::pin,
    time::Duration,
};

use quinn::{ReadError, ReadExactError, RecvStream, SendStream, StreamId, VarInt, WriteError};
use serde::Serialize;
use tokio::{sync::broadcast, time};

pub const STOPPED_CODE: VarInt = VarInt::from_u32(41);
pub const ABORTED_CODE: VarInt = VarInt::from_u32(42);

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("invalid zero frame length")]
    Empty,
    #[error("frame too large ({0} bytes)")]
    TooLarge(usize),
    #[error("frame prefix: {0}")]
    Prefix(ReadExactError),
    #[error("frame body: {0}")]
    Body(ReadExactError),
    #[error("request/reply has trailing bytes")]
    Trailing,
    #[error("FIN read failed: {0}")]
    FinRead(ReadError),
    #[error("frame deadline elapsed")]
    Deadline,
    #[error("frame JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("frame write: {0}")]
    Write(#[from] WriteError),
    #[error("cannot finish the stream: {0}")]
    Finish(#[from] quinn::ClosedStream),
}

pub struct Receiving {
    stream: RecvStream,
    finished: bool,
}

impl Receiving {
    pub fn new(stream: RecvStream) -> Self {
        Self {
            stream,
            finished: false,
        }
    }

    pub fn stream_id(&self) -> StreamId {
        self.stream.id()
    }

    pub async fn read_frame(&mut self, limit: usize) -> Result<Vec<u8>, FrameError> {
        let mut prefix = [0_u8; 4];
        self.stream
            .read_exact(&mut prefix)
            .await
            .map_err(FrameError::Prefix)?;
        let size = u32::from_be_bytes(prefix) as usize;
        if size == 0 {
            return Err(FrameError::Empty);
        }
        if size > limit {
            return Err(FrameError::TooLarge(size));
        }
        let mut body = vec![0_u8; size];
        self.stream
            .read_exact(&mut body)
            .await
            .map_err(FrameError::Body)?;
        Ok(body)
    }

    pub async fn confirm_fin(&mut self) -> Result<(), FrameError> {
        let mut extra = [0_u8; 1];
        match self
            .stream
            .read(&mut extra)
            .await
            .map_err(FrameError::FinRead)?
        {
            None => {
                self.finished = true;
                Ok(())
            }
            Some(_) => Err(FrameError::Trailing),
        }
    }
}

impl Drop for Receiving {
    fn drop(&mut self) {
        if !self.finished {
            let id = self.stream.id();
            let outcome = self.stream.stop(STOPPED_CODE);
            eprintln!(
                "quinn-receive-stop stream={} outcome={outcome:?}",
                id.index()
            );
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct CreditPending {
    pub stream: StreamId,
    pub frame_bytes: usize,
}

pub struct Sending {
    stream: SendStream,
    finished: bool,
    credit_receipts: Option<broadcast::Sender<CreditPending>>,
}

impl Sending {
    pub fn new(stream: SendStream) -> Self {
        Self {
            stream,
            finished: false,
            credit_receipts: None,
        }
    }

    pub fn with_credit_receipts(mut self, receipts: broadcast::Sender<CreditPending>) -> Self {
        self.credit_receipts = Some(receipts);
        self
    }

    pub fn stopped(
        &self,
    ) -> impl Future<Output = Result<Option<VarInt>, quinn::StoppedError>> + Send + 'static {
        self.stream.stopped()
    }

    pub async fn send_json<Payload: Serialize>(
        &mut self,
        payload: &Payload,
        limit: usize,
        deadline: Duration,
    ) -> Result<(), FrameError> {
        let body = serde_json::to_vec(payload)?;
        self.send_bytes(&body, limit, deadline).await
    }

    pub async fn send_bytes(
        &mut self,
        body: &[u8],
        limit: usize,
        deadline: Duration,
    ) -> Result<(), FrameError> {
        if body.is_empty() {
            return Err(FrameError::Empty);
        }
        if body.len() > limit {
            return Err(FrameError::TooLarge(body.len()));
        }
        let size = u32::try_from(body.len()).map_err(|_| FrameError::TooLarge(body.len()))?;
        let stream = self.stream.id();
        let receipts = self.credit_receipts.as_ref();
        time::timeout(deadline, async {
            self.stream.write_all(&size.to_be_bytes()).await?;
            // Quinn's write future returns Pending only when transport cannot accept more bytes.
            let mut pending_logged = false;
            let mut writing = pin!(self.stream.write_all(body));
            poll_fn(|context| {
                let update = writing.as_mut().poll(context);
                if update.is_pending() && !pending_logged {
                    pending_logged = true;
                    eprintln!(
                        "quinn-write-credit-pending stream={} bytes={size}",
                        stream.index()
                    );
                    if let Some(receipts) = receipts {
                        let _ = receipts.send(CreditPending {
                            stream,
                            frame_bytes: body.len(),
                        });
                    }
                }
                update
            })
            .await
        })
        .await
        .map_err(|_| FrameError::Deadline)??;
        Ok(())
    }

    pub fn finish(&mut self) -> Result<(), FrameError> {
        self.stream.finish()?;
        self.finished = true;
        Ok(())
    }
}

impl Drop for Sending {
    fn drop(&mut self) {
        if !self.finished {
            let id = self.stream.id();
            let outcome = self.stream.reset(ABORTED_CODE);
            eprintln!("quinn-send-reset stream={} outcome={outcome:?}", id.index());
        }
    }
}
