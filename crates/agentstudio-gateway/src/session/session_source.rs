use std::{future::Future, pin::Pin};

use agentstudio_gateway_contract::{
    observation_records::SessionObservation,
    session_contract::{InspectArguments, InspectResult, ObserveArguments},
};
use futures_util::Stream;
use tokio_util::sync::CancellationToken;

#[derive(Debug, thiserror::Error)]
pub enum SessionSourceError {
    #[error("Fixture session is outside the Gateway scope")]
    Denied,
    #[error("Session source is unavailable")]
    Unavailable,
}

pub type ObservationStream = Pin<Box<dyn Stream<Item = SessionObservation> + Send>>;

pub trait SessionSource: Send + Sync {
    fn inspect(
        &self,
        request: InspectArguments,
    ) -> impl Future<Output = Result<InspectResult, SessionSourceError>> + Send;
    fn observe(
        &self,
        request: ObserveArguments,
        cancellation: CancellationToken,
    ) -> impl Future<Output = Result<ObservationStream, SessionSourceError>> + Send;
}
