use std::{path::PathBuf, sync::Arc};

use agentstudio_gateway::{
    fixture::fixture_session_source::{FixtureSessionSource, fixture_target},
    unix_http::unix_gateway_server::GatewayServer,
};
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let path = match (args.next(), args.next(), args.next()) {
        (Some(option), Some(path), None) if option == "--fixture-socket" => PathBuf::from(path),
        _ => {
            return Err(
                "Usage: agentstudio-gateway --fixture-socket <absolute socket path>".into(),
            );
        }
    };
    let mut publish = signal(SignalKind::user_defined1())?;
    let mut reset = signal(SignalKind::user_defined2())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let source = Arc::new(FixtureSessionSource::default());
    let stop = CancellationToken::new();
    let server = GatewayServer::bind(&path, source.clone(), fixture_target()).await?;
    let serving = server.serve(stop.clone());
    tokio::pin!(serving);

    let mut published = 0_u64;
    loop {
        tokio::select! {
            result = &mut serving => return result.map_err(Into::into),
            _ = terminate.recv() => break,
            _ = interrupt.recv() => break,
            _ = publish.recv() => {
                published += 1;
                source.append_notice(format!("Fixture live update {published}"))?;
            }
            _ = reset.recv() => source.reset_epoch()?,
        }
    }
    stop.cancel();
    serving.await?;
    Ok(())
}
