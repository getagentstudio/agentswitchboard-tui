use std::{error::Error, net::SocketAddr, os::unix::fs::PermissionsExt, path::Path};

use agentstudio_gateway::{
    fixture::fixture_session_source::fixture_target, unix_http::unix_gateway_server::GatewayServer,
};
use agentstudio_gateway_proof::{
    endpoint_config::{
        CONCURRENT_STREAMS, CONNECTION_CREDIT, STREAM_CREDIT, proof_client, proof_server,
    },
    fixture_control::{FixtureControl, control_call},
    peer_transport::{PeerBounds, PeerControls, PeerTransport},
    quinn_session_source::QuinnSessionSource,
    witness_fixture::{WitnessFixture, WitnessHold},
};
use quinn::rustls::pki_types::CertificateDer;
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    match arguments.as_slice() {
        [mode, directory, history_one, history_two, hold] if mode == "peer" => {
            let hold = match hold.as_str() {
                "open" => WitnessHold::Released,
                "hold-first" => WitnessHold::BeforeFirst,
                "hold-cut" => WitnessHold::BeforeCut,
                _ => return Err("unknown proof hold mode".into()),
            };
            serve_peer(Path::new(directory), [history_one.clone(), history_two.clone()], hold).await
        }
        [mode, socket, peer, certificate] if mode == "gateway" => {
            serve_gateway(Path::new(socket), peer.parse()?, Path::new(certificate)).await
        }
        [mode, socket, data] if mode == "control" => {
            println!("{}", control_call(Path::new(socket), data.as_bytes()).await?);
            Ok(())
        }
        _ => Err("Usage: agentstudio-gateway-proof peer <private dir> <history-one> <history-two> <open|hold-first|hold-cut> | gateway <socket> <loopback peer> <certificate.der> | control <socket> <json>".into()),
    }
}

async fn serve_peer(
    directory: &Path,
    history: [String; 2],
    hold: WitnessHold,
) -> Result<(), Box<dyn Error>> {
    let metadata = tokio::fs::symlink_metadata(directory).await?;
    if !directory.is_absolute()
        || !metadata.is_dir()
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        return Err("proof peer requires an absolute private directory".into());
    }
    let (endpoint, cert) = proof_server("127.0.0.1:0".parse()?)?;
    let peer = endpoint.local_addr()?;
    let cert_path = directory.join("peer.der");
    let control_socket = directory.join("peer.sock");
    let controls = PeerControls::default();
    let fixture = WitnessFixture::new(fixture_target(), history, hold);
    let control = FixtureControl::bind(&control_socket, fixture.clone(), controls.clone()).await?;
    tokio::fs::write(&cert_path, cert.as_ref()).await?;
    let stop = CancellationToken::new();
    let bounds = PeerBounds::default();
    let transport = PeerTransport {
        endpoint,
        source: fixture,
        allowed_target: fixture_target(),
        bounds,
        controls,
    };

    eprintln!(
        "quinn-proof-peer-ready endpoint={peer} control={} cert={} intake_ms={} write_ms={} stream_credit={STREAM_CREDIT} connection_credit={CONNECTION_CREDIT} active_streams={CONCURRENT_STREAMS} hold={hold:?}",
        control_socket.display(),
        cert_path.display(),
        bounds.intake.as_millis(),
        bounds.write.as_millis()
    );
    let serving = transport.serve(stop.clone());
    let control_loop = control.serve(stop.clone());
    tokio::pin!(serving, control_loop);
    tokio::select! {
        () = &mut serving => { stop.cancel(); control_loop.await?; }
        result = &mut control_loop => { stop.cancel(); serving.await; result?; }
        result = shutdown_signal() => { stop.cancel(); serving.await; control_loop.await?; result?; }
    }
    Ok(())
}

async fn serve_gateway(
    socket: &Path,
    peer: SocketAddr,
    certificate: &Path,
) -> Result<(), Box<dyn Error>> {
    let der = CertificateDer::from(tokio::fs::read(certificate).await?);
    let endpoint = proof_client("127.0.0.1:0".parse()?, der)?;
    eprintln!(
        "quinn-proof-gateway-route local={} remote={peer}",
        endpoint.local_addr()?
    );
    let source = QuinnSessionSource::new(endpoint, peer)?;
    let stop = CancellationToken::new();
    let server = GatewayServer::bind(socket, source, fixture_target()).await?;
    let serving = server.serve(stop.clone());
    tokio::pin!(serving);
    tokio::select! {
        result = &mut serving => result?,
        result = shutdown_signal() => {
            stop.cancel();
            serving.await?;
            result?;
        }
    }
    Ok(())
}

async fn shutdown_signal() -> Result<(), std::io::Error> {
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    tokio::select! {
        _ = terminate.recv() => {}
        _ = interrupt.recv() => {}
    }
    Ok(())
}
