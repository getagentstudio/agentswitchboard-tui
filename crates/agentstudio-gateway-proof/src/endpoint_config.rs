use std::{net::SocketAddr, sync::Arc, time::Duration};

use quinn::{
    ClientConfig, Endpoint, ServerConfig, TransportConfig, VarInt,
    rustls::{
        RootCertStore,
        pki_types::{CertificateDer, PrivatePkcs8KeyDer},
    },
};

pub const SERVER_NAME: &str = "agstui-fixture.invalid";
pub const STREAM_CREDIT: u32 = 16 * 1024;
pub const CONNECTION_CREDIT: u32 = 256 * 1024;
pub const CONCURRENT_STREAMS: u32 = 16;

#[derive(Debug, thiserror::Error)]
pub enum EndpointError {
    #[error("proof endpoints must bind and connect on explicit loopback addresses")]
    NonLoopback,
    #[error("certificate generation: {0}")]
    Certificate(#[from] rcgen::Error),
    #[error("endpoint TLS configuration: {0}")]
    Tls(String),
    #[error("endpoint I/O: {0}")]
    Io(#[from] std::io::Error),
}

pub fn proof_server(
    bind: SocketAddr,
) -> Result<(Endpoint, CertificateDer<'static>), EndpointError> {
    require_loopback(bind)?;
    let identity = rcgen::generate_simple_self_signed(vec![SERVER_NAME.into()])?;
    let certificate = identity.cert.der().clone();
    let key = PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der());
    let mut config = ServerConfig::with_single_cert(vec![certificate.clone()], key.into())
        .map_err(|error| EndpointError::Tls(error.to_string()))?;
    config.transport_config(transport_config(STREAM_CREDIT));
    Ok((Endpoint::server(config, bind)?, certificate))
}

pub fn proof_client(
    bind: SocketAddr,
    certificate: CertificateDer<'static>,
) -> Result<Endpoint, EndpointError> {
    proof_client_with_credit(bind, certificate, STREAM_CREDIT)
}

pub fn proof_client_with_credit(
    bind: SocketAddr,
    certificate: CertificateDer<'static>,
    stream_credit: u32,
) -> Result<Endpoint, EndpointError> {
    require_loopback(bind)?;
    let mut roots = RootCertStore::empty();
    roots
        .add(certificate)
        .map_err(|error| EndpointError::Tls(error.to_string()))?;
    let mut config = ClientConfig::with_root_certificates(Arc::new(roots))
        .map_err(|error| EndpointError::Tls(error.to_string()))?;
    config.transport_config(transport_config(stream_credit));
    let mut endpoint = Endpoint::client(bind)?;
    endpoint.set_default_client_config(config);
    Ok(endpoint)
}

pub fn require_loopback(address: SocketAddr) -> Result<(), EndpointError> {
    if address.ip().is_loopback() {
        Ok(())
    } else {
        Err(EndpointError::NonLoopback)
    }
}

fn transport_config(stream_credit: u32) -> Arc<TransportConfig> {
    let mut transport = TransportConfig::default();
    transport
        .max_concurrent_bidi_streams(VarInt::from_u32(CONCURRENT_STREAMS))
        .max_concurrent_uni_streams(VarInt::from_u32(0))
        .stream_receive_window(VarInt::from_u32(stream_credit))
        .receive_window(VarInt::from_u32(CONNECTION_CREDIT))
        .send_window(u64::from(CONNECTION_CREDIT))
        .keep_alive_interval(Some(Duration::from_secs(3)));
    Arc::new(transport)
}
