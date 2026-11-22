use std::convert::Infallible;

use bytes::Bytes;
use http_body_util::combinators::UnsyncBoxBody;
use hyper::Response;

mod observation_response;
pub mod unix_gateway_server;

type GatewayBody = UnsyncBoxBody<Bytes, Infallible>;
type GatewayResponse = Response<GatewayBody>;
