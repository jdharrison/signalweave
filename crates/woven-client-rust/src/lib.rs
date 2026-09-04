//! Reference Woven client used as the integration-test driver.
//!
//! Connects to a Woven server over QUIC or WebTransport, completes the
//! protocol handshake, and exposes simple methods for join/subscribe/publish/drain.
//!
//! - `quic://host:port` — native QUIC (quinn)
//! - `wtransport://host:port/path` — WebTransport (wtransport crate)
//!
//! Transport is selected automatically based on the URL scheme.

#![deny(unsafe_code)]

mod transform;

pub use transform::Transform;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use quinn::{ClientConfig as QuinnClientConfig, Endpoint, crypto::rustls::QuicClientConfig};
use tracing::{debug, trace};
use woven_protocol::{
    Authenticate, AuthenticationScheme, Codec, CodecError, ControlPayload, DeliveryClass, Envelope,
    Hello, InferenceRequested, JoinSession, MessagePayload, OpaquePayload, PROTOCOL_VERSION,
    ProtocolError, SnapshotRequest, SpaceTransition, SubscribeSpace,
};
use wtransport::ClientConfig as WtransportClientConfig;

/// Scheme parsed from the connection URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UrlScheme {
    /// `quic://host:port` — native QUIC (quinn).
    Quic,
    /// `wtransport://host:port/path` — WebTransport (wtransport crate).
    WebTransport,
}

/// Configuration for a [`Client`] connection.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// Connection URL.
    ///
    /// - `quic://host:port` for native QUIC
    /// - `wtransport://host:port/path` for WebTransport
    pub url: String,
    /// Bearer token sent to the server during the `Authenticate` handshake step.
    pub token: String,
    /// Maximum frame size advertised in `Hello` (bytes). Default: 65536.
    pub max_frame_bytes: u32,
    /// Maximum payload size advertised in `Hello` (bytes). Default: 65536.
    pub max_payload_bytes: u32,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            token: String::new(),
            max_frame_bytes: 65536,
            max_payload_bytes: 65536,
        }
    }
}

/// Errors returned by the Woven reference client.
#[derive(Debug)]
pub enum ClientError {
    /// Transport-level error (QUIC, WebTransport, or I/O).
    Transport(String),
    /// Frame-level codec or decoding error.
    Protocol(CodecError),
    /// The server sent a `ProtocolError` control message.
    ServerError(ProtocolError),
    /// The handshake did not complete as expected.
    HandshakeFailed(String),
    /// The connection was closed (no more messages).
    Closed,
    /// The URL scheme is not recognised.
    UnsupportedScheme(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(msg) => write!(f, "transport error: {msg}"),
            Self::Protocol(e) => write!(f, "protocol error: {e}"),
            Self::ServerError(e) => write!(f, "server protocol error: {:?}", e.code),
            Self::HandshakeFailed(msg) => write!(f, "handshake failed: {msg}"),
            Self::Closed => write!(f, "connection closed"),
            Self::UnsupportedScheme(s) => write!(f, "unsupported URL scheme: {s}"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Protocol(e) => Some(e),
            Self::Transport(_)
            | Self::ServerError(_)
            | Self::HandshakeFailed(_)
            | Self::Closed
            | Self::UnsupportedScheme(_) => None,
        }
    }
}

fn parse_url(url: &str) -> Result<(UrlScheme, String, u16, String), ClientError> {
    let lower = url.to_ascii_lowercase();
    if lower.starts_with("quic://") {
        let stripped = &url["quic://".len()..];
        let (host_port, path) = match stripped.find('/') {
            Some(i) => (&stripped[..i], &stripped[i..]),
            None => (stripped, "/"),
        };
        let (host, port) = parse_host_port(host_port, 4433)?;
        Ok((UrlScheme::Quic, host, port, path.to_owned()))
    } else if lower.starts_with("wtransport://") {
        let stripped = &url["wtransport://".len()..];
        let (host_port, path) = match stripped.find('/') {
            Some(i) => (&stripped[..i], &stripped[i..]),
            None => (stripped, "/"),
        };
        let (host, port) = parse_host_port(host_port, 4433)?;
        Ok((UrlScheme::WebTransport, host, port, path.to_owned()))
    } else {
        Err(ClientError::UnsupportedScheme(url.to_owned()))
    }
}

fn parse_host_port(host_port: &str, default_port: u16) -> Result<(String, u16), ClientError> {
    if let Some(i) = host_port.rfind(':') {
        let host = host_port[..i].to_owned();
        let port: u16 = host_port[i + 1..]
            .parse()
            .map_err(|_| ClientError::Transport(format!("invalid port in {host_port}")))?;
        Ok((host, port))
    } else {
        Ok((host_port.to_owned(), default_port))
    }
}

fn quic_client_endpoint(_server_host: &str) -> Result<Endpoint, ClientError> {
    // Development-only: accept any server certificate, mirroring the WebTransport
    // client's `with_no_cert_validation()`. Production clients MUST pin a CA.
    let client_crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(DevServerCertVerifier))
        .with_no_client_auth();
    let client_config = QuinnClientConfig::new(Arc::new(
        QuicClientConfig::try_from(client_crypto)
            .map_err(|e| ClientError::Transport(e.to_string()))?,
    ));
    let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())
        .map_err(|e| ClientError::Transport(e.to_string()))?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

/// Development-only certificate verifier that accepts any presented server
/// certificate. Must never be used in production.
#[derive(Debug)]
struct DevServerCertVerifier;

impl rustls::client::danger::ServerCertVerifier for DevServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}

async fn quic_connect(
    endpoint: &Endpoint,
    host: &str,
    port: u16,
) -> Result<quinn::Connection, ClientError> {
    let addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .map_err(|e| ClientError::Transport(format!("invalid address: {e}")))?;
    let connection = endpoint
        .connect(addr, host)
        .map_err(|e| ClientError::Transport(e.to_string()))?
        .await
        .map_err(|e| ClientError::Transport(e.to_string()))?;
    Ok(connection)
}

async fn wtransport_connect(
    host: &str,
    port: u16,
    path: &str,
) -> Result<wtransport::Connection, ClientError> {
    let client_config = WtransportClientConfig::builder()
        .with_bind_default()
        .with_no_cert_validation()
        .build();
    let endpoint = wtransport::Endpoint::client(client_config)
        .map_err(|e| ClientError::Transport(e.to_string()))?;
    let url = format!("https://{host}:{port}{path}");
    let connection = tokio::time::timeout(Duration::from_secs(5), endpoint.connect(&url))
        .await
        .map_err(|_| ClientError::Transport("WebTransport connect timed out".to_owned()))?
        .map_err(|e| ClientError::Transport(e.to_string()))?;
    Ok(connection)
}

/// Read a single size-prefixed codec frame from a quinn `RecvStream`.
async fn read_quinn_envelope(
    stream: &mut quinn::RecvStream,
    codec: &Codec,
) -> Result<Envelope, ClientError> {
    let mut prefix = [0_u8; 4];
    stream
        .read_exact(&mut prefix)
        .await
        .map_err(|e| ClientError::Transport(e.to_string()))?;
    let frame_len = codec
        .expected_frame_len(&prefix)
        .map_err(ClientError::Protocol)?
        .ok_or_else(|| ClientError::Transport("incomplete size prefix".to_owned()))?;
    let mut frame = vec![0_u8; frame_len];
    frame[..prefix.len()].copy_from_slice(&prefix);
    stream
        .read_exact(&mut frame[prefix.len()..])
        .await
        .map_err(|e| ClientError::Transport(e.to_string()))?;
    codec.decode(&frame).map_err(ClientError::Protocol)
}

/// Read a single size-prefixed codec frame from a wtransport `RecvStream`.
async fn read_wtransport_envelope(
    stream: &mut wtransport::RecvStream,
    codec: &Codec,
) -> Result<Envelope, ClientError> {
    let mut prefix = [0_u8; 4];
    stream
        .read_exact(&mut prefix)
        .await
        .map_err(|e| ClientError::Transport(e.to_string()))?;
    let frame_len = codec
        .expected_frame_len(&prefix)
        .map_err(ClientError::Protocol)?
        .ok_or_else(|| ClientError::Transport("incomplete size prefix".to_owned()))?;
    let mut frame = vec![0_u8; frame_len];
    frame[..prefix.len()].copy_from_slice(&prefix);
    stream
        .read_exact(&mut frame[prefix.len()..])
        .await
        .map_err(|e| ClientError::Transport(e.to_string()))?;
    codec.decode(&frame).map_err(ClientError::Protocol)
}

enum Transport {
    Quic {
        connection: quinn::Connection,
        send: quinn::SendStream,
        recv: quinn::RecvStream,
    },
    WebTransport {
        connection: wtransport::Connection,
        send: wtransport::SendStream,
        recv: wtransport::RecvStream,
    },
}

/// Reference Woven client, used as the integration-test driver.
///
/// Connects over QUIC or WebTransport (auto-selected by URL scheme) and wraps
/// a [`Codec`] for encoding/decoding [`Envelope`]s.  Use [`Client::connect`]
/// to establish a connection and complete the protocol handshake.
pub struct Client {
    transport: Transport,
    codec: Codec,
}

impl Client {
    /// Connect to the server and complete the
    /// `Hello → Capabilities → Authenticate → Authenticated` handshake.
    ///
    /// URL scheme determines the transport:
    /// - `quic://host:port` — native QUIC (self-signed cert accepted in dev)
    /// - `wtransport://host:port/path` — WebTransport (no cert validation)
    #[allow(clippy::too_many_lines)]
    pub async fn connect(config: ClientConfig) -> Result<Self, ClientError> {
        debug!(url = %config.url, "connecting to woven server");

        let (scheme, host, port, path) = parse_url(&config.url)?;
        let codec = Codec::default();

        let mut client = match scheme {
            UrlScheme::Quic => {
                let endpoint = quic_client_endpoint(&host)?;
                let connection = quic_connect(&endpoint, &host, port).await?;
                let (send, recv) = connection
                    .clone()
                    .open_bi()
                    .await
                    .map_err(|e| ClientError::Transport(e.to_string()))?;
                Self {
                    transport: Transport::Quic {
                        connection,
                        send,
                        recv,
                    },
                    codec,
                }
            }
            UrlScheme::WebTransport => {
                let connection = wtransport_connect(&host, port, &path).await?;
                let (send, recv) = connection
                    .clone()
                    .open_bi()
                    .await
                    .map_err(|e| ClientError::Transport(e.to_string()))?
                    .await
                    .map_err(|e| ClientError::Transport(e.to_string()))?;
                Self {
                    transport: Transport::WebTransport {
                        connection,
                        send,
                        recv,
                    },
                    codec,
                }
            }
        };

        // ── Hello ────────────────────────────────────────────────────────────
        client
            .send_envelope(&Envelope {
                protocol_version: PROTOCOL_VERSION,
                delivery_class: DeliveryClass::ReliableOrdered,
                namespace_id: 0,
                session_id: 0,
                space_id: 0,
                channel_id: None,
                entity_id: None,
                space_epoch: 0,
                server_tick: 0,
                sender_sequence: 0,
                correlation_id: None,
                message: MessagePayload::Control(ControlPayload::Hello(Hello {
                    min_protocol_version: 1,
                    max_protocol_version: 1,
                    client_name: "woven-client".to_owned(),
                    client_version: env!("CARGO_PKG_VERSION").to_owned(),
                    capability_bits: 0,
                    max_frame_size: config.max_frame_bytes,
                    max_payload_size: config.max_payload_bytes,
                })),
            })
            .await?;
        trace!("sent Hello");

        // ── Capabilities ─────────────────────────────────────────────────────
        match client.recv().await?.message {
            MessagePayload::Control(ControlPayload::Capabilities(_)) => {
                trace!("received Capabilities");
            }
            MessagePayload::Control(ControlPayload::ProtocolError(e)) => {
                return Err(ClientError::ServerError(e));
            }
            other => {
                return Err(ClientError::HandshakeFailed(format!(
                    "expected Capabilities, got {:?}",
                    other.message_kind()
                )));
            }
        }

        // ── Authenticate ──────────────────────────────────────────────────────
        client
            .send_envelope(&Envelope {
                protocol_version: PROTOCOL_VERSION,
                delivery_class: DeliveryClass::ReliableOrdered,
                namespace_id: 0,
                session_id: 0,
                space_id: 0,
                channel_id: None,
                entity_id: None,
                space_epoch: 0,
                server_tick: 0,
                sender_sequence: 0,
                correlation_id: None,
                message: MessagePayload::Control(ControlPayload::Authenticate(Authenticate {
                    scheme: AuthenticationScheme::Development,
                    credentials: config.token.into_bytes(),
                })),
            })
            .await?;
        trace!("sent Authenticate");

        // ── Authenticated ─────────────────────────────────────────────────────
        match client.recv().await?.message {
            MessagePayload::Control(ControlPayload::Authenticated(_)) => {
                trace!("received Authenticated — handshake complete");
            }
            MessagePayload::Control(ControlPayload::ProtocolError(e)) => {
                return Err(ClientError::ServerError(e));
            }
            other => {
                return Err(ClientError::HandshakeFailed(format!(
                    "expected Authenticated, got {:?}",
                    other.message_kind()
                )));
            }
        }

        Ok(client)
    }

    /// Send a `JoinSession` control envelope.
    pub async fn join_session(
        &mut self,
        namespace_id: u64,
        session_id: u64,
    ) -> Result<(), ClientError> {
        self.send_envelope(&Envelope {
            protocol_version: PROTOCOL_VERSION,
            delivery_class: DeliveryClass::ReliableOrdered,
            namespace_id,
            session_id,
            space_id: 0,
            channel_id: None,
            entity_id: None,
            space_epoch: 0,
            server_tick: 0,
            sender_sequence: 0,
            correlation_id: None,
            message: MessagePayload::Control(ControlPayload::JoinSession(JoinSession {
                resume_token: vec![],
            })),
        })
        .await
    }

    /// Send a `SubscribeSpace` control envelope.
    pub async fn subscribe_space(
        &mut self,
        namespace_id: u64,
        session_id: u64,
        space_id: u64,
        space_epoch: u64,
        channel_id: u64,
    ) -> Result<(), ClientError> {
        self.send_envelope(&Envelope {
            protocol_version: PROTOCOL_VERSION,
            delivery_class: DeliveryClass::ReliableOrdered,
            namespace_id,
            session_id,
            space_id,
            channel_id: Some(channel_id),
            entity_id: None,
            space_epoch,
            server_tick: 0,
            sender_sequence: 0,
            correlation_id: None,
            message: MessagePayload::Control(ControlPayload::SubscribeSpace(SubscribeSpace)),
        })
        .await
    }

    /// Move an entity between two subscribed spaces.
    #[allow(clippy::too_many_arguments)]
    pub async fn transition_entity(
        &mut self,
        namespace_id: u64,
        session_id: u64,
        source_space_id: u64,
        source_epoch: u64,
        destination_space_id: u64,
        destination_epoch: u64,
        entity_id: u64,
    ) -> Result<(), ClientError> {
        self.send_envelope(&Envelope {
            protocol_version: PROTOCOL_VERSION,
            delivery_class: DeliveryClass::ReliableOrdered,
            namespace_id,
            session_id,
            space_id: source_space_id,
            channel_id: None,
            entity_id: Some(entity_id),
            space_epoch: source_epoch,
            server_tick: 0,
            sender_sequence: 0,
            correlation_id: None,
            message: MessagePayload::Control(ControlPayload::SpaceTransition(SpaceTransition {
                from_space_id: source_space_id,
                to_space_id: destination_space_id,
                to_space_epoch: destination_epoch,
            })),
        })
        .await
    }

    /// Request a scoped opaque snapshot from the server.
    pub async fn request_snapshot(
        &mut self,
        namespace_id: u64,
        session_id: u64,
        space_id: u64,
        space_epoch: u64,
        channel_id: u64,
    ) -> Result<(), ClientError> {
        self.send_envelope(&Envelope {
            protocol_version: PROTOCOL_VERSION,
            delivery_class: DeliveryClass::ReliableOrdered,
            namespace_id,
            session_id,
            space_id,
            channel_id: Some(channel_id),
            entity_id: None,
            space_epoch,
            server_tick: 0,
            sender_sequence: 0,
            correlation_id: None,
            message: MessagePayload::Control(ControlPayload::SnapshotRequest(SnapshotRequest {
                after_server_tick: None,
            })),
        })
        .await
    }

    /// Send a `ReliableEvent` opaque payload envelope.
    ///
    /// `sequence` must be strictly monotone per connection × space × epoch × entity × channel.
    #[allow(clippy::too_many_arguments)]
    pub async fn publish_event(
        &mut self,
        namespace_id: u64,
        session_id: u64,
        space_id: u64,
        space_epoch: u64,
        channel_id: u64,
        entity_id: u64,
        sequence: u64,
        type_id: u64,
        payload: Vec<u8>,
    ) -> Result<(), ClientError> {
        self.send_envelope(&Envelope {
            protocol_version: PROTOCOL_VERSION,
            delivery_class: DeliveryClass::ReliableOrdered,
            namespace_id,
            session_id,
            space_id,
            channel_id: Some(channel_id),
            entity_id: Some(entity_id),
            space_epoch,
            server_tick: 0,
            sender_sequence: sequence,
            correlation_id: None,
            message: MessagePayload::ReliableEvent(OpaquePayload {
                type_id,
                bytes: payload,
            }),
        })
        .await
    }

    /// Send a `LatestValue` (`EntityState`) opaque payload envelope.
    ///
    /// `sequence` must be strictly monotone per connection × space × epoch × entity × channel.
    /// The payload `type_id` identifies the coalesce component. Protocol v1 has no separate
    /// coalesce-component field, so applications that need independent values use distinct type IDs.
    #[allow(clippy::too_many_arguments)]
    pub async fn publish_state(
        &mut self,
        namespace_id: u64,
        session_id: u64,
        space_id: u64,
        space_epoch: u64,
        channel_id: u64,
        entity_id: u64,
        sequence: u64,
        type_id: u64,
        payload: Vec<u8>,
    ) -> Result<(), ClientError> {
        self.send_envelope(&Envelope {
            protocol_version: PROTOCOL_VERSION,
            delivery_class: DeliveryClass::LatestValue,
            namespace_id,
            session_id,
            space_id,
            channel_id: Some(channel_id),
            entity_id: Some(entity_id),
            space_epoch,
            server_tick: 0,
            sender_sequence: sequence,
            correlation_id: None,
            message: MessagePayload::EntityState(OpaquePayload {
                type_id,
                bytes: payload,
            }),
        })
        .await
    }

    /// Send an `InferenceRequested` control envelope addressed to the AI identity's entity.
    #[allow(clippy::too_many_arguments)]
    pub async fn request_inference(
        &mut self,
        namespace_id: u64,
        session_id: u64,
        space_id: u64,
        space_epoch: u64,
        ai_entity_id: u64,
        capability: &str,
        deadline_ms: u64,
        input: Vec<u8>,
    ) -> Result<(), ClientError> {
        self.send_envelope(&Envelope {
            protocol_version: PROTOCOL_VERSION,
            delivery_class: DeliveryClass::ReliableOrdered,
            namespace_id,
            session_id,
            space_id,
            channel_id: None,
            entity_id: Some(ai_entity_id),
            space_epoch,
            server_tick: 0,
            sender_sequence: 0,
            correlation_id: None,
            message: MessagePayload::Control(ControlPayload::InferenceRequested(
                InferenceRequested {
                    capability: capability.to_owned(),
                    deadline_ms,
                    input,
                },
            )),
        })
        .await
    }

    /// Receive the next decoded [`Envelope`], blocking until one arrives or an
    /// error occurs.
    pub async fn recv(&mut self) -> Result<Envelope, ClientError> {
        match &mut self.transport {
            Transport::Quic { recv, .. } => read_quinn_envelope(recv, &self.codec).await,
            Transport::WebTransport { recv, .. } => {
                read_wtransport_envelope(recv, &self.codec).await
            }
        }
    }

    /// Try to receive the next [`Envelope`] within `duration`.
    ///
    /// Returns `Ok(None)` if the timeout elapses before a frame arrives.
    pub async fn recv_timeout(
        &mut self,
        duration: Duration,
    ) -> Result<Option<Envelope>, ClientError> {
        match tokio::time::timeout(duration, self.recv()).await {
            Ok(result) => result.map(Some),
            Err(_elapsed) => Ok(None),
        }
    }

    /// Close the connection gracefully.
    pub fn close(self) -> Result<(), ClientError> {
        match self.transport {
            Transport::Quic { connection, .. } => {
                connection.close(quinn::VarInt::from_u32(0), b"client closed");
            }
            Transport::WebTransport { connection, .. } => {
                connection.close(wtransport::VarInt::from_u32(0), b"client closed");
            }
        }
        Ok(())
    }

    /// Encode `envelope` and send it on the transport's bidirectional stream.
    async fn send_envelope(&mut self, envelope: &Envelope) -> Result<(), ClientError> {
        let bytes = self.codec.encode(envelope).map_err(ClientError::Protocol)?;
        trace!(kind = ?envelope.message_kind(), len = bytes.len(), "sending envelope");
        match &mut self.transport {
            Transport::Quic { send, .. } => send
                .write_all(&bytes)
                .await
                .map_err(|e| ClientError::Transport(e.to_string())),
            Transport::WebTransport { send, .. } => send
                .write_all(&bytes)
                .await
                .map_err(|e| ClientError::Transport(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_quic_url() {
        let (scheme, host, port, path) = parse_url("quic://127.0.0.1:4433").unwrap();
        assert_eq!(scheme, UrlScheme::Quic);
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 4433);
        assert_eq!(path, "/");
    }

    #[test]
    fn parse_wtransport_url() {
        let (scheme, host, port, path) =
            parse_url("wtransport://127.0.0.1:8082/webtransport").unwrap();
        assert_eq!(scheme, UrlScheme::WebTransport);
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 8082);
        assert_eq!(path, "/webtransport");
    }

    #[test]
    fn parse_quic_url_default_port() {
        let (scheme, host, port, _path) = parse_url("quic://localhost").unwrap();
        assert_eq!(scheme, UrlScheme::Quic);
        assert_eq!(host, "localhost");
        assert_eq!(port, 4433);
    }
}
