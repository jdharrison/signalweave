//! Reference Signalweave client used as the integration-test driver.
//!
//! Connects to a Signalweave WebSocket server, completes the protocol handshake,
//! and exposes simple methods for join/subscribe/publish/drain.

#![deny(unsafe_code)]

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use signalweave_protocol::{
    Authenticate, AuthenticationScheme, Codec, CodecError, ControlPayload, DeliveryClass, Envelope,
    Hello, InferenceRequested, JoinSession, MessagePayload, OpaquePayload, PROTOCOL_VERSION,
    ProtocolError, SnapshotRequest, SpaceTransition, SubscribeSpace,
};
use tokio_tungstenite::{MaybeTlsStream, connect_async, tungstenite::Message};
use tracing::{debug, trace};

/// Configuration for a [`Client`] connection.
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// WebSocket URL, e.g. `"ws://127.0.0.1:8080/ws"`.
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

/// Errors returned by the Signalweave reference client.
#[derive(Debug)]
pub enum ClientError {
    /// Transport-level error (WebSocket or I/O).
    Transport(String),
    /// Frame-level codec or decoding error.
    Protocol(CodecError),
    /// The server sent a `ProtocolError` control message.
    ServerError(ProtocolError),
    /// The handshake did not complete as expected.
    HandshakeFailed(String),
    /// The connection was closed (no more messages).
    Closed,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Transport(msg) => write!(f, "transport error: {msg}"),
            Self::Protocol(e) => write!(f, "protocol error: {e}"),
            Self::ServerError(e) => write!(f, "server protocol error: {:?}", e.code),
            Self::HandshakeFailed(msg) => write!(f, "handshake failed: {msg}"),
            Self::Closed => write!(f, "connection closed"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Protocol(e) => Some(e),
            Self::Transport(_) | Self::ServerError(_) | Self::HandshakeFailed(_) | Self::Closed => {
                None
            }
        }
    }
}

type WsStream = tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Reference Signalweave client, used as the integration-test driver.
///
/// Wraps a WebSocket stream and a [`Codec`] for encoding/decoding
/// [`Envelope`]s. Use [`Client::connect`] to establish a connection and
/// complete the protocol handshake.
pub struct Client {
    ws: WsStream,
    codec: Codec,
}

impl Client {
    /// Connect to the server and complete the
    /// `Hello → Capabilities → Authenticate → Authenticated` handshake.
    ///
    /// Returns `Err` if:
    /// - the TCP/WebSocket connection fails,
    /// - any frame cannot be encoded or decoded,
    /// - the server sends an unexpected message kind, or
    /// - the server responds with a `ProtocolError`.
    pub async fn connect(config: ClientConfig) -> Result<Self, ClientError> {
        debug!(url = %config.url, "connecting to signalweave server");

        let (ws, _response) = connect_async(config.url.as_str())
            .await
            .map_err(|e| ClientError::Transport(e.to_string()))?;

        let codec = Codec::default();
        let mut client = Self { ws, codec };

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
                    client_name: "signalweave-client-rust".to_owned(),
                    client_version: "0.1.0".to_owned(),
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
    ///
    /// Non-binary frames (Ping, Pong) are silently skipped; tungstenite handles
    /// Ping/Pong automatically at the transport layer.
    pub async fn recv(&mut self) -> Result<Envelope, ClientError> {
        loop {
            let msg = self
                .ws
                .next()
                .await
                .ok_or(ClientError::Closed)?
                .map_err(|e| ClientError::Transport(e.to_string()))?;

            match msg {
                Message::Binary(bytes) => {
                    trace!(len = bytes.len(), "received binary frame");
                    return self
                        .codec
                        .decode(bytes.as_ref())
                        .map_err(ClientError::Protocol);
                }
                Message::Close(_) => return Err(ClientError::Closed),
                // Ping/Pong are handled by tungstenite internally; Text is rejected by the server.
                _ => {}
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

    /// Close the WebSocket connection gracefully.
    pub async fn close(mut self) -> Result<(), ClientError> {
        self.ws
            .close(None)
            .await
            .map_err(|e| ClientError::Transport(e.to_string()))
    }

    /// Encode `envelope` and send it as a binary WebSocket frame.
    async fn send_envelope(&mut self, envelope: &Envelope) -> Result<(), ClientError> {
        let bytes = self.codec.encode(envelope).map_err(ClientError::Protocol)?;
        trace!(kind = ?envelope.message_kind(), len = bytes.len(), "sending envelope");
        self.ws
            .send(Message::Binary(bytes.into()))
            .await
            .map_err(|e| ClientError::Transport(e.to_string()))
    }
}
