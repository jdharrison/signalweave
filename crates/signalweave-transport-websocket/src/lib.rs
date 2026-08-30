//! Binary WebSocket transport adapter.

#![deny(unsafe_code)]

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use signalweave_core::{Command, CommandResult, Credentials};
use signalweave_protocol::{
    Authenticated, Capabilities, Codec, ControlPayload, DeliveryClass, Envelope, MessageKind,
    MessagePayload, PROTOCOL_VERSION, ProtocolErrorCode,
};
use tokio::sync::mpsc;
use tracing::debug;

pub use signalweave_transport::*;

const WRITE_CAPACITY: usize = 128;

/// Configuration shared by WebSocket connection handlers.
#[derive(Clone)]
pub struct WebSocketConfig {
    pub worker: WorkerHandle,
    pub server_name: Arc<str>,
    pub server_version: Arc<str>,
}

impl WebSocketConfig {
    #[must_use]
    pub fn new(worker: WorkerHandle) -> Self {
        Self {
            worker,
            server_name: Arc::from("signalweave"),
            server_version: Arc::from(env!("CARGO_PKG_VERSION")),
        }
    }
}

/// Run one upgraded WebSocket connection until it closes or a protocol error occurs.
#[allow(
    clippy::manual_let_else,
    clippy::match_same_arms,
    clippy::single_match_else,
    clippy::too_many_lines
)]
pub async fn serve_connection(socket: WebSocket, config: WebSocketConfig) {
    let codec = Codec::default();
    let connection = match config.worker.execute(Command::TransportConnected).await {
        Ok(CommandResult::Connected(connection)) => connection,
        Ok(_) | Err(_) => return,
    };
    let (mut writer, mut reader) = socket.split();
    let (write_sender, mut write_receiver) = mpsc::channel::<Envelope>(WRITE_CAPACITY);
    let (shutdown_sender, mut shutdown_receiver) = mpsc::channel::<()>(1);
    let writer_codec = codec.clone();
    let writer_worker = config.worker.clone();
    let writer_shutdown = shutdown_sender.clone();
    let writer_task = tokio::spawn(async move {
        while let Some(envelope) = write_receiver.recv().await {
            let Ok(frame) = writer_codec.encode(&envelope) else {
                let _ = writer_shutdown.try_send(());
                writer_worker.discard_and_disconnect(connection).await;
                return;
            };
            if writer.send(Message::Binary(frame.into())).await.is_err() {
                let _ = writer_shutdown.try_send(());
                writer_worker.discard_and_disconnect(connection).await;
                return;
            }
        }
    });

    if config
        .worker
        .register_lifecycle(connection, write_sender.clone(), shutdown_sender.clone())
        .await
        .is_err()
    {
        config.worker.discard_and_disconnect(connection).await;
        drop(write_sender);
        writer_task.abort();
        return;
    }

    let drain_worker = config.worker.clone();
    let drain_sender = write_sender.clone();
    let drain_shutdown = shutdown_sender.clone();
    let drain_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(10));
        loop {
            interval.tick().await;
            if flush_outbound(&drain_worker, connection, &drain_sender)
                .await
                .is_err()
            {
                let _ = drain_shutdown.try_send(());
                drain_worker.discard_and_disconnect(connection).await;
                return;
            }
        }
    });

    let mut authenticated = false;
    let mut greeted = false;
    loop {
        let frame = tokio::select! {
            _ = shutdown_receiver.recv() => break,
            frame = reader.next() => frame,
        };
        let Some(frame) = frame else {
            break;
        };
        let envelope = match frame {
            Ok(Message::Binary(bytes)) => match codec.decode(bytes.as_ref()) {
                Ok(envelope) => envelope,
                Err(error) => {
                    send_error(
                        &write_sender,
                        MessageKind::Unknown,
                        ProtocolErrorCode::MalformedFrame,
                        error.to_string(),
                    )
                    .await;
                    break;
                }
            },
            Ok(Message::Text(_)) => {
                send_error(
                    &write_sender,
                    MessageKind::Unknown,
                    ProtocolErrorCode::MalformedFrame,
                    "text WebSocket frames are not supported".to_owned(),
                )
                .await;
                break;
            }
            Ok(Message::Close(_)) => break,
            Ok(Message::Ping(_) | Message::Pong(_)) => continue,
            Err(_) => break,
        };

        let result = if !greeted {
            if !matches!(
                envelope.message,
                MessagePayload::Control(ControlPayload::Hello(_))
            ) {
                send_error(
                    &write_sender,
                    envelope.message_kind(),
                    ProtocolErrorCode::UnsupportedMessage,
                    "expected Hello".to_owned(),
                )
                .await;
                break;
            }
            greeted = true;
            send_envelope(
                &write_sender,
                Envelope::control(
                    DeliveryClass::ReliableOrdered,
                    ControlPayload::Capabilities(Capabilities {
                        selected_protocol_version: PROTOCOL_VERSION,
                        server_name: config.server_name.to_string(),
                        server_version: config.server_version.to_string(),
                        capability_bits: 0,
                        max_frame_size: MAX_FRAME_BYTES,
                        max_payload_size: MAX_PAYLOAD_BYTES,
                    }),
                ),
            )
            .await
        } else if !authenticated {
            match envelope.message {
                MessagePayload::Control(ControlPayload::Authenticate(auth)) => {
                    let token = match String::from_utf8(auth.credentials) {
                        Ok(token) => token,
                        Err(_) => {
                            send_error(
                                &write_sender,
                                MessageKind::Authenticate,
                                ProtocolErrorCode::AuthenticationRequired,
                                "credentials must be UTF-8".to_owned(),
                            )
                            .await;
                            break;
                        }
                    };
                    match config
                        .worker
                        .execute(Command::Authenticate {
                            connection,
                            credentials: Credentials::new(token),
                        })
                        .await
                    {
                        Ok(CommandResult::Authenticated(principal)) => {
                            authenticated = true;
                            send_envelope(
                                &write_sender,
                                Envelope::control(
                                    DeliveryClass::ReliableOrdered,
                                    ControlPayload::Authenticated(Authenticated {
                                        principal_id: principal.get(),
                                        assigned_entity_id: None,
                                    }),
                                ),
                            )
                            .await
                        }
                        Ok(_) | Err(_) => {
                            send_error(
                                &write_sender,
                                MessageKind::Authenticate,
                                ProtocolErrorCode::Unauthorized,
                                "authentication failed".to_owned(),
                            )
                            .await;
                            break;
                        }
                    }
                }
                _ => {
                    send_error(
                        &write_sender,
                        envelope.message_kind(),
                        ProtocolErrorCode::AuthenticationRequired,
                        "expected Authenticate".to_owned(),
                    )
                    .await;
                    break;
                }
            }
        } else {
            handle_authenticated(&config.worker, connection, envelope, &write_sender).await
        };
        if result.is_err() {
            break;
        }
    }

    drain_task.abort();
    config.worker.discard_and_disconnect(connection).await;
    drop(write_sender);
    drop(shutdown_sender);
    let _ = writer_task.await;
    debug!(?connection, "WebSocket connection closed");
}
