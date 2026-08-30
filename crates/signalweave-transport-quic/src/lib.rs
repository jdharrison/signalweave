//! Native QUIC transport adapter.
//!
//! This adapter currently carries all Signalweave control and data envelopes on the first
//! bidirectional QUIC stream with reliable, ordered delivery. It intentionally does not map
//! Signalweave delivery classes to QUIC datagrams yet.

#![deny(unsafe_code)]

use std::{net::SocketAddr, sync::Arc, time::Duration};

use quinn::{Connection, Endpoint, RecvStream, SendStream, VarInt};
pub use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use signalweave_core::{Command, CommandResult, ConnectionId, Credentials};
use signalweave_protocol::{
    Authenticated, Capabilities, Codec, ControlPayload, DeliveryClass, Envelope, MessageKind,
    MessagePayload, PROTOCOL_VERSION, ProtocolErrorCode,
};
use signalweave_transport::{
    MAX_FRAME_BYTES, MAX_PAYLOAD_BYTES, WorkerHandle, flush_outbound, handle_authenticated,
    send_envelope, send_error,
};
use tokio::sync::{Semaphore, mpsc};
use tracing::debug;

const WRITE_CAPACITY: usize = 128;
const MAX_CONNECTION_TASKS: usize = 4_096;
const INITIAL_STREAM_TIMEOUT: Duration = Duration::from_secs(10);
const CLOSE_PROTOCOL: u32 = 0x100;
const CLOSE_TRANSPORT: u32 = 0x101;

/// Configuration shared by QUIC connection handlers.
#[derive(Clone)]
pub struct QuicConfig {
    /// Handle to the bounded, single-owner core worker.
    pub worker: WorkerHandle,
    /// Server name reported in the protocol capabilities response.
    pub server_name: Arc<str>,
    /// Server version reported in the protocol capabilities response.
    pub server_version: Arc<str>,
}

impl QuicConfig {
    #[must_use]
    pub fn new(worker: WorkerHandle) -> Self {
        Self {
            worker,
            server_name: Arc::from("signalweave"),
            server_version: Arc::from(env!("CARGO_PKG_VERSION")),
        }
    }
}

/// Build a QUIC server configuration from the certificate chain and private key DER supplied by
/// the embedding server. This crate never creates a self-signed production certificate.
pub fn server_config(
    certificate_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
) -> Result<quinn::ServerConfig, rustls::Error> {
    quinn::ServerConfig::with_single_cert(certificate_chain, private_key)
}

/// Bind a local QUIC endpoint for a previously constructed server configuration.
pub fn server_endpoint(
    bind_address: SocketAddr,
    server_config: quinn::ServerConfig,
) -> Result<Endpoint, std::io::Error> {
    Endpoint::server(server_config, bind_address)
}

/// Accept QUIC handshakes until `endpoint` is closed, serving each successful connection.
///
/// The endpoint loop caps active connection tasks at 4,096. It does not create an application-level
/// queue: when all permits are in use, it pauses acceptance and relies on
/// Quinn's configured endpoint limits for backpressure.
pub async fn serve_endpoint(endpoint: Endpoint, config: QuicConfig) {
    let connection_tasks = Arc::new(Semaphore::new(MAX_CONNECTION_TASKS));
    while let Some(incoming) = endpoint.accept().await {
        let Ok(connection) = incoming.await else {
            continue;
        };
        let Ok(permit) = connection_tasks.clone().acquire_owned().await else {
            return;
        };
        let connection_config = config.clone();
        std::mem::drop(tokio::spawn(async move {
            let _permit = permit;
            serve_connection(connection, connection_config).await;
        }));
    }
}

/// Serve one established QUIC connection.
///
/// The first client-initiated bidirectional stream is the Signalweave control stream. A second
/// client bidirectional stream is a protocol violation and closes the connection.
#[allow(
    clippy::manual_let_else,
    clippy::single_match_else,
    clippy::too_many_lines
)]
pub async fn serve_connection(connection: Connection, config: QuicConfig) {
    let codec = Codec::default();
    let core_connection = match config.worker.execute(Command::TransportConnected).await {
        Ok(CommandResult::Connected(connection)) => connection,
        Ok(_) | Err(_) => {
            close(&connection, CLOSE_TRANSPORT, b"core worker unavailable");
            return;
        }
    };

    let (send_stream, mut receive_stream) =
        match tokio::time::timeout(INITIAL_STREAM_TIMEOUT, connection.accept_bi()).await {
            Ok(Ok(streams)) => streams,
            Ok(Err(_)) | Err(_) => {
                config.worker.discard_and_disconnect(core_connection).await;
                close(
                    &connection,
                    CLOSE_PROTOCOL,
                    b"expected a client bidirectional stream",
                );
                return;
            }
        };

    let (write_sender, mut write_receiver) = mpsc::channel::<Envelope>(WRITE_CAPACITY);
    let (shutdown_sender, mut shutdown_receiver) = mpsc::channel::<()>(1);
    let writer_codec = codec.clone();
    let writer_worker = config.worker.clone();
    let writer_shutdown = shutdown_sender.clone();
    let writer_connection = connection.clone();
    let writer_task = tokio::spawn(async move {
        writer_loop(
            send_stream,
            &mut write_receiver,
            writer_codec,
            writer_worker,
            core_connection,
            writer_shutdown,
            writer_connection,
        )
        .await;
    });

    if config
        .worker
        .register_lifecycle(
            core_connection,
            write_sender.clone(),
            shutdown_sender.clone(),
        )
        .await
        .is_err()
    {
        config.worker.discard_and_disconnect(core_connection).await;
        drop(write_sender);
        writer_task.abort();
        close(&connection, CLOSE_TRANSPORT, b"core worker unavailable");
        return;
    }

    let drain_task = spawn_outbound_drain(
        config.worker.clone(),
        core_connection,
        write_sender.clone(),
        shutdown_sender.clone(),
    );

    let mut greeted = false;
    let mut authenticated = false;
    loop {
        tokio::select! {
            _ = shutdown_receiver.recv() => break,
            extra_stream = connection.accept_bi() => {
                if extra_stream.is_ok() {
                    send_error(
                        &write_sender,
                        MessageKind::Unknown,
                        ProtocolErrorCode::UnsupportedMessage,
                        "only one client bidirectional stream is supported".to_owned(),
                    ).await;
                }
                break;
            }
            received = read_envelope(&mut receive_stream, &codec) => {
                let envelope = match received {
                    Ok(envelope) => envelope,
                    Err(error) => {
                        send_error(
                            &write_sender,
                            MessageKind::Unknown,
                            ProtocolErrorCode::MalformedFrame,
                            error,
                        ).await;
                        break;
                    }
                };
                let result = if !greeted {
                    handle_hello(&write_sender, &config, &envelope).await.map(|()| greeted = true)
                } else if !authenticated {
                    handle_authenticate(&config, core_connection, &write_sender, envelope)
                        .await
                        .map(|()| authenticated = true)
                } else {
                    handle_authenticated(&config.worker, core_connection, envelope, &write_sender).await
                };
                if result.is_err() {
                    break;
                }
            }
        }
    }

    drain_task.abort();
    config.worker.discard_and_disconnect(core_connection).await;
    drop(write_sender);
    drop(shutdown_sender);
    let _ = writer_task.await;
    close(
        &connection,
        CLOSE_PROTOCOL,
        b"Signalweave QUIC connection closed",
    );
    debug!(?core_connection, "QUIC connection closed");
}

fn spawn_outbound_drain(
    worker: WorkerHandle,
    connection: ConnectionId,
    write_sender: mpsc::Sender<Envelope>,
    shutdown_sender: mpsc::Sender<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(10));
        loop {
            interval.tick().await;
            if flush_outbound(&worker, connection, &write_sender)
                .await
                .is_err()
            {
                let _ = shutdown_sender.try_send(());
                worker.discard_and_disconnect(connection).await;
                return;
            }
        }
    })
}

#[allow(clippy::manual_let_else, clippy::single_match_else)]
async fn writer_loop(
    mut stream: SendStream,
    receiver: &mut mpsc::Receiver<Envelope>,
    codec: Codec,
    worker: WorkerHandle,
    core_connection: ConnectionId,
    shutdown: mpsc::Sender<()>,
    connection: Connection,
) {
    while let Some(envelope) = receiver.recv().await {
        let frame = match codec.encode(&envelope) {
            Ok(frame) => frame,
            Err(_) => {
                let _ = shutdown.try_send(());
                worker.discard_and_disconnect(core_connection).await;
                close(
                    &connection,
                    CLOSE_TRANSPORT,
                    b"failed to encode protocol envelope",
                );
                return;
            }
        };
        if stream.write_all(&frame).await.is_err() {
            let _ = shutdown.try_send(());
            worker.discard_and_disconnect(core_connection).await;
            close(&connection, CLOSE_TRANSPORT, b"QUIC stream write failed");
            return;
        }
    }
    let _ = stream.finish();
}

async fn read_envelope(stream: &mut RecvStream, codec: &Codec) -> Result<Envelope, String> {
    let mut prefix = [0_u8; 4];
    stream
        .read_exact(&mut prefix)
        .await
        .map_err(|error| format!("failed to read frame prefix: {error}"))?;
    let frame_len = codec
        .expected_frame_len(&prefix)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "incomplete size prefix".to_owned())?;
    let mut frame = vec![0_u8; frame_len];
    frame[..prefix.len()].copy_from_slice(&prefix);
    stream
        .read_exact(&mut frame[prefix.len()..])
        .await
        .map_err(|error| format!("failed to read frame: {error}"))?;
    codec.decode(&frame).map_err(|error| error.to_string())
}

async fn handle_hello(
    write_sender: &mpsc::Sender<Envelope>,
    config: &QuicConfig,
    envelope: &Envelope,
) -> Result<(), ()> {
    if !matches!(
        envelope.message,
        MessagePayload::Control(ControlPayload::Hello(_))
    ) {
        send_error(
            write_sender,
            envelope.message_kind(),
            ProtocolErrorCode::UnsupportedMessage,
            "expected Hello".to_owned(),
        )
        .await;
        return Err(());
    }
    send_envelope(
        write_sender,
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
}

#[allow(clippy::manual_let_else, clippy::single_match_else)]
async fn handle_authenticate(
    config: &QuicConfig,
    connection: ConnectionId,
    write_sender: &mpsc::Sender<Envelope>,
    envelope: Envelope,
) -> Result<(), ()> {
    let MessagePayload::Control(ControlPayload::Authenticate(auth)) = envelope.message else {
        send_error(
            write_sender,
            envelope.message_kind(),
            ProtocolErrorCode::AuthenticationRequired,
            "expected Authenticate".to_owned(),
        )
        .await;
        return Err(());
    };
    let token = match String::from_utf8(auth.credentials) {
        Ok(token) => token,
        Err(_) => {
            send_error(
                write_sender,
                MessageKind::Authenticate,
                ProtocolErrorCode::AuthenticationRequired,
                "credentials must be UTF-8".to_owned(),
            )
            .await;
            return Err(());
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
            send_envelope(
                write_sender,
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
                write_sender,
                MessageKind::Authenticate,
                ProtocolErrorCode::Unauthorized,
                "authentication failed".to_owned(),
            )
            .await;
            Err(())
        }
    }
}

fn close(connection: &Connection, code: u32, reason: &[u8]) {
    connection.close(VarInt::from_u32(code), reason);
}
