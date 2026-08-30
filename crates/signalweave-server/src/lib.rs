//! HTTP control plane and default development server composition.

#![deny(unsafe_code)]

use std::{net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    extract::{State, ws::WebSocketUpgrade},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
};
use serde::Serialize;
use signalweave_core::{
    AccessGrant, AuthenticatedPrincipal, AuthorizationGrants, ChannelDefinition, ChannelId,
    ChannelScope, CoordinateFrame, CoreConfig, DevAuthenticator, NamespaceId, PersistenceClass,
    PrincipalId, RoutingPolicy, SessionId, SessionKey, SignalweaveCore, SpaceDescriptor,
    SpaceEpoch, SpaceId, SpaceKey, TransportIndependentWorker,
};
use signalweave_transport_quic::{
    PrivateKeyDer, QuicConfig, serve_endpoint as serve_quic_endpoint,
    server_config as quic_server_config, server_endpoint,
};
use signalweave_transport_websocket::{
    WebSocketConfig, WorkerHandle, serve_connection, spawn_worker,
};
use tower_http::trace::TraceLayer;

/// Server runtime configuration.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub bind_address: SocketAddr,
    pub quic_bind_address: SocketAddr,
    pub websocket_path: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: SocketAddr::from(([127, 0, 0, 1], 8080)),
            quic_bind_address: SocketAddr::from(([127, 0, 0, 1], 8081)),
            websocket_path: "/ws".to_owned(),
        }
    }
}

#[derive(Clone)]
struct AppState {
    websocket: WebSocketConfig,
    quic_enabled: bool,
}

/// Build the development router and its bounded single-owner core worker.
pub fn development_router() -> Result<Router, signalweave_core::CoreError> {
    let worker = TransportIndependentWorker::new(development_core()?);
    Ok(router_with_worker(spawn_worker(worker)))
}

/// Build an HTTP router around an already-created core worker.
pub fn router_with_worker(worker: WorkerHandle) -> Router {
    router_with_transports(worker, false)
}

fn router_with_transports(worker: WorkerHandle, quic_enabled: bool) -> Router {
    let state = Arc::new(AppState {
        websocket: WebSocketConfig::new(worker),
        quic_enabled,
    });
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics))
        .route("/v1/capabilities", get(capabilities))
        .route("/ws", get(websocket))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Serve the development composition on `config.bind_address`.
pub async fn serve(config: ServerConfig) -> Result<(), ServerError> {
    if config.websocket_path != "/ws" {
        return Err(ServerError::UnsupportedWebSocketPath);
    }
    let worker = spawn_worker(TransportIndependentWorker::new(development_core()?));
    let quic = development_quic_endpoint(config.quic_bind_address)?;
    tokio::spawn(serve_quic_endpoint(quic, QuicConfig::new(worker.clone())));
    let listener = tokio::net::TcpListener::bind(config.bind_address).await?;
    axum::serve(listener, router_with_transports(worker, true)).await?;
    Ok(())
}

/// Errors returned while starting the server.
#[derive(Debug)]
pub enum ServerError {
    Io(std::io::Error),
    Core(signalweave_core::CoreError),
    UnsupportedWebSocketPath,
    QuicConfiguration(String),
}
impl From<std::io::Error> for ServerError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}
impl From<signalweave_core::CoreError> for ServerError {
    fn from(error: signalweave_core::CoreError) -> Self {
        Self::Core(error)
    }
}
impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ServerError {}

async fn health() -> StatusCode {
    StatusCode::OK
}
async fn ready() -> StatusCode {
    StatusCode::OK
}
async fn metrics() -> &'static str {
    "# Signalweave metrics placeholder\n"
}

#[derive(Serialize)]
struct CapabilitiesResponse {
    protocol_version: u16,
    transports: Vec<&'static str>,
    max_frame_bytes: u32,
    max_payload_bytes: u32,
}
async fn capabilities(State(state): State<Arc<AppState>>) -> Json<CapabilitiesResponse> {
    let mut transports = vec!["websocket"];
    if state.quic_enabled {
        transports.push("quic");
    }
    Json(CapabilitiesResponse {
        protocol_version: signalweave_protocol::PROTOCOL_VERSION,
        transports,
        max_frame_bytes: 1_048_576,
        max_payload_bytes: 262_144,
    })
}
async fn websocket(
    State(state): State<Arc<AppState>>,
    upgrade: WebSocketUpgrade,
) -> impl IntoResponse {
    let websocket = state.websocket.clone();
    upgrade.on_upgrade(move |socket| serve_connection(socket, websocket))
}

fn development_quic_endpoint(bind_address: SocketAddr) -> Result<quinn::Endpoint, ServerError> {
    let certificate =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
            .map_err(|error| ServerError::QuicConfiguration(error.to_string()))?;
    let config = quic_server_config(
        vec![certificate.cert.der().clone()],
        PrivateKeyDer::Pkcs8(certificate.key_pair.serialize_der().into()),
    )
    .map_err(|error| ServerError::QuicConfiguration(error.to_string()))?;
    server_endpoint(bind_address, config).map_err(ServerError::Io)
}

fn development_core() -> Result<SignalweaveCore<DevAuthenticator>, signalweave_core::CoreError> {
    let namespace = NamespaceId::new(1);
    let session = SessionKey {
        namespace,
        session: SessionId::new(1),
    };
    let mut grants = AuthorizationGrants::new();
    grants.grant_namespace(namespace, AccessGrant::ReadWrite);
    grants.grant_session(session, AccessGrant::ReadWrite);
    for space_id in [1, 2] {
        grants.grant_space(
            SpaceKey {
                session,
                space: SpaceId::new(space_id),
            },
            AccessGrant::ReadWrite,
        );
    }
    for channel_id in [1, 2] {
        grants.grant_channel(
            ChannelScope::new(session, ChannelId::new(channel_id)),
            AccessGrant::ReadWrite,
        );
    }
    let mut authenticator = DevAuthenticator::new();
    let _ = authenticator.insert(
        "dev-token",
        AuthenticatedPrincipal::new(PrincipalId::new(1), grants),
    );
    let mut core = SignalweaveCore::new(authenticator, CoreConfig::default())?;
    core.register_channel(ChannelDefinition::relay_owned(
        ChannelId::new(1),
        signalweave_core::DeliveryClass::ReliableOrdered,
        PersistenceClass::Ephemeral,
        64 * 1024,
    ))?;
    core.register_channel(ChannelDefinition::relay_owned(
        ChannelId::new(2),
        signalweave_core::DeliveryClass::LatestValue,
        PersistenceClass::Stateful,
        64 * 1024,
    ))?;
    core.provision_session(session)?;
    for space_id in [1, 2] {
        core.install_space(
            session,
            SpaceDescriptor {
                id: SpaceId::new(space_id),
                local_frame: CoordinateFrame::Logical,
                parent: None,
                epoch: SpaceEpoch::new(1),
                routing: RoutingPolicy::BroadcastAll,
            },
        )?;
    }
    Ok(core)
}
