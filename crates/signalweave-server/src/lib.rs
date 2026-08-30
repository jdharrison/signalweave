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
    ChannelScope, CoordinateFrame, CoreConfig, DevAuthenticator, EntityId, NamespaceId,
    PersistenceClass, PrincipalId, RoutingPolicy, SessionId, SessionKey, SignalweaveCore,
    SpaceDescriptor, SpaceEpoch, SpaceId, SpaceKey, TransportIndependentWorker,
};
use signalweave_inference_coordinator::{AiIdentityConfig, CoordinatorConfig};
use signalweave_inference_test_provider::DeterministicProvider;
use signalweave_inference_tools::{ToolRegistry, ToolRegistryError, demo as inference_demo};
use signalweave_transport::UnroutedControl;
use signalweave_transport_quic::{
    PrivateKeyDer, QuicConfig, serve_endpoint as serve_quic_endpoint,
    server_config as quic_server_config, server_endpoint,
};
use signalweave_transport_websocket::{
    WebSocketConfig, WorkerHandle, serve_connection, spawn_worker,
};
use signalweave_transport_webtransport::{
    WebTransportConfig, serve_endpoint as serve_webtransport_endpoint,
    server_endpoint as webtransport_server_endpoint,
};
use tokio::sync::mpsc;
use tower_http::trace::TraceLayer;

/// AI dev principal/token/channel used by the bundled deterministic inference demo.
const AI_DEV_TOKEN: &str = "ai-companion-dev-token";
const AI_PRINCIPAL_ID: u64 = 2;
const AI_STATUS_CHANNEL_ID: u64 = 3;
/// Bounded capacity for client-sent inference control messages awaiting the coordinator.
const INFERENCE_INBOUND_CAPACITY: usize = 64;
/// Bounded concurrent in-flight inference requests.
const INFERENCE_QUEUE_CAPACITY: usize = 16;

/// Server runtime configuration.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub bind_address: SocketAddr,
    pub quic_bind_address: SocketAddr,
    pub webtransport_bind_address: SocketAddr,
    pub websocket_path: String,
    pub webtransport_path: String,
    /// Enables the optional adjacent inference plane (`SIGNALWEAVE_INFERENCE_ENABLED`).
    /// Disabled by default; the relay is fully unaffected either way (ADR 0009).
    pub inference_enabled: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: SocketAddr::from(([127, 0, 0, 1], 8080)),
            quic_bind_address: SocketAddr::from(([127, 0, 0, 1], 8081)),
            webtransport_bind_address: SocketAddr::from(([127, 0, 0, 1], 8082)),
            websocket_path: "/ws".to_owned(),
            webtransport_path: "/webtransport".to_owned(),
            inference_enabled: false,
        }
    }
}

#[derive(Clone)]
struct AppState {
    websocket: WebSocketConfig,
    quic_enabled: bool,
    webtransport_enabled: bool,
    inference_enabled: bool,
}

/// Build the development router and its bounded single-owner core worker. Inference is
/// disabled on this path; relay behavior here is unaffected by the inference plane
/// existing anywhere in the workspace.
pub fn development_router() -> Result<Router, signalweave_core::CoreError> {
    let worker = TransportIndependentWorker::new(development_core()?);
    Ok(router_with_worker(spawn_worker(worker)))
}

/// Build an HTTP router around an already-created core worker.
pub fn router_with_worker(worker: WorkerHandle) -> Router {
    router_with_transports(worker, false, false, None)
}

/// Build the development router with the deterministic inference demo enabled, returning
/// the AI identity's assigned `EntityId` so callers (tests, examples) can address it.
pub async fn development_router_with_inference() -> Result<(Router, EntityId), ServerError> {
    let worker = spawn_worker(TransportIndependentWorker::new(development_core()?));
    let (inference_tx, entity) = spawn_inference_coordinator(worker.clone()).await?;
    Ok((
        router_with_transports(worker, false, false, Some(inference_tx)),
        entity,
    ))
}

fn router_with_transports(
    worker: WorkerHandle,
    quic_enabled: bool,
    webtransport_enabled: bool,
    inference_sink: Option<mpsc::Sender<UnroutedControl>>,
) -> Router {
    let inference_enabled = inference_sink.is_some();
    let mut websocket_config = WebSocketConfig::new(worker);
    websocket_config.inference_sink = inference_sink;
    let state = Arc::new(AppState {
        websocket: websocket_config,
        quic_enabled,
        webtransport_enabled,
        inference_enabled,
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
    let inference_sink = if config.inference_enabled {
        let (inference_tx, _entity) = spawn_inference_coordinator(worker.clone()).await?;
        Some(inference_tx)
    } else {
        None
    };
    let quic = development_quic_endpoint(config.quic_bind_address)?;
    let mut quic_config = QuicConfig::new(worker.clone());
    quic_config.inference_sink = inference_sink.clone();
    tokio::spawn(serve_quic_endpoint(quic, quic_config));
    let webtransport = development_webtransport_endpoint(config.webtransport_bind_address)?;
    let mut webtransport_config = WebTransportConfig::new(worker.clone());
    webtransport_config.path = Arc::from(config.webtransport_path);
    webtransport_config.inference_sink = inference_sink.clone();
    tokio::spawn(serve_webtransport_endpoint(
        webtransport,
        webtransport_config,
    ));
    let listener = tokio::net::TcpListener::bind(config.bind_address).await?;
    axum::serve(
        listener,
        router_with_transports(worker, true, true, inference_sink),
    )
    .await?;
    Ok(())
}

/// Registers the demo tool set, spawns the AI identity's core connection, and starts the
/// coordinator's background tasks. The deterministic fake provider is the only provider
/// wired up in this milestone (a real HTTP provider is explicitly deferred).
async fn spawn_inference_coordinator(
    worker: WorkerHandle,
) -> Result<(mpsc::Sender<UnroutedControl>, EntityId), ServerError> {
    let mut tools = ToolRegistry::new();
    tools
        .register(Arc::new(inference_demo::DiagnosticTool))
        .map_err(|error| inference_registry_error(&error))?;
    tools
        .register(Arc::new(inference_demo::StatusUpdateTool::new(
            ChannelId::new(AI_STATUS_CHANNEL_ID),
        )))
        .map_err(|error| inference_registry_error(&error))?;

    let (inference_tx, inference_rx) = mpsc::channel(INFERENCE_INBOUND_CAPACITY);
    let coordinator_config = CoordinatorConfig {
        worker,
        identity: ai_identity_config(),
        provider: Arc::new(DeterministicProvider),
        tools: Arc::new(tools),
        queue_capacity: INFERENCE_QUEUE_CAPACITY,
    };
    let (_connection, entity) =
        signalweave_inference_coordinator::spawn(coordinator_config, inference_rx)
            .await
            .map_err(|error| ServerError::Inference(error.to_string()))?;
    Ok((inference_tx, entity))
}

fn inference_registry_error(error: &ToolRegistryError) -> ServerError {
    ServerError::Inference(format!("{error:?}"))
}

fn ai_identity_config() -> AiIdentityConfig {
    AiIdentityConfig {
        token: AI_DEV_TOKEN.to_owned(),
        namespace: NamespaceId::new(1),
        session: SessionId::new(1),
        space: SpaceId::new(1),
        space_epoch: SpaceEpoch::new(1),
        status_channel: ChannelId::new(AI_STATUS_CHANNEL_ID),
    }
}

/// Errors returned while starting the server.
#[derive(Debug)]
pub enum ServerError {
    Io(std::io::Error),
    Core(signalweave_core::CoreError),
    UnsupportedWebSocketPath,
    QuicConfiguration(String),
    Inference(String),
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
    features: Vec<&'static str>,
    max_frame_bytes: u32,
    max_payload_bytes: u32,
}
async fn capabilities(State(state): State<Arc<AppState>>) -> Json<CapabilitiesResponse> {
    let mut transports = vec!["websocket"];
    if state.quic_enabled {
        transports.push("quic");
    }
    if state.webtransport_enabled {
        transports.push("webtransport");
    }
    let mut features = Vec::new();
    if state.inference_enabled {
        features.push("inference");
    }
    Json(CapabilitiesResponse {
        protocol_version: signalweave_protocol::PROTOCOL_VERSION,
        transports,
        features,
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

fn development_webtransport_endpoint(
    bind_address: SocketAddr,
) -> Result<signalweave_transport_webtransport::ServerEndpoint, ServerError> {
    let identity = wtransport::Identity::self_signed(["localhost", "127.0.0.1"])
        .map_err(|error| ServerError::QuicConfiguration(error.to_string()))?;
    webtransport_server_endpoint(bind_address, identity).map_err(ServerError::Io)
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
    let mut ai_grants = AuthorizationGrants::new();
    ai_grants.grant_namespace(namespace, AccessGrant::ReadWrite);
    ai_grants.grant_session(session, AccessGrant::ReadWrite);
    ai_grants.grant_space(
        SpaceKey {
            session,
            space: SpaceId::new(1),
        },
        AccessGrant::ReadWrite,
    );
    ai_grants.grant_channel(
        ChannelScope::new(session, ChannelId::new(AI_STATUS_CHANNEL_ID)),
        AccessGrant::ReadWrite,
    );

    let mut authenticator = DevAuthenticator::new();
    let _ = authenticator.insert(
        "dev-token",
        AuthenticatedPrincipal::new(PrincipalId::new(1), grants),
    );
    let _ = authenticator.insert(
        AI_DEV_TOKEN,
        AuthenticatedPrincipal::new(PrincipalId::new(AI_PRINCIPAL_ID), ai_grants),
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
    core.register_channel(ChannelDefinition::relay_owned(
        ChannelId::new(AI_STATUS_CHANNEL_ID),
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
