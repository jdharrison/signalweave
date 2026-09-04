//! HTTP control plane and default development server composition.

#![deny(unsafe_code)]

pub mod admission;

use std::{net::SocketAddr, sync::Arc};

use axum::{Json, Router, extract::State, http::StatusCode, routing::get};
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
use signalweave_transport::{UnroutedControl, WorkerHandle, spawn_worker};
use signalweave_transport_quic::webtransport::{
    WebTransportConfig, serve_endpoint as serve_webtransport_endpoint,
    server_endpoint as webtransport_server_endpoint,
};
use signalweave_transport_quic::{
    PrivateKeyDer, QuicConfig, serve_endpoint as serve_quic_endpoint,
    server_config as quic_server_config, server_endpoint,
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
            webtransport_path: "/webtransport".to_owned(),
            inference_enabled: false,
        }
    }
}

#[derive(Clone)]
struct AppState {
    quic_enabled: bool,
    webtransport_enabled: bool,
    webtransport_endpoint: Option<String>,
    inference_enabled: bool,
}

/// Build the HTTP/health/capabilities router with the given transport capabilities reported.
///
/// `webtransport_endpoint` is the relative `port/path` of the WebTransport endpoint as
/// advertised in `/v1/capabilities`; clients already connected to this host resolve it
/// against the host and scheme they used to reach the control plane. `None` when
/// WebTransport is disabled.
#[allow(clippy::too_many_arguments)]
fn router_with_transports(
    quic_enabled: bool,
    webtransport_enabled: bool,
    webtransport_endpoint: Option<String>,
    inference_enabled: bool,
) -> Router {
    let state = Arc::new(AppState {
        quic_enabled,
        webtransport_enabled,
        webtransport_endpoint,
        inference_enabled,
    });
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics))
        .route("/v1/capabilities", get(capabilities))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Serve the development composition on `config.bind_address`.
pub async fn serve(config: ServerConfig) -> Result<(), ServerError> {
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
    let webtransport_port = webtransport.local_addr()?.port();
    let webtransport_path = config.webtransport_path.clone();
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
        router_with_transports(
            true,
            true,
            Some(format!("{webtransport_port}{webtransport_path}")),
            inference_sink.is_some(),
        ),
    )
    .await?;
    Ok(())
}

/// URLs for a development server started on ephemeral ports.
#[derive(Clone, Debug)]
pub struct DevServeUrls {
    /// HTTP control-plane base, e.g. `http://127.0.0.1:PORT`.
    pub http: String,
    /// Native QUIC connection URL, e.g. `quic://127.0.0.1:PORT`.
    pub quic: String,
    /// WebTransport connection URL, e.g. `wtransport://127.0.0.1:PORT/webtransport`.
    pub webtransport: String,
    /// The AI identity's `EntityId` when inference is enabled, otherwise `None`.
    pub ai_entity: Option<EntityId>,
}

/// Start the full development composition (HTTP + QUIC + WebTransport) on ephemeral ports and
/// return the connection URLs. Intended for integration tests and local tooling.
///
/// When `inference_enabled` is true, the deterministic AI demo is started and its `EntityId`
/// is returned via [`DevServeUrls::ai_entity`].
pub async fn serve_dev_ephemeral(inference_enabled: bool) -> Result<DevServeUrls, ServerError> {
    let worker = spawn_worker(TransportIndependentWorker::new(development_core()?));
    let (inference_sink, ai_entity) = if inference_enabled {
        let (tx, entity) = spawn_inference_coordinator(worker.clone()).await?;
        (Some(tx), Some(entity))
    } else {
        (None, None)
    };

    let quic = development_quic_endpoint(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let quic_address = quic.local_addr()?;
    let mut quic_config = QuicConfig::new(worker.clone());
    quic_config.inference_sink = inference_sink.clone();
    tokio::spawn(serve_quic_endpoint(quic, quic_config));

    let webtransport = development_webtransport_endpoint(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let webtransport_address = webtransport.local_addr()?;
    let mut webtransport_config = WebTransportConfig::new(worker);
    webtransport_config.path = Arc::from("/webtransport");
    webtransport_config.inference_sink = inference_sink.clone();
    tokio::spawn(serve_webtransport_endpoint(
        webtransport,
        webtransport_config,
    ));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let http_address = listener.local_addr()?;
    let webtransport_port = webtransport_address.port();
    let http_handle = tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            router_with_transports(
                true,
                true,
                Some(format!("{webtransport_port}/webtransport")),
                inference_sink.is_some(),
            ),
        )
        .await;
    });
    std::mem::drop(http_handle);

    Ok(DevServeUrls {
        http: format!("http://{http_address}"),
        quic: format!("quic://{quic_address}"),
        webtransport: format!("wtransport://127.0.0.1:{webtransport_port}/webtransport"),
        ai_entity,
    })
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
    /// Relative `port/path` of the WebTransport endpoint, resolved against the host
    /// the client already used to reach this control plane. Present only when
    /// WebTransport is enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    webtransport: Option<String>,
}
async fn capabilities(State(state): State<Arc<AppState>>) -> Json<CapabilitiesResponse> {
    let mut transports = Vec::new();
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
        webtransport: state.webtransport_endpoint.clone(),
    })
}

fn development_webtransport_endpoint(
    bind_address: SocketAddr,
) -> Result<signalweave_transport_quic::webtransport::ServerEndpoint, ServerError> {
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
