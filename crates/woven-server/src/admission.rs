//! HTTP adapter for the transport-neutral admission and queue protocol.
//!
//! These routes are development-facing: authentication is left to the control-plane layer
//! that provisions virtual servers. They demonstrate how Weaver clients can join, poll,
//! claim offers, and cancel over HTTP without coupling the core admission logic to any
//! transport.

use std::sync::Arc;
use std::time::Instant;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use woven_core::{
    AdmissionController, AdmissionMetadata, AdmissionSnapshot, CapacityUpdate, IdempotencyKey,
    JoinDecision, JoinRequest, NamespaceId, NodeId, PrincipalId, QueuePolicy, QueueStatus,
    QueueTicketId, RejectionReason, SessionId, SessionKey, UsageCounters,
};

pub type AdmissionState = Arc<Mutex<AdmissionController>>;

#[must_use]
pub fn development_state() -> AdmissionState {
    let server_id = SessionKey::new(NamespaceId::new(1), SessionId::new(1));
    let metadata = AdmissionMetadata {
        node_id: NodeId::new(1),
        session: server_id,
    };
    Arc::new(Mutex::new(AdmissionController::new(
        metadata,
        QueuePolicy::default(),
        CapacityUpdate {
            allocated_ccu: 0,
            revision: 1,
        },
        Arc::new(UsageCounters::new(metadata)),
    )))
}

#[derive(Clone, Debug, Deserialize)]
pub struct JoinRequestBody {
    pub principal_id: u64,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct JoinResponse {
    pub decision: JoinDecision,
}

#[derive(Clone, Debug, Serialize)]
pub struct QueueStatusResponse {
    pub ticket: QueueTicketId,
    pub state: QueueStatus,
    pub poll_after_ms: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct StructuredRejection {
    pub reason: RejectionReason,
    pub message: &'static str,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ServerPath {
    #[allow(dead_code)]
    server_id: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TicketPath {
    ticket: u64,
}

async fn join(
    State(controller): State<AdmissionState>,
    Path(_path): Path<ServerPath>,
    Json(body): Json<JoinRequestBody>,
) -> impl IntoResponse {
    let Some(idempotency_key) = IdempotencyKey::new(body.idempotency_key) else {
        return bad_request("idempotency key exceeds maximum length");
    };
    if body.principal_id == 0 {
        return bad_request("principal_id must be non-zero");
    }
    let decision = controller.lock().await.request_join_at(
        JoinRequest::new(PrincipalId::new(body.principal_id), idempotency_key),
        Instant::now(),
    );
    Json(JoinResponse { decision }).into_response()
}

async fn queue_status(
    State(controller): State<AdmissionState>,
    Path(path): Path<TicketPath>,
) -> impl IntoResponse {
    if path.ticket == 0 {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let ticket = QueueTicketId::new(path.ticket);
    let state = controller
        .lock()
        .await
        .queue_status_at(ticket, Instant::now());
    Json(QueueStatusResponse {
        ticket,
        state,
        poll_after_ms: 3_000,
    })
    .into_response()
}

async fn heartbeat(
    State(controller): State<AdmissionState>,
    Path(path): Path<TicketPath>,
) -> impl IntoResponse {
    if path.ticket == 0 {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let ticket = QueueTicketId::new(path.ticket);
    let state = controller.lock().await.heartbeat_at(ticket, Instant::now());
    Json(QueueStatusResponse {
        ticket,
        state,
        poll_after_ms: 3_000,
    })
    .into_response()
}

async fn claim(
    State(controller): State<AdmissionState>,
    Path(path): Path<TicketPath>,
) -> impl IntoResponse {
    if path.ticket == 0 {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let ticket = QueueTicketId::new(path.ticket);
    let result = controller
        .lock()
        .await
        .claim_offer_at(ticket, Instant::now());
    match result {
        Ok(lease) => Json(lease).into_response(),
        Err(_) => StatusCode::CONFLICT.into_response(),
    }
}

async fn cancel(
    State(controller): State<AdmissionState>,
    Path(path): Path<TicketPath>,
) -> impl IntoResponse {
    if path.ticket == 0 {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let ticket = QueueTicketId::new(path.ticket);
    controller.lock().await.cancel_at(ticket, Instant::now());
    StatusCode::NO_CONTENT.into_response()
}

async fn snapshot(
    State(controller): State<AdmissionState>,
    Path(_path): Path<ServerPath>,
) -> impl IntoResponse {
    let snapshot = controller.lock().await.snapshot();
    Json::<AdmissionSnapshot>(snapshot).into_response()
}

fn bad_request(message: &'static str) -> axum::response::Response {
    (
        StatusCode::BAD_REQUEST,
        Json(StructuredRejection {
            reason: RejectionReason::InvalidIdempotencyKey,
            message,
        }),
    )
        .into_response()
}

/// Build admission and queue routes. In production these must sit behind authentication
/// and capacity-control authorization; the dev server exposes them unauthenticated.
pub fn routes() -> axum::Router<AdmissionState> {
    axum::Router::new()
        .route("/v1/virtual-servers/{server_id}/join", post(join))
        .route("/v1/queues/{ticket}", get(queue_status))
        .route("/v1/queues/{ticket}/heartbeat", post(heartbeat))
        .route("/v1/queues/{ticket}/claim", post(claim))
        .route("/v1/queues/{ticket}", delete(cancel))
        .route("/v1/virtual-servers/{server_id}/snapshot", get(snapshot))
}
