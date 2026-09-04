use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::json;
use tower::ServiceExt;
use woven_core::{
    AdmissionController, AdmissionMetadata, CapacityUpdate, NamespaceId, NodeId, QueuePolicy,
    SessionId, SessionKey, UsageCounters,
};
use woven_server::admission::{self, AdmissionState};

fn state_with_capacity(capacity: u32) -> AdmissionState {
    let server_id = SessionKey::new(NamespaceId::new(1), SessionId::new(1));
    let metadata = AdmissionMetadata {
        node_id: NodeId::new(1),
        session: server_id,
    };
    std::sync::Arc::new(tokio::sync::Mutex::new(AdmissionController::new(
        metadata,
        QueuePolicy::default(),
        CapacityUpdate {
            allocated_ccu: capacity,
            revision: 1,
        },
        std::sync::Arc::new(UsageCounters::new(metadata)),
    )))
}

#[tokio::test]
async fn http_join_admits_and_queues() {
    let app = admission::routes().with_state(state_with_capacity(2));
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/virtual-servers/1/join")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"principal_id": 1, "idempotency_key": "a"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let _ = app.clone().oneshot(join_request(2, "b")).await.unwrap();
    let queued = app.clone().oneshot(join_request(3, "c")).await.unwrap();
    assert_eq!(queued.status(), StatusCode::OK);
    let body = axum::body::to_bytes(queued.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["decision"]["Queued"]["principal"], 3);
}

#[tokio::test]
async fn http_queue_status_and_heartbeat_return_state() {
    let state = state_with_capacity(1);
    let app = admission::routes().with_state(state.clone());

    let admitted = app.clone().oneshot(join_request(1, "a")).await.unwrap();
    assert_eq!(admitted.status(), StatusCode::OK);

    let queued = app.clone().oneshot(join_request(2, "b")).await.unwrap();
    let body = axum::body::to_bytes(queued.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let ticket = json["decision"]["Queued"]["id"].as_u64().unwrap();

    let status = app
        .clone()
        .oneshot(queue_request("GET", ticket, ""))
        .await
        .unwrap();
    assert_eq!(status.status(), StatusCode::OK);

    let heartbeat = app
        .clone()
        .oneshot(queue_request("POST", ticket, "heartbeat"))
        .await
        .unwrap();
    assert_eq!(heartbeat.status(), StatusCode::OK);
}

fn join_request(principal: u64, key: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/virtual-servers/1/join")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({"principal_id": principal, "idempotency_key": key}).to_string(),
        ))
        .unwrap()
}

fn queue_request(method: &str, ticket: u64, suffix: &str) -> Request<Body> {
    let uri = if suffix.is_empty() {
        format!("/v1/queues/{ticket}")
    } else {
        format!("/v1/queues/{ticket}/{suffix}")
    };
    Request::builder()
        .method(method)
        .uri(&uri)
        .body(Body::empty())
        .unwrap()
}

#[tokio::test]
async fn http_rejects_zero_principal_id() {
    let app = admission::routes().with_state(state_with_capacity(2));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/virtual-servers/1/join")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"principal_id": 0, "idempotency_key": "a"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn http_rejects_zero_ticket_id() {
    let app = admission::routes().with_state(state_with_capacity(2));
    let response = app
        .clone()
        .oneshot(queue_request("GET", 0, ""))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
