//! Milestone 5 exit-criteria coverage: an AI conversation round-trip, a read-only tool
//! call, and rejection of a deliberately stale state-changing tool proposal.
//!
//! Disabling the plane leaving relay tests unchanged is proven by `tests/websocket.rs`
//! continuing to pass unmodified: it exercises `development_router()`, which never enables
//! inference (`ServerConfig::inference_enabled` defaults to `false`).

use std::time::Duration;

use signalweave_client_rust::{Client, ClientConfig};
use signalweave_inference_test_provider::{TRIGGER_DIAGNOSTIC, TRIGGER_STALE_STATUS_UPDATE};
use signalweave_protocol::{ControlPayload, MessagePayload, ToolCallRejectionCode};
use signalweave_server::development_router_with_inference;

async fn start_server_with_inference() -> (String, u64) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (app, ai_entity) = development_router_with_inference().await.unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("ws://{address}/ws"), ai_entity.get())
}

async fn connect_and_subscribe(url: String) -> Client {
    let mut client = Client::connect(ClientConfig {
        url,
        token: "dev-token".to_owned(),
        ..ClientConfig::default()
    })
    .await
    .unwrap();
    client.join_session(1, 1).await.unwrap();
    client.subscribe_space(1, 1, 1, 1, 1).await.unwrap();
    // SubscriptionAccepted then this client's own EntityEntered.
    client.recv().await.unwrap();
    client.recv().await.unwrap();
    client
}

#[tokio::test]
async fn ai_conversation_round_trip_completes() {
    let (url, ai_entity) = start_server_with_inference().await;
    let mut client = connect_and_subscribe(url).await;

    client
        .request_inference(
            1,
            1,
            1,
            1,
            ai_entity,
            "language.dialogue",
            0,
            b"hello".to_vec(),
        )
        .await
        .unwrap();

    let accepted = tokio::time::timeout(Duration::from_secs(1), client.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        accepted.message,
        MessagePayload::Control(ControlPayload::InferenceAccepted(_))
    ));

    let completed = tokio::time::timeout(Duration::from_secs(1), client.recv())
        .await
        .unwrap()
        .unwrap();
    let MessagePayload::Control(ControlPayload::InferenceCompleted(payload)) = completed.message
    else {
        panic!("expected InferenceCompleted, got {:?}", completed.message);
    };
    assert_eq!(payload.result, b"heard: hello");
}

#[tokio::test]
async fn read_only_diagnostic_tool_call_completes_without_rejection() {
    let (url, ai_entity) = start_server_with_inference().await;
    let mut client = connect_and_subscribe(url).await;

    client
        .request_inference(
            1,
            1,
            1,
            1,
            ai_entity,
            "language.dialogue",
            0,
            TRIGGER_DIAGNOSTIC.as_bytes().to_vec(),
        )
        .await
        .unwrap();

    // InferenceAccepted, InferenceStreamChunk
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(1), client.recv())
            .await
            .unwrap()
            .unwrap();
    }

    let proposed = tokio::time::timeout(Duration::from_secs(1), client.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        proposed.message,
        MessagePayload::Control(ControlPayload::ToolCallProposed(_))
    ));

    let accepted = tokio::time::timeout(Duration::from_secs(1), client.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        accepted.message,
        MessagePayload::Control(ControlPayload::ToolCallAccepted(_))
    ));

    let completed = tokio::time::timeout(Duration::from_secs(1), client.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        completed.message,
        MessagePayload::Control(ControlPayload::ToolCallCompleted(_))
    ));

    let inference_completed = tokio::time::timeout(Duration::from_secs(1), client.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        inference_completed.message,
        MessagePayload::Control(ControlPayload::InferenceCompleted(_))
    ));
}

#[tokio::test]
async fn stale_state_changing_proposal_is_rejected() {
    let (url, ai_entity) = start_server_with_inference().await;
    let mut client = connect_and_subscribe(url).await;

    client
        .request_inference(
            1,
            1,
            1,
            1,
            ai_entity,
            "language.dialogue",
            0,
            TRIGGER_STALE_STATUS_UPDATE.as_bytes().to_vec(),
        )
        .await
        .unwrap();

    let accepted = tokio::time::timeout(Duration::from_secs(1), client.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        accepted.message,
        MessagePayload::Control(ControlPayload::InferenceAccepted(_))
    ));

    let proposed = tokio::time::timeout(Duration::from_secs(1), client.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        proposed.message,
        MessagePayload::Control(ControlPayload::ToolCallProposed(_))
    ));

    let rejected = tokio::time::timeout(Duration::from_secs(1), client.recv())
        .await
        .unwrap()
        .unwrap();
    let MessagePayload::Control(ControlPayload::ToolCallRejected(payload)) = rejected.message
    else {
        panic!("expected ToolCallRejected, got {:?}", rejected.message);
    };
    assert_eq!(payload.code, ToolCallRejectionCode::Stale);

    // The rejected proposal must not have produced a ToolCallCompleted before the
    // outer InferenceCompleted.
    let inference_completed = tokio::time::timeout(Duration::from_secs(1), client.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        inference_completed.message,
        MessagePayload::Control(ControlPayload::InferenceCompleted(_))
    ));
}
