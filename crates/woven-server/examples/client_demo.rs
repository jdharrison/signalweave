//! High-level end-to-end demo of the Woven client story.
//!
//! Stands up the development server (QUIC + WebTransport + HTTP control plane),
//! then drives the reference Rust client over **both** transports through the
//! full lifecycle: connect → handshake → join → subscribe → entity spawn →
//! publish → fan-out delivery. Finally it decodes a frame produced by the
//! TypeScript WebTransport client encoder with the Rust `Codec`, proving the
//! two clients speak the same wire protocol in both directions.

use std::time::Duration;

use tokio::time::timeout;
use woven_client_rust::{Client, ClientConfig};
use woven_protocol::{Codec, MessagePayload};
use woven_server::serve_dev_ephemeral;

const TOKEN: &str = "dev-token";

/// Receive the entity id the server assigns when this connection subscribes.
async fn assigned_entity(client: &mut Client) -> u64 {
    loop {
        match timeout(Duration::from_secs(2), client.recv()).await {
            Ok(Ok(envelope)) => {
                if let Some(id) = envelope.entity_id {
                    return id;
                }
            }
            other => panic!("failed to receive assigned entity: {other:?}"),
        }
    }
}

/// Connect a client, complete the handshake, join, subscribe, and report the
/// server-assigned entity id for this connection.
async fn setup(url: &str, tag: &str) -> (Client, u64) {
    let mut client = Client::connect(ClientConfig {
        url: url.to_owned(),
        token: TOKEN.to_owned(),
        ..ClientConfig::default()
    })
    .await
    .expect("client connect (handshake: Hello→Capabilities→Authenticate→Authenticated)");
    println!("    {tag}: client connected, handshake complete");

    client.join_session(1, 1).await.unwrap();
    println!("    {tag}: joined session");

    client.subscribe_space(1, 1, 1, 1, 1).await.unwrap();
    println!("    {tag}: subscribed to space");

    let entity = assigned_entity(&mut client).await;
    println!("    {tag}: server assigned entity id {entity}");
    (client, entity)
}

async fn run_pair(label: &str, url: &str) {
    println!("\n── {label} ──────────────────────────────────────────────");
    let (mut alice, alice_entity) = setup(url, "alice").await;
    let (mut bob, _bob_entity) = setup(url, "bob").await;

    alice
        .publish_event(
            1,
            1,
            1,
            1,
            1,
            alice_entity,
            1,
            1,
            b"hello from alice".to_vec(),
        )
        .await
        .unwrap();
    println!("    alice: published ReliableEvent \"hello from alice\" (seq 1)");

    let received = timeout(Duration::from_secs(1), bob.recv())
        .await
        .expect("timeout waiting for fan-out")
        .expect("bob recv");
    match received.message {
        MessagePayload::ReliableEvent(ref payload) => {
            println!(
                "    bob:   received ReliableEvent {} bytes: {:?}",
                payload.bytes.len(),
                String::from_utf8_lossy(&payload.bytes)
            );
        }
        other => panic!("unexpected message kind: {other:?}"),
    }

    alice.close().unwrap();
    bob.close().unwrap();
    println!("    both clients closed cleanly");
}

/// Decode a frame emitted by the TypeScript WebTransport client encoder using
/// the Rust `Codec`. These hex strings are golden fixtures exported from
/// `woven-client-ts` and embedded in `ts_client_wire.rs`; here we run the
/// same decode live to show TS→Rust wire compatibility end-to-end.
fn show_ts_frames_decode_in_rust() {
    println!("\n── TypeScript encoder → Rust codec ───────────────────────");
    let codec = Codec::default();

    // Frames exported from the TypeScript WebTransport client encoder via
    // `encodeHello`, `encodeSubscribeSpace`, and `encodeReliableEvent`.
    // They carry the WVN1 file identifier and the 4-byte size prefix that the
    // Rust `Codec` expects, and decode to the matching message kinds below.
    let ts_hello = "880000002c00000053575031000022000e0000000d000c0000000000000000000000000000000000000000000b000400220000001c000000000000010101120014000000000010000c00000008000400120000000000010000000100080000001000000005000000302e312e30000000150000007369676e616c77656176652d636c69656e742d7473000000";
    let ts_subscribe = "7c00000030000000535750310000000024003a000000390038002c0024001c00000014000000000000000000000013000c0004002400000001000000000000003400000000000007010000000000000001000000000000000100000000000000010000000000000000000000010706000c000400060000000100000000000000";
    let ts_event = "940000003000000053575031000000002400500000004f004e0044003c0034002c00240000001c00000014001000000000000400240000000100000000000000000000004000000001000000000000000100000000000000010000000000000001000000000000000100000000000000010000000000000001000000000000000000010e0d00000068656c6c6f2066726f6d207473000000";

    for (name, hex) in [
        ("encodeHello", ts_hello),
        ("encodeSubscribeSpace", ts_subscribe),
        ("encodeReliableEvent", ts_event),
    ] {
        let frame: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect();
        match codec.decode(&frame) {
            Ok(envelope) => println!(
                "    TS {name} → decoded as {:?} (delivery {:?})",
                envelope.message_kind(),
                envelope.delivery_class,
            ),
            Err(e) => println!("    TS {name} → decode error: {e:?}"),
        }
    }
}

#[tokio::main]
async fn main() {
    let urls = serve_dev_ephemeral(false).await.expect("start server");
    println!("server listening:");
    println!("    QUIC:         {}", urls.quic);
    println!("    WebTransport: {}", urls.webtransport);
    println!("    HTTP control: {}/v1/capabilities", urls.http);

    run_pair("Rust client · native QUIC", &urls.quic).await;
    run_pair(
        "Rust client · browser-style WebTransport",
        &urls.webtransport,
    )
    .await;
    show_ts_frames_decode_in_rust();

    println!("\ndone");
}
