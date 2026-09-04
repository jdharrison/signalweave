//! Cross-language proof that the TypeScript client's encoder is wire-compatible
//! with the Rust `Codec`. Each fixture below is the exact byte sequence produced by
//! `signalweave-client-ts/src/encode.ts` for the corresponding outbound message.
//!
//! The Rust `Codec` must verify and decode every one of them into the expected
//! semantic envelope. This is the server-side counterpart to the TS-side tests
//! that decode Rust-produced golden fixtures (`tests/fixtures/*.swp`).

use signalweave_protocol::{Codec, CodecError, Hello, MessagePayload, SubscribeSpace};

fn hex_decode(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex nibble"))
        .collect()
}

fn decode_hex(s: &str) -> Result<signalweave_protocol::Envelope, CodecError> {
    Codec::default().decode(&hex_decode(s))
}

const TS_HELLO: &str = "880000002c00000053575031000022000e0000000d000c0000000000000000000000000000000000000000000b000400220000001c000000000000010101120014000000000010000c00000008000400120000000000010000000100080000001000000005000000302e312e30000000150000007369676e616c77656176652d636c69656e742d7473000000";
const TS_AUTHENTICATE: &str = "5c0000002c00000053575031000022000c0000000b000a0000000000000000000000000000000000000000000900040022000000100000000003010308000c000b000400080000000800000000000002090000006465762d746f6b656e000000";
const TS_JOIN_SESSION: &str = "6400000030000000535750310000000000002200220000002100200014000c00000000000000000000000000000000000b00040022000000240000000000000501000000000000000100000000000000000000000105060008000400060000000400000000000000";
const TS_SUBSCRIBE_SPACE: &str = "7c00000030000000535750310000000024003a000000390038002c0024001c00000014000000000000000000000013000c0004002400000001000000000000003400000000000007010000000000000001000000000000000100000000000000010000000000000000000000010706000c000400060000000100000000000000";
const TS_RELIABLE_EVENT: &str = "8c0000003000000053575031000000002400540000005300520044003c0034002c00240000001c0000001400100000000000040024000000010000000000000000000000440000000100000000000000010000000000000001000000000000000100000000000000010000000000000001000000000000000100000000000000000000000000010e0200000068690000";
const TS_ENTITY_STATE: &str = "8c0000003000000053575031000000002400500000004f004e0044003c0034002c00240000001c00000014001000000000000400240000000200000000000000000000004000000005000000000000000300000000000000010000000000000007000000000000000100000000000000010000000000000001000000000000000000030d050000007374617465000000";

#[test]
fn ts_hello_decodes() {
    let envelope = decode_hex(TS_HELLO).expect("TS Hello must decode");
    let MessagePayload::Control(signalweave_protocol::ControlPayload::Hello(hello)) =
        envelope.message
    else {
        panic!("expected Hello, got {:?}", envelope.message);
    };
    assert_eq!(
        hello,
        Hello {
            min_protocol_version: 1,
            max_protocol_version: 1,
            client_name: "signalweave-client-ts".to_owned(),
            client_version: "0.1.0".to_owned(),
            capability_bits: 0,
            max_frame_size: 65536,
            max_payload_size: 65536,
        }
    );
    assert_eq!(envelope.namespace_id, 0);
}

#[test]
fn ts_authenticate_decodes() {
    let envelope = decode_hex(TS_AUTHENTICATE).expect("TS Authenticate must decode");
    let MessagePayload::Control(signalweave_protocol::ControlPayload::Authenticate(auth)) =
        envelope.message
    else {
        panic!("expected Authenticate, got {:?}", envelope.message);
    };
    assert_eq!(auth.credentials, b"dev-token");
}

#[test]
fn ts_join_session_decodes() {
    let envelope = decode_hex(TS_JOIN_SESSION).expect("TS JoinSession must decode");
    let MessagePayload::Control(signalweave_protocol::ControlPayload::JoinSession(_)) =
        envelope.message
    else {
        panic!("expected JoinSession, got {:?}", envelope.message);
    };
    assert_eq!(envelope.namespace_id, 1);
    assert_eq!(envelope.session_id, 1);
}

#[test]
fn ts_subscribe_space_decodes() {
    let envelope = decode_hex(TS_SUBSCRIBE_SPACE).expect("TS SubscribeSpace must decode");
    let MessagePayload::Control(signalweave_protocol::ControlPayload::SubscribeSpace(
        SubscribeSpace,
    )) = envelope.message
    else {
        panic!("expected SubscribeSpace, got {:?}", envelope.message);
    };
    assert_eq!(envelope.namespace_id, 1);
    assert_eq!(envelope.session_id, 1);
    assert_eq!(envelope.space_id, 1);
    assert_eq!(envelope.space_epoch, 1);
    assert_eq!(envelope.channel_id, Some(1));
}

#[test]
fn ts_reliable_event_decodes() {
    let envelope = decode_hex(TS_RELIABLE_EVENT).expect("TS ReliableEvent must decode");
    let MessagePayload::ReliableEvent(payload) = envelope.message else {
        panic!("expected ReliableEvent, got {:?}", envelope.message);
    };
    assert_eq!(payload.bytes, b"hi");
    assert_eq!(payload.type_id, 1);
    assert_eq!(envelope.namespace_id, 1);
    assert_eq!(envelope.space_id, 1);
    assert_eq!(envelope.space_epoch, 1);
    assert_eq!(envelope.channel_id, Some(1));
    assert_eq!(envelope.entity_id, Some(1));
    assert_eq!(envelope.sender_sequence, 1);
}

#[test]
fn ts_entity_state_decodes() {
    let envelope = decode_hex(TS_ENTITY_STATE).expect("TS EntityState must decode");
    let MessagePayload::EntityState(payload) = envelope.message else {
        panic!("expected EntityState, got {:?}", envelope.message);
    };
    assert_eq!(payload.bytes, b"state");
    assert_eq!(payload.type_id, 5);
    assert_eq!(envelope.entity_id, Some(7));
    assert_eq!(envelope.sender_sequence, 3);
    assert_eq!(envelope.channel_id, Some(2));
}
