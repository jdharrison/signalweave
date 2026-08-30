mod common;

use signalweave_protocol::Codec;

const GOLDEN: &[u8] = include_bytes!("fixtures/reliable_event_v1.swp");
const TOOL_CALL_COMPLETED_GOLDEN: &[u8] = include_bytes!("fixtures/tool_call_completed_v1.swp");

#[test]
fn golden_fixture_is_byte_for_byte_stable() {
    let encoded = Codec::default()
        .encode(&common::golden_envelope())
        .expect("golden envelope should encode");
    assert_eq!(encoded.as_slice(), GOLDEN);
}

#[test]
fn golden_fixture_decodes_to_expected_values() {
    let decoded = Codec::default()
        .decode(GOLDEN)
        .expect("golden fixture should verify and decode");
    assert_eq!(decoded, common::golden_envelope());
}

/// Second, additive fixture (Milestone 5): proves a typed inference/tool-call control
/// message round-trips byte-for-byte, independent of the `ReliableEvent` fixture above.
#[test]
fn tool_call_completed_fixture_is_byte_for_byte_stable() {
    let encoded = Codec::default()
        .encode(&common::tool_call_completed_envelope())
        .expect("tool_call_completed envelope should encode");
    assert_eq!(encoded.as_slice(), TOOL_CALL_COMPLETED_GOLDEN);
}

#[test]
fn tool_call_completed_fixture_decodes_to_expected_values() {
    let decoded = Codec::default()
        .decode(TOOL_CALL_COMPLETED_GOLDEN)
        .expect("tool_call_completed fixture should verify and decode");
    assert_eq!(decoded, common::tool_call_completed_envelope());
}
