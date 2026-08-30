mod common;

use signalweave_protocol::Codec;

const GOLDEN: &[u8] = include_bytes!("fixtures/reliable_event_v1.swp");

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
