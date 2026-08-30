use signalweave_protocol::{DeliveryClass, Envelope, OpaquePayload};

pub fn golden_envelope() -> Envelope {
    let mut envelope = Envelope::reliable_event(
        DeliveryClass::ReliableOrdered,
        OpaquePayload {
            type_id: 0x1020_3040_5060_7080,
            bytes: vec![0xde, 0xad, 0xbe, 0xef, 0x53, 0x57, 0x50, 0x31],
        },
    );
    envelope.namespace_id = 0x0102_0304_0506_0708;
    envelope.session_id = 0x1112_1314_1516_1718;
    envelope.space_id = 0x2122_2324_2526_2728;
    envelope.channel_id = Some(0x292a_2b2c_2d2e_2f30);
    envelope.entity_id = Some(0x3132_3334_3536_3738);
    envelope.space_epoch = 0x4142_4344_4546_4748;
    envelope.server_tick = 0x5152_5354_5556_5758;
    envelope.sender_sequence = 0x6162_6364_6566_6768;
    envelope.correlation_id = Some(0x7172_7374_7576_7778);
    envelope
}
