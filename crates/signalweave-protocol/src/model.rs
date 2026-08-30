use crate::PROTOCOL_VERSION;

macro_rules! numeric_enum {
    ($name:ident, $repr:ty, { $($variant:ident = $value:expr),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
        #[repr($repr)]
        pub enum $name {
            $($variant = $value),+
        }

        impl $name {
            #[must_use]
            pub const fn value(self) -> $repr {
                self as $repr
            }

            pub(crate) fn from_wire(value: $repr) -> Option<Self> {
                match value {
                    $($value => Some(Self::$variant),)+
                    _ => None,
                }
            }
        }
    };
}

numeric_enum!(MessageKind, u8, {
    Unknown = 0,
    Hello = 1,
    Capabilities = 2,
    Authenticate = 3,
    Authenticated = 4,
    JoinSession = 5,
    LeaveSession = 6,
    SubscribeSpace = 7,
    UnsubscribeSpace = 8,
    SubscriptionAccepted = 9,
    SubscriptionRejected = 10,
    EntityEntered = 11,
    EntityLeft = 12,
    EntityState = 13,
    ReliableEvent = 14,
    SnapshotRequest = 15,
    Snapshot = 16,
    SpaceTransition = 17,
    Ping = 18,
    Pong = 19,
    ProtocolError = 20,
});

numeric_enum!(DeliveryClass, u8, {
    Unknown = 0,
    ReliableOrdered = 1,
    ReliableUnordered = 2,
    LatestValue = 3,
    UnreliableSequenced = 4,
    BestEffortEvent = 5,
});

impl DeliveryClass {
    /// Returns `true` for delivery classes that should be carried on
    /// unreliable datagram transports rather than reliable streams.
    #[must_use]
    pub const fn is_unreliable(self) -> bool {
        matches!(self, Self::UnreliableSequenced | Self::BestEffortEvent)
    }
}

numeric_enum!(AuthenticationScheme, u8, {
    Unknown = 0,
    Bearer = 1,
    Development = 2,
});

numeric_enum!(SubscriptionRejectionCode, u16, {
    Unknown = 0,
    Unauthorized = 1,
    SpaceNotFound = 2,
    CapacityExceeded = 3,
    EpochMismatch = 4,
});

numeric_enum!(EntityLeaveReason, u8, {
    Unknown = 0,
    Unsubscribed = 1,
    Disconnected = 2,
    Transitioned = 3,
    Removed = 4,
});

numeric_enum!(ProtocolErrorCode, u16, {
    Unknown = 0,
    MalformedFrame = 1,
    UnsupportedVersion = 2,
    UnsupportedMessage = 3,
    AuthenticationRequired = 4,
    Unauthorized = 5,
    InvalidScope = 6,
    StaleEpoch = 7,
    SequenceRejected = 8,
    PayloadTooLarge = 9,
    RateLimited = 10,
    Internal = 11,
});

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Hello {
    pub min_protocol_version: u16,
    pub max_protocol_version: u16,
    pub client_name: String,
    pub client_version: String,
    pub capability_bits: u64,
    pub max_frame_size: u32,
    pub max_payload_size: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Capabilities {
    pub selected_protocol_version: u16,
    pub server_name: String,
    pub server_version: String,
    pub capability_bits: u64,
    pub max_frame_size: u32,
    pub max_payload_size: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Authenticate {
    pub scheme: AuthenticationScheme,
    pub credentials: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Authenticated {
    pub principal_id: u64,
    pub assigned_entity_id: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinSession {
    pub resume_token: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaveSession {
    pub reason: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscribeSpace;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnsubscribeSpace {
    pub subscription_id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubscriptionAccepted {
    pub subscription_id: u64,
    pub accepted_space_epoch: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionRejected {
    pub code: SubscriptionRejectionCode,
    pub reason: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EntityEntered {
    pub owner_entity_id: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EntityLeft {
    pub reason: EntityLeaveReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotRequest {
    pub after_server_tick: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SpaceTransition {
    pub from_space_id: u64,
    pub to_space_id: u64,
    pub to_space_epoch: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Ping {
    pub nonce: u64,
    pub sender_time_micros: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Pong {
    pub nonce: u64,
    pub sender_time_micros: u64,
    pub responder_time_micros: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtocolError {
    pub code: ProtocolErrorCode,
    pub related_message_kind: MessageKind,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlPayload {
    Hello(Hello),
    Capabilities(Capabilities),
    Authenticate(Authenticate),
    Authenticated(Authenticated),
    JoinSession(JoinSession),
    LeaveSession(LeaveSession),
    SubscribeSpace(SubscribeSpace),
    UnsubscribeSpace(UnsubscribeSpace),
    SubscriptionAccepted(SubscriptionAccepted),
    SubscriptionRejected(SubscriptionRejected),
    EntityEntered(EntityEntered),
    EntityLeft(EntityLeft),
    SnapshotRequest(SnapshotRequest),
    SpaceTransition(SpaceTransition),
    Ping(Ping),
    Pong(Pong),
    ProtocolError(ProtocolError),
}

impl ControlPayload {
    #[must_use]
    pub const fn message_kind(&self) -> MessageKind {
        match self {
            Self::Hello(_) => MessageKind::Hello,
            Self::Capabilities(_) => MessageKind::Capabilities,
            Self::Authenticate(_) => MessageKind::Authenticate,
            Self::Authenticated(_) => MessageKind::Authenticated,
            Self::JoinSession(_) => MessageKind::JoinSession,
            Self::LeaveSession(_) => MessageKind::LeaveSession,
            Self::SubscribeSpace(_) => MessageKind::SubscribeSpace,
            Self::UnsubscribeSpace(_) => MessageKind::UnsubscribeSpace,
            Self::SubscriptionAccepted(_) => MessageKind::SubscriptionAccepted,
            Self::SubscriptionRejected(_) => MessageKind::SubscriptionRejected,
            Self::EntityEntered(_) => MessageKind::EntityEntered,
            Self::EntityLeft(_) => MessageKind::EntityLeft,
            Self::SnapshotRequest(_) => MessageKind::SnapshotRequest,
            Self::SpaceTransition(_) => MessageKind::SpaceTransition,
            Self::Ping(_) => MessageKind::Ping,
            Self::Pong(_) => MessageKind::Pong,
            Self::ProtocolError(_) => MessageKind::ProtocolError,
        }
    }

    pub(crate) fn variable_len(&self) -> usize {
        match self {
            Self::Hello(value) => value
                .client_name
                .len()
                .saturating_add(value.client_version.len()),
            Self::Capabilities(value) => value
                .server_name
                .len()
                .saturating_add(value.server_version.len()),
            Self::Authenticate(value) => value.credentials.len(),
            Self::JoinSession(value) => value.resume_token.len(),
            Self::LeaveSession(value) => value.reason.len(),
            Self::SubscriptionRejected(value) => value.reason.len(),
            Self::ProtocolError(value) => value.message.len(),
            Self::Authenticated(_)
            | Self::SubscribeSpace(_)
            | Self::UnsubscribeSpace(_)
            | Self::SubscriptionAccepted(_)
            | Self::EntityEntered(_)
            | Self::EntityLeft(_)
            | Self::SnapshotRequest(_)
            | Self::SpaceTransition(_)
            | Self::Ping(_)
            | Self::Pong(_) => 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpaquePayload {
    pub type_id: u64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MessagePayload {
    Control(ControlPayload),
    EntityState(OpaquePayload),
    ReliableEvent(OpaquePayload),
    Snapshot(OpaquePayload),
}

impl MessagePayload {
    #[must_use]
    pub const fn message_kind(&self) -> MessageKind {
        match self {
            Self::Control(control) => control.message_kind(),
            Self::EntityState(_) => MessageKind::EntityState,
            Self::ReliableEvent(_) => MessageKind::ReliableEvent,
            Self::Snapshot(_) => MessageKind::Snapshot,
        }
    }

    #[must_use]
    pub const fn opaque(&self) -> Option<&OpaquePayload> {
        match self {
            Self::Control(_) => None,
            Self::EntityState(payload) | Self::ReliableEvent(payload) | Self::Snapshot(payload) => {
                Some(payload)
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Envelope {
    pub protocol_version: u16,
    pub delivery_class: DeliveryClass,
    pub namespace_id: u64,
    pub session_id: u64,
    pub space_id: u64,
    pub channel_id: Option<u64>,
    pub entity_id: Option<u64>,
    pub space_epoch: u64,
    pub server_tick: u64,
    pub sender_sequence: u64,
    pub correlation_id: Option<u64>,
    pub message: MessagePayload,
}

impl Envelope {
    #[must_use]
    pub const fn new(delivery_class: DeliveryClass, message: MessagePayload) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            delivery_class,
            namespace_id: 0,
            session_id: 0,
            space_id: 0,
            channel_id: None,
            entity_id: None,
            space_epoch: 0,
            server_tick: 0,
            sender_sequence: 0,
            correlation_id: None,
            message,
        }
    }

    #[must_use]
    pub const fn control(delivery_class: DeliveryClass, payload: ControlPayload) -> Self {
        Self::new(delivery_class, MessagePayload::Control(payload))
    }

    #[must_use]
    pub const fn entity_state(delivery_class: DeliveryClass, payload: OpaquePayload) -> Self {
        Self::new(delivery_class, MessagePayload::EntityState(payload))
    }

    #[must_use]
    pub const fn reliable_event(delivery_class: DeliveryClass, payload: OpaquePayload) -> Self {
        Self::new(delivery_class, MessagePayload::ReliableEvent(payload))
    }

    #[must_use]
    pub const fn snapshot(delivery_class: DeliveryClass, payload: OpaquePayload) -> Self {
        Self::new(delivery_class, MessagePayload::Snapshot(payload))
    }

    #[must_use]
    pub const fn message_kind(&self) -> MessageKind {
        self.message.message_kind()
    }

    #[must_use]
    pub const fn payload_type_id(&self) -> Option<u64> {
        match self.message.opaque() {
            Some(payload) => Some(payload.type_id),
            None => None,
        }
    }

    #[must_use]
    pub fn payload_bytes(&self) -> &[u8] {
        self.message
            .opaque()
            .map_or(&[], |payload| payload.bytes.as_slice())
    }
}
