use std::fmt;
use std::sync::Arc;

use crate::{
    ChannelId, CoalesceKey, ConnectionId, DeliveryClass, EntityId, PersistenceClass, PrincipalId,
    SpaceEpoch, SpaceId,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthorityContext {
    pub connection: ConnectionId,
    pub principal: PrincipalId,
    pub is_session_member: bool,
    pub is_space_subscriber: bool,
    pub entity_owner: Option<ConnectionId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProposedMessage<'a> {
    pub space: SpaceId,
    pub space_epoch: SpaceEpoch,
    pub entity: Option<EntityId>,
    pub channel: ChannelId,
    pub sequence: u64,
    pub delivery: DeliveryClass,
    pub persistence: PersistenceClass,
    pub coalesce_key: Option<CoalesceKey>,
    pub payload: &'a [u8],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorityTransform {
    pub coalesce_key: Option<CoalesceKey>,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorityEmission {
    pub space: SpaceId,
    pub space_epoch: SpaceEpoch,
    pub entity: Option<EntityId>,
    pub channel: ChannelId,
    pub sequence: u64,
    pub delivery: DeliveryClass,
    pub persistence: PersistenceClass,
    pub coalesce_key: Option<CoalesceKey>,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorityRejection {
    AuthenticationRequired,
    SessionMembershipRequired,
    SpaceSubscriptionRequired,
    EntityRequired,
    EntityNotOwned,
    PolicyDenied,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthorityOutcome {
    Accept,
    Reject(AuthorityRejection),
    Transform(AuthorityTransform),
    Emit(Box<[AuthorityEmission]>),
}

pub trait AuthorityPolicy: Send + Sync {
    fn evaluate(
        &self,
        context: &AuthorityContext,
        proposed: ProposedMessage<'_>,
    ) -> AuthorityOutcome;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RelayOwned;

impl AuthorityPolicy for RelayOwned {
    fn evaluate(
        &self,
        context: &AuthorityContext,
        proposed: ProposedMessage<'_>,
    ) -> AuthorityOutcome {
        if !context.is_session_member {
            return AuthorityOutcome::Reject(AuthorityRejection::SessionMembershipRequired);
        }
        if !context.is_space_subscriber {
            return AuthorityOutcome::Reject(AuthorityRejection::SpaceSubscriptionRequired);
        }
        if proposed.entity.is_none() {
            return AuthorityOutcome::Reject(AuthorityRejection::EntityRequired);
        }
        if context.entity_owner != Some(context.connection) {
            return AuthorityOutcome::Reject(AuthorityRejection::EntityNotOwned);
        }
        AuthorityOutcome::Accept
    }
}

#[derive(Clone)]
pub struct ChannelDefinition {
    pub id: ChannelId,
    pub delivery: DeliveryClass,
    pub persistence: PersistenceClass,
    pub max_payload_bytes: usize,
    pub authority: Arc<dyn AuthorityPolicy>,
}

impl ChannelDefinition {
    #[must_use]
    pub fn new(
        id: ChannelId,
        delivery: DeliveryClass,
        persistence: PersistenceClass,
        max_payload_bytes: usize,
    ) -> Self {
        Self::relay_owned(id, delivery, persistence, max_payload_bytes)
    }

    #[must_use]
    pub fn relay_owned(
        id: ChannelId,
        delivery: DeliveryClass,
        persistence: PersistenceClass,
        max_payload_bytes: usize,
    ) -> Self {
        Self {
            id,
            delivery,
            persistence,
            max_payload_bytes,
            authority: Arc::new(RelayOwned),
        }
    }

    #[must_use]
    pub fn with_authority(
        id: ChannelId,
        delivery: DeliveryClass,
        persistence: PersistenceClass,
        max_payload_bytes: usize,
        authority: Arc<dyn AuthorityPolicy>,
    ) -> Self {
        Self {
            id,
            delivery,
            persistence,
            max_payload_bytes,
            authority,
        }
    }
}

impl fmt::Debug for ChannelDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChannelDefinition")
            .field("id", &self.id)
            .field("delivery", &self.delivery)
            .field("persistence", &self.persistence)
            .field("max_payload_bytes", &self.max_payload_bytes)
            .field("authority", &"dyn AuthorityPolicy")
            .finish()
    }
}
