#![forbid(unsafe_code)]

mod auth;
mod authority;
mod core;
mod ids;
mod journal;
mod model;
mod queue;
mod worker;

pub use auth::{
    AccessGrant, AuthError, AuthenticatedPrincipal, Authenticator, AuthorizationGrants,
    ChannelScope, Credentials, DevAuthenticator, DevAuthenticatorError,
};
pub use authority::{
    AuthorityContext, AuthorityEmission, AuthorityOutcome, AuthorityPolicy, AuthorityRejection,
    AuthorityTransform, ChannelDefinition, ProposedMessage, RelayOwned,
};
pub use core::{
    CleanupSummary, CoreConfig, CoreError, EntityTransition, EntityTransitionRequest, IdKind,
    PublishOutcome, PublishRateLimit, PublishRequest, QueueActivity, RemovedEntity,
    SignalweaveCore,
};
pub use ids::{
    ChannelId, ConnectionId, EntityId, NamespaceId, PrincipalId, SessionId, SessionKey, SpaceEpoch,
    SpaceId, SpaceKey,
};
pub use journal::{
    JournalError, JournalOutbox, JournalOutboxError, JournalRecord, JournalSink, NoopJournalSink,
};
pub use model::{
    CoalesceKey, CoordinateFrame, DeliveryClass, EntitySnapshot, OutboundMessage, ParentAnchor,
    PersistenceClass, RoutingPolicy, ScopedCoalesceKey, SessionSnapshot, SpaceDescriptor,
    SpaceSnapshot, SpaceValidationError, StateSnapshot,
};
pub use queue::{
    OutboundQueue, OutboundQueueConfig, QueueConfigError, QueueError, QueueEviction, QueuePush,
};
pub use worker::{Command, CommandResult, HarnessError, TransportIndependentWorker, WorkerHarness};
