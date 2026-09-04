//! Transport-independent worker, lifecycle fan-out, and protocol bridge.

#![deny(unsafe_code)]

use signalweave_core::{
    Authenticator, CleanupSummary, CoalesceKey, Command, CommandResult, ConnectionId, CoreError,
    DeliveryClass as CoreDelivery, EntityTransition, PersistenceClass, PublishRequest,
    RemovedEntity, SpaceEpoch, SpaceKey, TransportIndependentWorker,
};
use signalweave_protocol::{
    ControlPayload, DeliveryClass, EntityEntered, EntityLeaveReason, EntityLeft, Envelope,
    MessageKind, MessagePayload, OpaquePayload, PROTOCOL_VERSION, ProtocolError, ProtocolErrorCode,
};
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::{mpsc, oneshot};
/// Maximum number of commands pending for the single core owner.
pub const COMMAND_CAPACITY: usize = 256;
/// Maximum protocol frame size advertised by the transport bridge.
pub const MAX_FRAME_BYTES: u32 = 1_048_576;
/// Maximum domain payload size advertised by the transport bridge.
pub const MAX_PAYLOAD_BYTES: u32 = 262_144;

/// Errors from the transport boundary.
#[derive(Debug)]
pub enum TransportError {
    WorkerUnavailable,
    Core(CoreError),
    UnknownConnection,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WorkerUnavailable => formatter.write_str("core worker is unavailable"),
            Self::Core(error) => write!(formatter, "core error: {error:?}"),
            Self::UnknownConnection => {
                formatter.write_str("connection is not registered for direct delivery")
            }
        }
    }
}

impl std::error::Error for TransportError {}

enum WorkerRequest {
    Command {
        command: Command,
        reply: oneshot::Sender<Result<CommandResult, CoreError>>,
    },
    RegisterLifecycle {
        connection: ConnectionId,
        recipient: LifecycleRecipient,
        reply: oneshot::Sender<()>,
    },
    SubscribeAndSpawn {
        connection: ConnectionId,
        space: SpaceKey,
        epoch: SpaceEpoch,
        reply: oneshot::Sender<Result<signalweave_core::EntityId, CoreError>>,
    },
    ActivateSubscription {
        connection: ConnectionId,
        space: SpaceKey,
        reply: oneshot::Sender<()>,
    },
    BroadcastToSpace {
        space: SpaceKey,
        envelope: Envelope,
        exclude: Option<ConnectionId>,
        reply: oneshot::Sender<()>,
    },
    SendToConnection {
        connection: ConnectionId,
        envelope: Envelope,
        reply: oneshot::Sender<Result<(), TransportError>>,
    },
}

struct LifecycleRecipient {
    sender: mpsc::Sender<Envelope>,
    shutdown: mpsc::Sender<()>,
}

#[derive(Clone, Copy)]
enum LifecycleAction {
    None,
    Subscribe {
        connection: ConnectionId,
        space: SpaceKey,
    },
    Unsubscribe {
        connection: ConnectionId,
        space: SpaceKey,
    },
    LeaveSession {
        connection: ConnectionId,
        session: signalweave_core::SessionKey,
    },
    Spawn {
        space: SpaceKey,
        epoch: SpaceEpoch,
    },
    RemoveEntity,
    Transition,
    TransportLost {
        connection: ConnectionId,
    },
}

/// A control envelope `handle_authenticated` does not itself implement, handed off to an
/// optional adjacent plane (e.g. the inference coordinator) rather than rejected outright.
/// `signalweave-transport` has no knowledge of what consumes these.
#[derive(Debug)]
pub struct UnroutedControl {
    pub connection: ConnectionId,
    pub envelope: Envelope,
}

/// Cloneable bounded command client for the single Signalweave core owner.
#[derive(Clone)]
pub struct WorkerHandle {
    sender: mpsc::Sender<WorkerRequest>,
}

impl WorkerHandle {
    /// Submit a command to the single owner and wait for its result.
    pub async fn execute(&self, command: Command) -> Result<CommandResult, TransportError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(WorkerRequest::Command { command, reply })
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?;
        receive
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?
            .map_err(TransportError::Core)
    }

    pub async fn register_lifecycle(
        &self,
        connection: ConnectionId,
        sender: mpsc::Sender<Envelope>,
        shutdown: mpsc::Sender<()>,
    ) -> Result<(), TransportError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(WorkerRequest::RegisterLifecycle {
                connection,
                recipient: LifecycleRecipient { sender, shutdown },
                reply,
            })
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?;
        receive.await.map_err(|_| TransportError::WorkerUnavailable)
    }

    pub async fn discard_and_disconnect(&self, connection: ConnectionId) {
        let _ = self.execute(Command::DrainOutbound { connection }).await;
        let _ = self.execute(Command::TransportLost { connection }).await;
    }

    pub async fn subscribe_and_spawn(
        &self,
        connection: ConnectionId,
        space: SpaceKey,
        epoch: SpaceEpoch,
    ) -> Result<signalweave_core::EntityId, TransportError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(WorkerRequest::SubscribeAndSpawn {
                connection,
                space,
                epoch,
                reply,
            })
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?;
        receive
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?
            .map_err(TransportError::Core)
    }

    pub async fn activate_subscription(
        &self,
        connection: ConnectionId,
        space: SpaceKey,
    ) -> Result<(), TransportError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(WorkerRequest::ActivateSubscription {
                connection,
                space,
                reply,
            })
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?;
        receive.await.map_err(|_| TransportError::WorkerUnavailable)
    }

    /// Deliver `envelope` directly to every connection currently subscribed to `space`,
    /// bypassing the per-connection outbound queue. Mirrors the delivery path already used
    /// for `EntityEntered`/`EntityLeft`/`SpaceTransition`, generalized for any envelope.
    pub async fn broadcast_to_space(
        &self,
        space: SpaceKey,
        envelope: Envelope,
        exclude: Option<ConnectionId>,
    ) -> Result<(), TransportError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(WorkerRequest::BroadcastToSpace {
                space,
                envelope,
                exclude,
                reply,
            })
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?;
        receive.await.map_err(|_| TransportError::WorkerUnavailable)
    }

    /// Deliver `envelope` directly to a single registered connection, bypassing the
    /// per-connection outbound queue. The connection must have called `register_lifecycle`.
    pub async fn send_to_connection(
        &self,
        connection: ConnectionId,
        envelope: Envelope,
    ) -> Result<(), TransportError> {
        let (reply, receive) = oneshot::channel();
        self.sender
            .send(WorkerRequest::SendToConnection {
                connection,
                envelope,
                reply,
            })
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?;
        receive
            .await
            .map_err(|_| TransportError::WorkerUnavailable)?
    }
}

/// Spawn the bounded, single-owner core command worker.
pub fn spawn_worker<A>(worker: TransportIndependentWorker<A>) -> WorkerHandle
where
    A: Authenticator + Send + 'static,
{
    let (sender, mut receiver) = mpsc::channel::<WorkerRequest>(COMMAND_CAPACITY);
    tokio::spawn(async move {
        let mut worker = worker;
        let mut recipients = BTreeMap::new();
        let mut subscriptions = BTreeMap::new();
        while let Some(request) = receiver.recv().await {
            match request {
                WorkerRequest::Command { command, reply } => {
                    let action = lifecycle_action(&command);
                    let result = worker.handle(command);
                    if let Ok(result) = &result {
                        apply_lifecycle_action(
                            &mut worker,
                            &mut recipients,
                            &mut subscriptions,
                            action,
                            result,
                        );
                    }
                    let _ = reply.send(result);
                }
                WorkerRequest::RegisterLifecycle {
                    connection,
                    recipient,
                    reply,
                } => {
                    recipients.insert(connection, recipient);
                    subscriptions
                        .entry(connection)
                        .or_insert_with(BTreeSet::new);
                    let _ = reply.send(());
                }
                WorkerRequest::SubscribeAndSpawn {
                    connection,
                    space,
                    epoch,
                    reply,
                } => {
                    let result = match worker.core_mut().subscribe(connection, space) {
                        Ok(()) => match worker.core_mut().spawn_entity(connection, space, epoch) {
                            Ok(entity) => {
                                distribute_to_space_excluding(
                                    &mut worker,
                                    &mut recipients,
                                    &mut subscriptions,
                                    space,
                                    &entity_entered_envelope(space, epoch, entity),
                                    Some(connection),
                                );
                                Ok(entity)
                            }
                            Err(error) => Err(error),
                        },
                        Err(error) => Err(error),
                    };
                    let _ = reply.send(result);
                }
                WorkerRequest::ActivateSubscription {
                    connection,
                    space,
                    reply,
                } => {
                    subscriptions.entry(connection).or_default().insert(space);
                    let _ = reply.send(());
                }
                WorkerRequest::BroadcastToSpace {
                    space,
                    envelope,
                    exclude,
                    reply,
                } => {
                    distribute_to_space_excluding(
                        &mut worker,
                        &mut recipients,
                        &mut subscriptions,
                        space,
                        &envelope,
                        exclude,
                    );
                    let _ = reply.send(());
                }
                WorkerRequest::SendToConnection {
                    connection,
                    envelope,
                    reply,
                } => {
                    let result = deliver_to_connection(
                        &mut worker,
                        &mut recipients,
                        &mut subscriptions,
                        connection,
                        &envelope,
                    );
                    let _ = reply.send(result);
                }
            }
        }
    });
    WorkerHandle { sender }
}

fn lifecycle_action(command: &Command) -> LifecycleAction {
    match command {
        Command::Subscribe { connection, space } => LifecycleAction::Subscribe {
            connection: *connection,
            space: *space,
        },
        Command::Unsubscribe { connection, space } => LifecycleAction::Unsubscribe {
            connection: *connection,
            space: *space,
        },
        Command::LeaveSession {
            connection,
            session,
        } => LifecycleAction::LeaveSession {
            connection: *connection,
            session: *session,
        },
        Command::SpawnEntity { space, epoch, .. } => LifecycleAction::Spawn {
            space: *space,
            epoch: *epoch,
        },
        Command::RemoveEntity { .. } => LifecycleAction::RemoveEntity,
        Command::TransitionEntity(_) => LifecycleAction::Transition,
        Command::TransportLost { connection } => LifecycleAction::TransportLost {
            connection: *connection,
        },
        Command::TransportConnected
        | Command::Authenticate { .. }
        | Command::JoinSession { .. }
        | Command::RequestSessionAdmission { .. }
        | Command::JoinSessionWithAdmission { .. }
        | Command::UpdateEntityPosition { .. }
        | Command::Publish(_)
        | Command::Snapshot { .. }
        | Command::DrainOutbound { .. } => LifecycleAction::None,
    }
}

fn apply_lifecycle_action<A>(
    worker: &mut TransportIndependentWorker<A>,
    recipients: &mut BTreeMap<ConnectionId, LifecycleRecipient>,
    subscriptions: &mut BTreeMap<ConnectionId, BTreeSet<SpaceKey>>,
    action: LifecycleAction,
    result: &CommandResult,
) where
    A: Authenticator,
{
    match (action, result) {
        (LifecycleAction::Subscribe { connection, space }, CommandResult::Subscribed) => {
            subscriptions.entry(connection).or_default().insert(space);
        }
        (
            LifecycleAction::Unsubscribe { connection, space },
            CommandResult::Unsubscribed(summary),
        ) => {
            if let Some(connection_subscriptions) = subscriptions.get_mut(&connection) {
                connection_subscriptions.remove(&space);
            }
            distribute_removed_entities(
                worker,
                recipients,
                subscriptions,
                summary,
                EntityLeaveReason::Removed,
            );
        }
        (
            LifecycleAction::LeaveSession {
                connection,
                session,
            },
            CommandResult::Left(summary),
        ) => {
            if let Some(connection_subscriptions) = subscriptions.get_mut(&connection) {
                connection_subscriptions.retain(|space| space.session != session);
            }
            distribute_removed_entities(
                worker,
                recipients,
                subscriptions,
                summary,
                EntityLeaveReason::Removed,
            );
        }
        (LifecycleAction::Spawn { space, epoch }, CommandResult::EntitySpawned(entity)) => {
            distribute_to_space(
                worker,
                recipients,
                subscriptions,
                space,
                &entity_entered_envelope(space, epoch, *entity),
            );
        }
        (LifecycleAction::RemoveEntity, CommandResult::EntityRemoved(summary)) => {
            distribute_removed_entities(
                worker,
                recipients,
                subscriptions,
                summary,
                EntityLeaveReason::Removed,
            );
        }
        (LifecycleAction::Transition, CommandResult::EntityTransitioned(transition)) => {
            distribute_transition(worker, recipients, subscriptions, *transition);
        }
        (LifecycleAction::TransportLost { connection }, CommandResult::Disconnected(summary)) => {
            recipients.remove(&connection);
            subscriptions.remove(&connection);
            distribute_removed_entities(
                worker,
                recipients,
                subscriptions,
                summary,
                EntityLeaveReason::Disconnected,
            );
        }
        _ => {}
    }
}

fn distribute_transition<A>(
    worker: &mut TransportIndependentWorker<A>,
    recipients: &mut BTreeMap<ConnectionId, LifecycleRecipient>,
    subscriptions: &mut BTreeMap<ConnectionId, BTreeSet<SpaceKey>>,
    transition: EntityTransition,
) where
    A: Authenticator,
{
    let source = SpaceKey::new(transition.session, transition.source_space);
    distribute_to_space(
        worker,
        recipients,
        subscriptions,
        source,
        &entity_left_envelope(
            source,
            transition.source_epoch,
            transition.entity,
            EntityLeaveReason::Transitioned,
        ),
    );
    let destination = SpaceKey::new(transition.session, transition.destination_space);
    distribute_to_space(
        worker,
        recipients,
        subscriptions,
        destination,
        &entity_entered_envelope(destination, transition.destination_epoch, transition.entity),
    );
}

fn distribute_removed_entities<A>(
    worker: &mut TransportIndependentWorker<A>,
    recipients: &mut BTreeMap<ConnectionId, LifecycleRecipient>,
    subscriptions: &mut BTreeMap<ConnectionId, BTreeSet<SpaceKey>>,
    summary: &CleanupSummary,
    reason: EntityLeaveReason,
) where
    A: Authenticator,
{
    for removed in &summary.removed_entities {
        distribute_removed_entity(worker, recipients, subscriptions, *removed, reason);
    }
}

fn distribute_removed_entity<A>(
    worker: &mut TransportIndependentWorker<A>,
    recipients: &mut BTreeMap<ConnectionId, LifecycleRecipient>,
    subscriptions: &mut BTreeMap<ConnectionId, BTreeSet<SpaceKey>>,
    removed: RemovedEntity,
    reason: EntityLeaveReason,
) where
    A: Authenticator,
{
    let space = SpaceKey::new(removed.session, removed.space);
    distribute_to_space(
        worker,
        recipients,
        subscriptions,
        space,
        &entity_left_envelope(space, removed.space_epoch, removed.entity, reason),
    );
}

fn distribute_to_space<A>(
    worker: &mut TransportIndependentWorker<A>,
    recipients: &mut BTreeMap<ConnectionId, LifecycleRecipient>,
    subscriptions: &mut BTreeMap<ConnectionId, BTreeSet<SpaceKey>>,
    space: SpaceKey,
    envelope: &Envelope,
) where
    A: Authenticator,
{
    distribute_to_space_excluding(worker, recipients, subscriptions, space, envelope, None);
}

fn distribute_to_space_excluding<A>(
    worker: &mut TransportIndependentWorker<A>,
    recipients: &mut BTreeMap<ConnectionId, LifecycleRecipient>,
    subscriptions: &mut BTreeMap<ConnectionId, BTreeSet<SpaceKey>>,
    space: SpaceKey,
    envelope: &Envelope,
    excluded: Option<ConnectionId>,
) where
    A: Authenticator,
{
    let targets = subscriptions
        .iter()
        .filter_map(|(connection, connection_subscriptions)| {
            (excluded != Some(*connection) && connection_subscriptions.contains(&space))
                .then_some(*connection)
        })
        .collect::<Vec<_>>();
    let mut disconnected = Vec::new();
    for connection in targets {
        let Some(recipient) = recipients.get(&connection) else {
            continue;
        };
        if recipient.sender.try_send(envelope.clone()).is_err() {
            let _ = recipient.shutdown.try_send(());
            disconnected.push(connection);
        }
    }
    for connection in disconnected {
        cleanup_dead_connection(worker, recipients, subscriptions, connection);
    }
}

/// Deliver `envelope` directly to a single registered connection.
fn deliver_to_connection<A>(
    worker: &mut TransportIndependentWorker<A>,
    recipients: &mut BTreeMap<ConnectionId, LifecycleRecipient>,
    subscriptions: &mut BTreeMap<ConnectionId, BTreeSet<SpaceKey>>,
    connection: ConnectionId,
    envelope: &Envelope,
) -> Result<(), TransportError>
where
    A: Authenticator,
{
    let Some(recipient) = recipients.get(&connection) else {
        return Err(TransportError::UnknownConnection);
    };
    if recipient.sender.try_send(envelope.clone()).is_ok() {
        return Ok(());
    }
    let _ = recipient.shutdown.try_send(());
    cleanup_dead_connection(worker, recipients, subscriptions, connection);
    Err(TransportError::UnknownConnection)
}

/// Remove a connection whose write channel has failed and run the same disconnect cleanup
/// and fan-out as an explicit `TransportLost`.
fn cleanup_dead_connection<A>(
    worker: &mut TransportIndependentWorker<A>,
    recipients: &mut BTreeMap<ConnectionId, LifecycleRecipient>,
    subscriptions: &mut BTreeMap<ConnectionId, BTreeSet<SpaceKey>>,
    connection: ConnectionId,
) where
    A: Authenticator,
{
    recipients.remove(&connection);
    subscriptions.remove(&connection);
    let _ = worker.handle(Command::DrainOutbound { connection });
    if let Ok(CommandResult::Disconnected(summary)) =
        worker.handle(Command::TransportLost { connection })
    {
        distribute_removed_entities(
            worker,
            recipients,
            subscriptions,
            &summary,
            EntityLeaveReason::Disconnected,
        );
    }
}

#[must_use]
pub fn entity_entered_envelope(
    space: SpaceKey,
    epoch: SpaceEpoch,
    entity: signalweave_core::EntityId,
) -> Envelope {
    Envelope {
        protocol_version: PROTOCOL_VERSION,
        delivery_class: DeliveryClass::ReliableOrdered,
        namespace_id: space.session.namespace.get(),
        session_id: space.session.session.get(),
        space_id: space.space.get(),
        channel_id: None,
        entity_id: Some(entity.get()),
        space_epoch: epoch.get(),
        server_tick: 0,
        sender_sequence: 0,
        correlation_id: None,
        message: MessagePayload::Control(ControlPayload::EntityEntered(EntityEntered {
            owner_entity_id: Some(entity.get()),
        })),
    }
}

#[must_use]
pub fn entity_left_envelope(
    space: SpaceKey,
    epoch: SpaceEpoch,
    entity: signalweave_core::EntityId,
    reason: EntityLeaveReason,
) -> Envelope {
    Envelope {
        protocol_version: PROTOCOL_VERSION,
        delivery_class: DeliveryClass::ReliableOrdered,
        namespace_id: space.session.namespace.get(),
        session_id: space.session.session.get(),
        space_id: space.space.get(),
        channel_id: None,
        entity_id: Some(entity.get()),
        space_epoch: epoch.get(),
        server_tick: 0,
        sender_sequence: 0,
        correlation_id: None,
        message: MessagePayload::Control(ControlPayload::EntityLeft(EntityLeft { reason })),
    }
}

#[allow(clippy::too_many_lines, clippy::result_unit_err)]
pub async fn handle_authenticated(
    worker: &WorkerHandle,
    connection: ConnectionId,
    envelope: Envelope,
    write_sender: &mpsc::Sender<Envelope>,
    inference_sink: Option<&mpsc::Sender<UnroutedControl>>,
) -> Result<(), ()> {
    use signalweave_core::{
        ChannelId, NamespaceId, SessionId, SessionKey, SpaceEpoch, SpaceId, SpaceKey,
    };
    if let Some(sink) = inference_sink
        && matches!(
            envelope.message,
            MessagePayload::Control(
                ControlPayload::InferenceRequested(_) | ControlPayload::InferenceCancelled(_)
            )
        )
    {
        let _ = sink.try_send(UnroutedControl {
            connection,
            envelope,
        });
        return Ok(());
    }
    let session = SessionKey {
        namespace: NamespaceId::new(envelope.namespace_id),
        session: SessionId::new(envelope.session_id),
    };
    let space = SpaceKey {
        session,
        space: SpaceId::new(envelope.space_id),
    };
    if matches!(
        envelope.message,
        MessagePayload::Control(ControlPayload::SnapshotRequest(_))
    ) {
        return match worker
            .execute(Command::Snapshot {
                connection,
                session,
            })
            .await
        {
            Ok(CommandResult::Snapshot(snapshot)) => {
                let mut response = envelope;
                response.message = MessagePayload::Snapshot(OpaquePayload {
                    type_id: 1,
                    bytes: format!("{snapshot:?}").into_bytes(),
                });
                send_envelope(write_sender, response).await
            }
            Ok(_) => Err(()),
            Err(error) => {
                send_error(
                    write_sender,
                    MessageKind::SnapshotRequest,
                    ProtocolErrorCode::Internal,
                    error.to_string(),
                )
                .await;
                Err(())
            }
        };
    }
    if matches!(
        envelope.message,
        MessagePayload::Control(ControlPayload::SubscribeSpace(_))
    ) {
        return match worker
            .subscribe_and_spawn(connection, space, SpaceEpoch::new(envelope.space_epoch))
            .await
        {
            Ok(entity) => {
                let subscription_id = envelope.space_id;
                let mut accepted = envelope.clone();
                accepted.message = MessagePayload::Control(ControlPayload::SubscriptionAccepted(
                    signalweave_protocol::SubscriptionAccepted {
                        subscription_id,
                        accepted_space_epoch: accepted.space_epoch,
                    },
                ));
                accepted.delivery_class = DeliveryClass::ReliableOrdered;
                send_envelope(write_sender, accepted).await?;
                send_envelope(
                    write_sender,
                    entity_entered_envelope(space, SpaceEpoch::new(envelope.space_epoch), entity),
                )
                .await?;
                worker
                    .activate_subscription(connection, space)
                    .await
                    .map_err(|_| ())?;
                flush_outbound(worker, connection, write_sender).await
            }
            Err(error) => {
                let code = match &error {
                    TransportError::Core(core_error) => core_error_code(core_error),
                    TransportError::WorkerUnavailable | TransportError::UnknownConnection => {
                        ProtocolErrorCode::Internal
                    }
                };
                send_error(
                    write_sender,
                    MessageKind::SubscribeSpace,
                    code,
                    error.to_string(),
                )
                .await;
                Err(())
            }
        };
    }
    let command = match &envelope.message {
        MessagePayload::Control(ControlPayload::JoinSession(_)) => Command::JoinSession {
            connection,
            session,
        },
        MessagePayload::Control(ControlPayload::LeaveSession(_)) => Command::LeaveSession {
            connection,
            session,
        },
        MessagePayload::Control(ControlPayload::UnsubscribeSpace(_)) => {
            Command::Unsubscribe { connection, space }
        }
        MessagePayload::Control(ControlPayload::SpaceTransition(transition)) => {
            Command::TransitionEntity(signalweave_core::EntityTransitionRequest {
                connection,
                session,
                entity: signalweave_core::EntityId::new(envelope.entity_id.ok_or(())?),
                source_space: SpaceId::new(transition.from_space_id),
                source_epoch: SpaceEpoch::new(envelope.space_epoch),
                destination_space: SpaceId::new(transition.to_space_id),
                destination_epoch: SpaceEpoch::new(transition.to_space_epoch),
            })
        }
        MessagePayload::ReliableEvent(payload) | MessagePayload::EntityState(payload) => {
            let delivery = core_delivery(envelope.delivery_class).ok_or(())?;
            let persistence = if matches!(envelope.message, MessagePayload::EntityState(_)) {
                PersistenceClass::Stateful
            } else {
                PersistenceClass::Ephemeral
            };
            let entity = envelope.entity_id.map(signalweave_core::EntityId::new);
            let channel = ChannelId::new(envelope.channel_id.ok_or(())?);
            Command::Publish(PublishRequest {
                connection,
                session,
                space: SpaceId::new(envelope.space_id),
                space_epoch: SpaceEpoch::new(envelope.space_epoch),
                entity,
                channel,
                sequence: envelope.sender_sequence,
                delivery,
                persistence,
                coalesce_key: if delivery.is_replaceable() {
                    Some(CoalesceKey::new(channel, entity, payload.type_id))
                } else {
                    None
                },
                payload: payload.bytes.clone(),
            })
        }
        _ => {
            send_error(
                write_sender,
                envelope.message_kind(),
                ProtocolErrorCode::UnsupportedMessage,
                "message is not implemented by this transport".to_owned(),
            )
            .await;
            return Err(());
        }
    };
    match worker.execute(command).await {
        Ok(_) => {}
        Err(error) => {
            let code = match &error {
                TransportError::Core(core_error) => core_error_code(core_error),
                TransportError::WorkerUnavailable | TransportError::UnknownConnection => {
                    ProtocolErrorCode::Internal
                }
            };
            send_error(
                write_sender,
                envelope.message_kind(),
                code,
                error.to_string(),
            )
            .await;
            return Err(());
        }
    }
    flush_outbound(worker, connection, write_sender).await
}

#[allow(clippy::result_unit_err)]
pub async fn flush_outbound(
    worker: &WorkerHandle,
    connection: ConnectionId,
    write_sender: &mpsc::Sender<Envelope>,
) -> Result<(), ()> {
    if let Ok(CommandResult::Outbound(messages)) =
        worker.execute(Command::DrainOutbound { connection }).await
    {
        for message in messages {
            send_envelope(write_sender, outbound_envelope(message)).await?;
        }
    }
    Ok(())
}

pub fn outbound_envelope(message: signalweave_core::OutboundMessage) -> Envelope {
    let delivery = protocol_delivery(message.delivery);
    let opaque = OpaquePayload {
        type_id: message.coalesce_key.map_or(1, |key| key.component),
        bytes: message.payload,
    };
    let payload = if matches!(
        delivery,
        DeliveryClass::LatestValue | DeliveryClass::UnreliableSequenced
    ) {
        MessagePayload::EntityState(opaque)
    } else {
        MessagePayload::ReliableEvent(opaque)
    };
    Envelope {
        protocol_version: PROTOCOL_VERSION,
        delivery_class: delivery,
        namespace_id: message.namespace.get(),
        session_id: message.session.get(),
        space_id: message.space.get(),
        channel_id: Some(message.channel.get()),
        entity_id: message.entity.map(signalweave_core::EntityId::get),
        space_epoch: message.space_epoch.get(),
        server_tick: 0,
        sender_sequence: message.sequence,
        correlation_id: None,
        message: payload,
    }
}

#[allow(clippy::unused_async, clippy::result_unit_err)]
pub async fn send_envelope(sender: &mpsc::Sender<Envelope>, envelope: Envelope) -> Result<(), ()> {
    sender.try_send(envelope).map_err(|_| ())
}

pub async fn send_error(
    sender: &mpsc::Sender<Envelope>,
    related: MessageKind,
    code: ProtocolErrorCode,
    message: String,
) {
    let related = if related == MessageKind::Unknown {
        MessageKind::ProtocolError
    } else {
        related
    };
    let _ = send_envelope(
        sender,
        Envelope::control(
            DeliveryClass::ReliableOrdered,
            ControlPayload::ProtocolError(ProtocolError {
                code,
                related_message_kind: related,
                message,
            }),
        ),
    )
    .await;
}

#[must_use]
pub fn core_delivery(delivery: DeliveryClass) -> Option<CoreDelivery> {
    Some(match delivery {
        DeliveryClass::ReliableOrdered => CoreDelivery::ReliableOrdered,
        DeliveryClass::ReliableUnordered => CoreDelivery::ReliableUnordered,
        DeliveryClass::LatestValue => CoreDelivery::LatestValue,
        DeliveryClass::UnreliableSequenced => CoreDelivery::UnreliableSequenced,
        DeliveryClass::BestEffortEvent => CoreDelivery::BestEffortEvent,
        DeliveryClass::Unknown => return None,
    })
}

#[must_use]
pub fn protocol_delivery(delivery: CoreDelivery) -> DeliveryClass {
    match delivery {
        CoreDelivery::ReliableOrdered => DeliveryClass::ReliableOrdered,
        CoreDelivery::ReliableUnordered => DeliveryClass::ReliableUnordered,
        CoreDelivery::LatestValue => DeliveryClass::LatestValue,
        CoreDelivery::UnreliableSequenced => DeliveryClass::UnreliableSequenced,
        CoreDelivery::BestEffortEvent => DeliveryClass::BestEffortEvent,
    }
}

#[must_use]
pub fn core_error_code(error: &CoreError) -> ProtocolErrorCode {
    match error {
        CoreError::AuthenticationRequired => ProtocolErrorCode::AuthenticationRequired,
        CoreError::NamespaceReadAccessDenied(_)
        | CoreError::NamespaceWriteAccessDenied(_)
        | CoreError::SessionReadAccessDenied(_)
        | CoreError::SessionWriteAccessDenied(_)
        | CoreError::SpaceReadAccessDenied(_)
        | CoreError::SpaceWriteAccessDenied(_)
        | CoreError::ChannelWriteAccessDenied(_)
        | CoreError::EntityNotOwned(_)
        | CoreError::AuthorityRejected(_) => ProtocolErrorCode::Unauthorized,
        CoreError::SpaceEpochMismatch { .. } => ProtocolErrorCode::StaleEpoch,
        CoreError::StaleSequence { .. } => ProtocolErrorCode::SequenceRejected,
        CoreError::PayloadTooLarge { .. } => ProtocolErrorCode::PayloadTooLarge,
        CoreError::PublishRateLimited { .. } => ProtocolErrorCode::RateLimited,
        _ => ProtocolErrorCode::Internal,
    }
}
