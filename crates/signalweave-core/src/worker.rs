use std::collections::VecDeque;

use crate::{
    Authenticator, CleanupSummary, ConnectionId, CoreError, Credentials, EntityId, OutboundMessage,
    PrincipalId, PublishOutcome, PublishRequest, SessionKey, SessionSnapshot, SignalweaveCore,
    SpaceEpoch, SpaceKey,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    TransportConnected,
    Authenticate {
        connection: ConnectionId,
        credentials: Credentials,
    },
    JoinSession {
        connection: ConnectionId,
        session: SessionKey,
    },
    LeaveSession {
        connection: ConnectionId,
        session: SessionKey,
    },
    Subscribe {
        connection: ConnectionId,
        space: SpaceKey,
    },
    Unsubscribe {
        connection: ConnectionId,
        space: SpaceKey,
    },
    SpawnEntity {
        connection: ConnectionId,
        space: SpaceKey,
        epoch: SpaceEpoch,
    },
    RemoveEntity {
        connection: ConnectionId,
        session: SessionKey,
        entity: EntityId,
    },
    Publish(PublishRequest),
    Snapshot {
        connection: ConnectionId,
        session: SessionKey,
    },
    DrainOutbound {
        connection: ConnectionId,
    },
    TransportLost {
        connection: ConnectionId,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum CommandResult {
    Connected(ConnectionId),
    Authenticated(PrincipalId),
    Joined,
    Left(CleanupSummary),
    Subscribed,
    Unsubscribed(CleanupSummary),
    EntitySpawned(EntityId),
    EntityRemoved(CleanupSummary),
    Published(PublishOutcome),
    Snapshot(SessionSnapshot),
    Outbound(Vec<OutboundMessage>),
    Disconnected(CleanupSummary),
}

pub struct TransportIndependentWorker<A> {
    core: SignalweaveCore<A>,
}

impl<A: Authenticator> TransportIndependentWorker<A> {
    #[must_use]
    pub const fn new(core: SignalweaveCore<A>) -> Self {
        Self { core }
    }

    #[must_use]
    pub const fn core(&self) -> &SignalweaveCore<A> {
        &self.core
    }

    #[must_use]
    pub const fn core_mut(&mut self) -> &mut SignalweaveCore<A> {
        &mut self.core
    }

    pub fn handle(&mut self, command: Command) -> Result<CommandResult, CoreError> {
        match command {
            Command::TransportConnected => self
                .core
                .transport_connected()
                .map(CommandResult::Connected),
            Command::Authenticate {
                connection,
                credentials,
            } => self
                .core
                .authenticate(connection, &credentials)
                .map(CommandResult::Authenticated),
            Command::JoinSession {
                connection,
                session,
            } => self
                .core
                .join_session(connection, session)
                .map(|()| CommandResult::Joined),
            Command::LeaveSession {
                connection,
                session,
            } => self
                .core
                .leave_session(connection, session)
                .map(CommandResult::Left),
            Command::Subscribe { connection, space } => self
                .core
                .subscribe(connection, space)
                .map(|()| CommandResult::Subscribed),
            Command::Unsubscribe { connection, space } => self
                .core
                .unsubscribe(connection, space)
                .map(CommandResult::Unsubscribed),
            Command::SpawnEntity {
                connection,
                space,
                epoch,
            } => self
                .core
                .spawn_entity(connection, space, epoch)
                .map(CommandResult::EntitySpawned),
            Command::RemoveEntity {
                connection,
                session,
                entity,
            } => self
                .core
                .remove_entity(connection, session, entity)
                .map(CommandResult::EntityRemoved),
            Command::Publish(request) => self.core.publish(request).map(CommandResult::Published),
            Command::Snapshot {
                connection,
                session,
            } => self
                .core
                .snapshot(connection, session)
                .map(CommandResult::Snapshot),
            Command::DrainOutbound { connection } => self
                .core
                .drain_outbound(connection)
                .map(CommandResult::Outbound),
            Command::TransportLost { connection } => self
                .core
                .transport_lost(connection)
                .map(CommandResult::Disconnected),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HarnessError {
    ZeroCapacity,
    Full,
}

pub struct WorkerHarness<A> {
    worker: TransportIndependentWorker<A>,
    capacity: usize,
    commands: VecDeque<Command>,
}

impl<A: Authenticator> WorkerHarness<A> {
    pub fn new(
        worker: TransportIndependentWorker<A>,
        capacity: usize,
    ) -> Result<Self, HarnessError> {
        if capacity == 0 {
            return Err(HarnessError::ZeroCapacity);
        }
        Ok(Self {
            worker,
            capacity,
            commands: VecDeque::with_capacity(capacity),
        })
    }

    pub fn submit(&mut self, command: Command) -> Result<(), HarnessError> {
        if self.commands.len() == self.capacity {
            return Err(HarnessError::Full);
        }
        self.commands.push_back(command);
        Ok(())
    }

    pub fn step(&mut self) -> Option<Result<CommandResult, CoreError>> {
        self.commands
            .pop_front()
            .map(|command| self.worker.handle(command))
    }

    pub fn run_pending(&mut self) -> Vec<Result<CommandResult, CoreError>> {
        let mut outcomes = Vec::with_capacity(self.commands.len());
        while let Some(outcome) = self.step() {
            outcomes.push(outcome);
        }
        outcomes
    }

    #[must_use]
    pub const fn worker(&self) -> &TransportIndependentWorker<A> {
        &self.worker
    }

    #[must_use]
    pub const fn worker_mut(&mut self) -> &mut TransportIndependentWorker<A> {
        &mut self.worker
    }

    #[must_use]
    pub fn pending_len(&self) -> usize {
        self.commands.len()
    }
}
