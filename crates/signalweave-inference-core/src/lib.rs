//! Inference capability, request, and provider data model.
//!
//! This crate holds only vocabulary and traits: no provider SDKs, no network clients, and no
//! dependency on `signalweave-protocol` or `signalweave-transport`. Per ADR 0009, inference
//! stays adjacent to the relay; this crate is the shared boundary the coordinator, tool
//! gateway, and providers build on without pulling inference concerns into the core.

#![forbid(unsafe_code)]

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use signalweave_core::{EntityId, PrincipalId};

/// A capability a provider advertises and a request targets, e.g. `"language.dialogue"`.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Capability(pub String);

impl Capability {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderLocality {
    Local,
    Hosted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrivacyClass {
    Public,
    Restricted,
    Confidential,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LatencyClass {
    Interactive,
    Background,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CostClass {
    Free,
    Low,
    Medium,
    High,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QualityTier {
    Basic,
    Standard,
    High,
}

/// What a provider advertises about itself before it is registered.
#[derive(Clone, Debug)]
pub struct ProviderDescriptor {
    pub capability: Capability,
    pub locality: ProviderLocality,
    pub privacy: PrivacyClass,
    pub modalities: Vec<String>,
    pub supports_streaming: bool,
    pub max_context_items: usize,
    pub max_concurrency: usize,
    pub latency_class: LatencyClass,
    pub cost_class: CostClass,
    pub quality_tier: QualityTier,
}

/// A cooperative cancellation flag shared between a request's owner and the provider running
/// it. Cheap to clone; checking it never blocks.
#[derive(Clone, Debug, Default)]
pub struct Cancellation(Arc<AtomicBool>);

impl Cancellation {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

/// One pre-assembled scoped context item handed to a provider. Providers never receive raw
/// session state; the coordinator assembles this from the requester's authorized scopes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextItem {
    pub source: String,
    pub bytes: Vec<u8>,
}

/// A bounded, deterministic-format request handed to a provider.
#[derive(Clone, Debug)]
pub struct InferenceRequest {
    pub capability: Capability,
    pub principal: PrincipalId,
    pub acting_entity: EntityId,
    pub deadline: Instant,
    pub cancellation: Cancellation,
    pub context: Vec<ContextItem>,
    pub input: Vec<u8>,
    pub streaming: bool,
}

/// A model-proposed tool invocation. Never applied directly; always evaluated by the
/// deterministic gateway in `signalweave-inference-tools`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolCallProposal {
    pub tool_id: String,
    pub tool_version: u32,
    pub arguments: Vec<u8>,
    pub expected_revision: u64,
}

/// One step of a provider's response. A provider may emit any number of these before
/// terminating with exactly one of `Completed` or `Failed`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InferenceEvent {
    Progress {
        percent: u8,
    },
    StreamChunk {
        sequence: u32,
        chunk: Vec<u8>,
        is_final: bool,
    },
    ToolCallProposed(ToolCallProposal),
    Completed {
        result: Vec<u8>,
    },
    Failed {
        reason: String,
    },
}

/// The full sequence of events from running one request to completion. The coordinator
/// checks its own deadline before publishing each event; an outcome carries no expiry
/// judgment of its own.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InferenceOutcome {
    pub events: Vec<InferenceEvent>,
}

impl InferenceOutcome {
    #[must_use]
    pub fn new(events: Vec<InferenceEvent>) -> Self {
        Self { events }
    }
}

/// A registered inference backend. Implementations range from the deterministic fake
/// provider used in tests to real local or hosted model adapters; this crate defines only
/// the boundary.
#[async_trait::async_trait]
pub trait Provider: Send + Sync {
    fn descriptor(&self) -> ProviderDescriptor;
    async fn run(&self, request: InferenceRequest) -> InferenceOutcome;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancellation_is_observed_across_clones() {
        let cancellation = Cancellation::new();
        let clone = cancellation.clone();
        assert!(!clone.is_cancelled());
        cancellation.cancel();
        assert!(clone.is_cancelled());
    }
}
