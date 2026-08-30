//! A deterministic, scripted [`Provider`] implementation.
//!
//! Runs no network calls and uses no randomness, so tests and local development get
//! reproducible inference behavior without a paid provider. The request's `input` bytes,
//! interpreted as UTF-8, select which scripted response to return. Tool IDs here must match
//! the registry in `signalweave-inference-tools`.

#![forbid(unsafe_code)]

use signalweave_inference_core::{
    CostClass, InferenceEvent, InferenceOutcome, InferenceRequest, LatencyClass, PrivacyClass,
    Provider, ProviderDescriptor, ProviderLocality, QualityTier, ToolCallProposal,
};

/// Input that triggers a read-only diagnostic tool call.
pub const TRIGGER_DIAGNOSTIC: &str = "diagnostics";
/// Input that triggers a state-changing tool call with a deliberately stale revision, to
/// exercise the gateway's staleness rejection.
pub const TRIGGER_STALE_STATUS_UPDATE: &str = "stale-status-update";
/// Input that triggers a state-changing tool call with a fresh, correct revision.
pub const TRIGGER_STATUS_UPDATE: &str = "status-update";

const DIAGNOSTIC_TOOL_ID: &str = "diagnostics.report";
const STATUS_TOOL_ID: &str = "status.set";

/// Deterministic fake provider. Never calls a real model.
#[derive(Clone, Copy, Debug, Default)]
pub struct DeterministicProvider;

#[async_trait::async_trait]
impl Provider for DeterministicProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor {
            capability: signalweave_inference_core::Capability::new("language.dialogue"),
            locality: ProviderLocality::Local,
            privacy: PrivacyClass::Public,
            modalities: vec!["text".to_owned()],
            supports_streaming: true,
            max_context_items: 16,
            max_concurrency: 4,
            latency_class: LatencyClass::Interactive,
            cost_class: CostClass::Free,
            quality_tier: QualityTier::Basic,
        }
    }

    async fn run(&self, request: InferenceRequest) -> InferenceOutcome {
        let input = String::from_utf8_lossy(&request.input);
        let events = match input.trim() {
            TRIGGER_DIAGNOSTIC => vec![
                InferenceEvent::StreamChunk {
                    sequence: 1,
                    chunk: b"checking systems...".to_vec(),
                    is_final: true,
                },
                InferenceEvent::ToolCallProposed(ToolCallProposal {
                    tool_id: DIAGNOSTIC_TOOL_ID.to_owned(),
                    tool_version: 1,
                    arguments: b"{}".to_vec(),
                    expected_revision: 0,
                }),
                InferenceEvent::Completed {
                    result: b"all systems nominal".to_vec(),
                },
            ],
            TRIGGER_STALE_STATUS_UPDATE => vec![
                InferenceEvent::ToolCallProposed(ToolCallProposal {
                    tool_id: STATUS_TOOL_ID.to_owned(),
                    tool_version: 1,
                    arguments: b"{\"level\":\"critical\"}".to_vec(),
                    expected_revision: 0,
                }),
                InferenceEvent::Completed {
                    result: b"attempted status update".to_vec(),
                },
            ],
            TRIGGER_STATUS_UPDATE => vec![
                InferenceEvent::ToolCallProposed(ToolCallProposal {
                    tool_id: STATUS_TOOL_ID.to_owned(),
                    tool_version: 1,
                    arguments: b"{\"level\":\"nominal\"}".to_vec(),
                    expected_revision: 1,
                }),
                InferenceEvent::Completed {
                    result: b"status updated".to_vec(),
                },
            ],
            other => vec![InferenceEvent::Completed {
                result: format!("heard: {other}").into_bytes(),
            }],
        };
        InferenceOutcome::new(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use signalweave_core::{EntityId, PrincipalId};
    use signalweave_inference_core::{Cancellation, Capability};
    use std::time::{Duration, Instant};

    fn request(input: &str) -> InferenceRequest {
        InferenceRequest {
            capability: Capability::new("language.dialogue"),
            principal: PrincipalId::new(1),
            acting_entity: EntityId::new(1),
            deadline: Instant::now() + Duration::from_secs(5),
            cancellation: Cancellation::new(),
            context: Vec::new(),
            input: input.as_bytes().to_vec(),
            streaming: true,
        }
    }

    #[tokio::test]
    async fn diagnostic_trigger_proposes_the_read_only_tool() {
        let outcome = DeterministicProvider.run(request(TRIGGER_DIAGNOSTIC)).await;
        assert!(outcome.events.iter().any(|event| matches!(
            event,
            InferenceEvent::ToolCallProposed(proposal) if proposal.tool_id == DIAGNOSTIC_TOOL_ID
        )));
        assert!(matches!(
            outcome.events.last(),
            Some(InferenceEvent::Completed { .. })
        ));
    }

    #[tokio::test]
    async fn stale_trigger_proposes_a_stale_revision() {
        let outcome = DeterministicProvider
            .run(request(TRIGGER_STALE_STATUS_UPDATE))
            .await;
        assert!(outcome.events.iter().any(|event| matches!(
            event,
            InferenceEvent::ToolCallProposed(proposal)
                if proposal.tool_id == STATUS_TOOL_ID && proposal.expected_revision == 0
        )));
    }

    #[tokio::test]
    async fn unrecognized_input_completes_with_an_echo() {
        let outcome = DeterministicProvider.run(request("hello")).await;
        assert_eq!(
            outcome.events,
            vec![InferenceEvent::Completed {
                result: b"heard: hello".to_vec()
            }]
        );
    }
}
