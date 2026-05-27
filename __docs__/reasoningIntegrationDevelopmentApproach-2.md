<h1>Reasoning Integration Development Approach - 2</h1>

***LLMAgentWrapper Implementation Approach - Spec Compliance***

> **Spec sources**:
>
> - [`reasoningIntegrationSpecs-2.md`](./reasoningIntegrationSpecs-2.md) - decision-making points 1-9, overall solution structure, key points
> - [`reasoningIntegrationSpecs-3.md`](./reasoningIntegrationSpecs-3.md) - sharpening and missing items
> - [`reasoningIntegrationSpecs-4.md`](./reasoningIntegrationSpecs-4.md) - LLMAgentWrapper design and resolved concerns
>
> **Implementation**: [`reasoningIntegrationDevelopmentApproach-1.md`](./reasoningIntegrationDevelopmentApproach-1.md)

---

**Contents**:

- [Addressed](#addressed)
- [Partially Addressed](#partially-addressed)
- [Not Addressed (Deferred by Spec)](#not-addressed-deferred-by-spec)

---

# Addressed

**Overall solution structure - structured context / structured response, with defined fallbacks**

The spec requires the engine to handle structurally invalid responses, logically invalid responses, and LLM call failures, each with a well-defined fallback. The implementation addresses all three: `FALLBACK_STRUCTURAL` (parse failure), `FALLBACK_LOGICAL` (invalid quantities, budget violations, unknown item IDs), and the runner-level `try/except` as a last-resort catch for unhandled exceptions. All three produce distinct event log entries and substitute `RuleBasedAgent` decisions before reaching `_validate_decisions`.

**Decision point 1 - Trigger condition as a configurable parameter**

`executor_trigger_every_n_ticks` is a named field in `LLMAgentWrapperConfig`, separate from `SimConfig`, and is one of the six parameters logged to MLflow at run start.

**Decision point 2 - Trigger mode and async/sync split**

The implementation correctly separates the tick-synchronous monitoring loop from the async executor thread, connected via a shared result slot. The tick loop is never blocked by LLM latency. This is the simulation-phase realisation of the PROD pattern where monitoring and execution are decoupled processes.

**Decision point 3 - Resilience measures**

Defined at two layers: the LLMAgentWrapper's pre-flight validation (structural + logical) with `RuleBasedAgent` fallback, and the runner's `try/except` as a safety net. The open question from `reasoningIntegration-2-3` (whether `_run_tick` has a try/except) is resolved by adding one.

**Decision point 6 - Push targets and pull consumers**

Resolved: the monitoring loop writes to `hist_eval_metrics`; pull consumers (LangFuse, MLflow, dashboards) read from it downstream. No direct engine-to-consumer push calls.

**Decision point 8 - Feedback latency**

Addressed via the `context_obsolescence_threshold_k` parameter: a context assembled at tick T is stale if `current_tick - T > K`, with K defaulting to the minimum lead time. This prevents the executor from acting on a context whose `PendingOrder` expected arrival ticks have already passed, which is the primary source of misleading feedback signal.

**Decision point 9 - Inter-process communication between monitoring and executor**

The shared result slot (`_result_slot`, `_executor_busy`, `_last_committed`) is the defined IPC mechanism. Monitoring pushes to the queue; the executor reads from a snapshot of it and writes back to the slot. The two sides share no other state.

**LLMAgentWrapper design - Queue message schema**

`QueueMessage` is fully defined with all required fields: `trigger_tick`, `trigger_condition_met`, `assembly_timestamp`, `obsolescence_threshold`, `context`, `sim_id`. `sim_id` and `context.tick` are explicitly included for Her Majesty Reshma the Boss's LangFuse trace attachment.

**LLMAgentWrapper design - Obsolescence condition**

Defined as tick-elapsed only (`current_tick - assembly_tick > K`). Disruption-based obsolescence explicitly excluded for now, as resolved. Default K tied to minimum lead time.

**LLMAgentWrapper design - Queue drain policy**

Drain logic runs in full regardless of `queue_size` - not short-circuited for the default of 1. Consumes to the latest non-outdated message.

**LLMAgentWrapper design - "Agent busy" policy**

On trigger: if executor busy, skip dispatch and return `_last_committed`. When result arrives, consume on the next tick's sync check before any new dispatch.

**KEY POINT 2 - Simulation-based design leaving PROD options open**

The background thread boundary is a direct analogue of the PROD decoupled-process pattern. The queue, message schema, and result slot are designed to be replaceable with a message broker and inter-service HTTP/event call without changing the surrounding logic.

---

# Partially Addressed

**Decision point 1 - MLflow: LLM call frequency and token cost logging**

`executor_trigger_every_n_ticks` is logged, which governs call frequency. However, per-call token cost logging is not included in the implementation approach - this requires the actual LLM response to carry token usage metadata, which is only available once Her Majesty Reshma the Boss's integration is in place. The parameter logging at run start is the correct first step; per-call metrics are a second step.

**Decision point 4 - Tool abstraction layer (UC functions)**

The implementation approach does not define UC read functions. These are Her Majesty Reshma the Boss's scope (UC read tools for `AgentContext` assembly: `ops_warehouse_state`, `hist_demand_actuals`, `ops_pending_orders`, `ops_cost_accumulator`, `ops_active_disruptions`). The implementation approach correctly defers this but does not note the dependency explicitly: the LLMAgentWrapper cannot be tested with a real LLM until these tools exist.

**KEY POINT 1 - Working toward PROD via UC functions**

Structurally the implementation moves in this direction (thread boundary as process boundary analogue, queue as message broker analogue), but UC function definitions for Delta table writes are not addressed. This is consistent with the simulation scope but should be noted as the next PROD-facing step after the UC read tools are in place.

---

# Not Addressed (Deferred by Spec)

**Decision point 5 - Trigger condition as a governable UC artefact**

Explicitly deferred in the LLMAgentWrapper design doc. Acceptable for simulation scope; `LLMAgentWrapperConfig` is the placeholder.

**Decision point 7 - Lakebase / shared state for context enrichment**

Explicitly deferred. For simulation, history is injected into `AgentContext` from `hist_*` tables via `agent_history_window_ticks`. No cross-run memory store is in scope.

**Decision point 1 (clarifications point 3) - Threshold for suggestion 1's meta-loop**

No threshold has been defined for when the suggestion 1 meta-loop (LLM governing its own call frequency) becomes worth implementing. This remains open and is not a blocker for the current implementation phase.