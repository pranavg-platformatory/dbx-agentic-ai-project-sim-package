<h1>Reasoning Integration Specifications - 4</h1>

***LLM Agent Wrapper - Design & Feedback***

> **Context**: [`reasoningIntegrationSpecs-3.md`](./reasoningIntegrationSpecs-3.md)
>
> This document captures the LLMAgentWrapper design and the corresponding feedback, including how open concerns from [`reasoningIntegrationSpecs-2.md`](./reasoningIntegrationSpecs-2.md) are addressed or not addressed, and which concerns fall under Pranav's vs. Her Majesty Reshma the Boss's scope.

---

**Contents**:

- [LLMAgentWrapper Design](#llmagentwrapper-design)
  - [Core Structure](#core-structure)
  - [Async/Sync Split](#asyncsync-split)
- [Feedback](#feedback)
  - [Against Open Concerns](#against-open-concerns)
  - [Design-Specific Observations](#design-specific-observations)
- [Scope Summary](#scope-summary)

---

# LLMAgentWrapper Design

## Core Structure

```
LLMAgentWrapper: monitoring/tick  --> AgentContext encapsulation --> insert to queue

LLMAgentWrapper: executor logic: check trigger condition
                 ^
                 |
--> No => wait --+
--> Yes => retrieve the last non-outdated AgentContext object in queue
           (AgentContext obsolescence conditions have to be defined)
    --> LLM
         | --> No response --(fallback to)--> Rule-based agent
         |                                    |
         |      +-----------------------------+
         |      |
         v      v
         Response --> reorder function calling
```

The evaluation tool calling will be called by a separate loop; for now, it is part of the monitoring/AgentContext encapsulation loop, clearly specified via comments.

---

## Async/Sync Split

**Consideration**: How much time will the executor take?

In PROD, since monitoring and executor are decoupled and async, the executor can take as long as it needs without disrupting the system. To allow for this in the simulation, the executor must be a separate system, and the LLM wrapper must:

1. Call the executor
2. Identify the "agent" as busy until it gets a response (every tick)

Hence, the executor is split:

1. **Async processing** in external system
2. **Sync tick-based environment interaction** via LLM wrapper

In effect, the executor is async as a decision-maker.

---

# Feedback

## Against Open Concerns

**Point 1 - Trigger condition parameters unnamed** *(resolved - Pranav's scope)*

Partially addressed. The design establishes per-tick monitoring with configurable executor invocation, but the trigger condition's structure is still not named. Open questions: is it purely tick-based (every N ticks), or can it also fire on state conditions (stock below threshold, disruption active)? To be resolved under "define parameters and parameter retrieval."

> **Resolution**: Purely tick-based. The executor is invoked every N ticks, where N is a configurable parameter. No state-condition triggers for now.

**Point 4 - UC read tools missing from UC function list** *(Her Majesty Reshma the Boss's scope)*

Not addressed here, correctly so. To communicate to her: read tools for `AgentContext` assembly (`ops_warehouse_state`, `hist_demand_actuals`, `ops_pending_orders`, `ops_cost_accumulator`, `ops_active_disruptions`) are the first UC functions to implement - ahead of write functions - since the wrapper cannot be tested end-to-end without them.

**Point 6 - Pull consumers' source not stated** *(shared scope)*

Partially addressed: evaluation tool calling is placed in the monitoring/context encapsulation loop, implying the monitoring loop writes to whatever pull consumers read from. Alignment needed with Her Majesty Reshma the Boss on what the monitoring loop writes and where, since Pranav owns that loop and she owns the LangFuse/MLflow instrumentation downstream of it.

> **Resolution**: The monitoring loop writes evaluation metrics to a dedicated table - `hist_eval_metrics` - and nowhere else. It has no direct dependency on LangFuse, MLflow, or any reasoning-side tooling. Pull consumers (LangFuse, MLflow, dashboards) read from this table on their own schedule, downstream of the monitoring loop entirely. The evaluation metrics are queryable by the reasoning system via UC read functions over `hist_eval_metrics`, consistent with the tool abstraction layer. This achieves maximum decoupling while keeping evaluation data accessible to the LLM when needed.

**Points 7 & 8 - Deferred without minimum viable resolution** *(Point 8 resolved - Pranav's scope; Point 7 remains deferred)*

Point 8 (feedback latency): the `AgentContext` obsolescence condition concept is the right place to handle the lag between decision and effect. The obsolescence condition should encode: a context is outdated if the tick it was assembled for is more than N ticks behind the current tick, where N accounts for lead time. To be defined as part of the obsolescence condition design.

> **Resolution**: Obsolescence defined purely in terms of ticks elapsed: a context assembled at tick T is outdated if `current_tick - T > K`, where K is a configurable obsolescence threshold. Default value of K should be set relative to the minimum lead time in the simulation config - a context older than the shortest lead time is stale by definition. Additionally, a `queue_size` parameter is introduced, defaulting to 1, meaning only the latest context is ever in the queue. With queue size 1, the drain logic (consume until earliest non-outdated context is found, then process) is a no-op by design - the general drain logic should still be implemented and commented accordingly, for when queue size becomes configurable.

Point 7 (Lakebase/shared state): remains deferred. Acceptable for now.

**Message queue message schema** *(resolved - Pranav's scope)*

The design names the queue and describes what flows through it (AgentContext objects) but not the message envelope. Minimum required fields: trigger tick, trigger condition met, assembly timestamp, obsolescence threshold. Must be defined before the queue is implemented.

> **Resolution**: Message schema fields: `trigger_tick`, `trigger_condition_met`, `assembly_timestamp`, `obsolescence_threshold`, `AgentContext` object. Sufficient for current scope.

**Point 5 - Trigger condition governance** *(still open - deferred, acceptable)*

**Job schedule vs. tick loop** *(closed)*

Directly resolved by the async/sync split. The monitoring loop remains tick-synchronous; the executor is async. The tick loop is retained as the scheduler for monitoring; the executor runs independently.

**LangFuse trace structure** *(Her Majesty Reshma the Boss's scope)*

Not addressed here. To communicate to her: `sim_id`, `tick`, and trigger condition metadata originate from the monitoring loop and must be included in the queue message so she can attach them to LangFuse traces on the executor side without querying for them separately.

---

## Design-Specific Observations

**The obsolescence condition is the most important thing to define next.**

The queue can accumulate multiple `AgentContext` objects between executor invocations. "Retrieve the last non-outdated object" requires a precise definition of outdatedness. Minimum: a context assembled at tick T is outdated if the current tick is T + K for some configurable K. Additional consideration: is a context outdated if a new disruption has activated since assembly, even if the tick delta is small? This is a design decision with direct impact on decision quality.

> **Resolution**: Obsolescence defined purely by ticks elapsed (`current_tick - T > K`). Default K tied to minimum lead time in sim config. Disruption-based obsolescence not included for now. Queue size defaults to 1, so in practice only the latest context is ever evaluated.

**The "agent busy" behaviour needs a defined policy.**

When the executor is processing and new ticks arrive, the monitoring loop continues assembling and queuing contexts. When the executor finishes, does it process the most recent non-outdated context, or drain the queue? Draining is almost certainly wrong (stale decisions). Processing only the most recent non-outdated context is the right call - but this should be stated explicitly in the design.

> **Resolution**: Drain logic consumes until the earliest non-outdated context is found, then processes that one. With queue size defaulting to 1, this is trivially always the latest context. General drain logic should be implemented and commented for future configurability.

**The fallback path should cover logical invalidity, not just structural failure.**

The current design triggers fallback on no-response/timeout. It should also trigger on logically invalid responses: quantities outside `[min_order_qty, max_order_qty]`, budget violations, unknown item IDs. Structurally valid but semantically wrong responses are a distinct failure mode and should be an explicit case in the event log, alongside timeout and parse failure.

> **Resolution**: Fallback explicitly covers both structural invalidity (parse failure, bad response format) and logical invalidity (out-of-range quantities, budget violations, unknown item IDs). Each produces a distinct event log entry - e.g. `FALLBACK_STRUCTURAL` and `FALLBACK_LOGICAL` - to preserve diagnostic signal in post-run analysis.

**The evaluation loop placement is correct for now.**

Embedding it in the monitoring loop with clear comments is pragmatic. Annotation should be explicit enough that the evaluation calls are trivially extractable into a separate loop later - this will matter when Her Majesty Reshma the Boss instruments LangFuse around them.

---

# Scope Summary

| Concern | Owner | Status |
|---|---|---|
| Trigger condition parameter structure | Pranav | Resolved |
| Message queue message schema | Pranav | Resolved |
| AgentContext obsolescence condition | Pranav | Resolved |
| Queue size parameter | Pranav | Resolved |
| "Agent busy" queue drain policy | Pranav | Resolved |
| Fallback for structural invalidity | Pranav | Resolved |
| Fallback for logical invalidity | Pranav | Resolved |
| UC read tool definitions and ordering | Her Majesty Reshma the Boss | Open |
| LangFuse trace structure and hierarchy | Her Majesty Reshma the Boss | Open |
| Monitoring loop write targets (alignment) | Both | Resolved |
| Lakebase/shared state | Deferred | - |
| Trigger condition governance (UC) | Deferred | - |