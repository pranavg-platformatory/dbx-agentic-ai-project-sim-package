<h1>Reasoning Integration Development Approach - 1</h1>

***LLMAgentWrapper - Implementation Approach***

> **Context**:
> 
> - [`reasoningIntegrationSpecs-2.md`](./reasoningIntegrationSpecs-2.md)
> - [`reasoningIntegrationSpecs-3.md`](./reasoningIntegrationSpecs-3.md)
> - [`reasoningIntegrationSpecs-4.md`](./reasoningIntegrationSpecs-4.md)

---

**Contents**:

- [1. Simulation Code - What Gets Touched and How](#1-simulation-code---what-gets-touched-and-how)
  - [Untouched](#untouched)
  - [`warehouse_sim/agent/` - New files added](#warehouse_simagent---new-files-added)
  - [`warehouse_sim/engine/runner.py` - One targeted change](#warehouse_simenginerunnerpy---one-targeted-change)
  - [`warehouse_sim/agent/rule_based.py` - Now needed](#warehouse_simagentrule_basedpy---now-needed)
  - [`warehouse_sim/data_store/` - New table: `hist_eval_metrics`](#warehouse_simdata_store---new-table-hist_eval_metrics)
- [2. External Elements and Systems](#2-external-elements-and-systems)
  - [LLMAgentWrapper Config (`LLMAgentWrapperConfig`)](#llm_agent_wrapper-config-llm_agent_wrapperconfig)
  - [In-Process Queue](#in-process-queue)
  - [Async Executor and Shared Result Slot](#async-executor-and-shared-result-slot)
  - [MLflow - Run-level parameter logging](#mlflow---run-level-parameter-logging)
  - [Event log - Two new event types](#event-log---two-new-event-types)
- [3. Expected Flow - Stub Agent Phase](#3-expected-flow---stub-agent-phase)
  - [Structural overview](#structural-overview)
  - [The stub agent](#the-stub-agent)
  - [What this phase validates end-to-end](#what-this-phase-validates-end-to-end)

---

# 1. Simulation Code - What Gets Touched and How

## Untouched

The core simulation - everything built in Stages 1 through 7 - is not modified. The engine, data models, event logger, pattern sampler, and visualisation layer remain as-is. The agent contract (`BaseAgent`, `AgentContext`, `ReorderDecision` in `agent/base.py`) is the defined boundary; the LLMAgentWrapper sits outside it.

## `warehouse_sim/agent/` - New files added

This sub-package already exists with `base.py`, `hold_agent.py`, and `reorder_agent.py`. The LLMAgentWrapper is added here as a new concrete `BaseAgent` subclass:

```
warehouse_sim/agent/
├── base.py            (unchanged)
├── hold_agent.py      (unchanged)
├── reorder_agent.py   (unchanged)
├── llm_agent_wrapper.py            <- NEW: LLMAgentWrapper(BaseAgent)
└── llm_agent_wrapper_config.py     <- NEW: LLMAgentWrapperConfig (Pydantic model for LLMAgentWrapper parameters)
```

`LLMAgentWrapper` implements `decide(context: AgentContext) -> list[ReorderDecision]` - the same contract as any other agent. From the runner's perspective, the LLMAgentWrapper is just another `BaseAgent`. The runner is never aware of the monitoring loop, queue, executor thread, or LLM call inside it.

> **Based on comparison with spec (see: `reasoningIntegration-3-1`)**: UC read tool definitions (`ops_warehouse_state`, `hist_demand_actuals`, `ops_pending_orders`, `ops_cost_accumulator`, `ops_active_disruptions`) are Her Majesty Reshma the Boss's scope and are not included here. However, this is an explicit dependency: the LLMAgentWrapper cannot be tested with a real LLM until those UC read tools exist. This should be tracked as a handoff point between the stub phase and the live LLM phase.

## `warehouse_sim/engine/runner.py` - One targeted change

The open question in `reasoningIntegration-2-3` is confirmed to require a resolution before implementation: `_run_tick` currently has no `try/except` around `agent.decide()`, meaning an unhandled exception from the agent halts the simulation.

The runner needs a minimal resilience wrap around the agent call:

```python
try:
    decisions = self._agent.decide(context)
except Exception as e:
    # log AGENT_ERROR event; halt or substitute hold decisions
    ...
```

This is a **narrow, targeted change** - a single try/except block around one call site, with a defined fallback (log event, substitute hold decisions for all items). It does not touch tick sequencing, state writes, or any other runner logic. The LLMAgentWrapper's own internal validation still sits *before* this - the runner's wrap is the last-resort safety net, as per the design.

## `warehouse_sim/agent/rule_based.py` - Now needed

The `RuleBasedAgent` was deferred at Stage 6. It is now a dependency: the LLMAgentWrapper's fallback path (for both `FALLBACK_STRUCTURAL` and `FALLBACK_LOGICAL` cases) calls a rule-based agent rather than defaulting to hold-all. This needs to be a proper, importable `RuleBasedAgent(BaseAgent)` - not an inline stub - so the fallback path is testable independently of the LLMAgentWrapper.

**Rule**: reorder when `stock_on_hand < reorder_point`, quantity = `min_order_qty`. This is the minimum viable rule; it must use the shared `PatternSampler` RNG instance if it introduces any stochastic element (reproducibility requirement FR-07).

## `warehouse_sim/data_store/` - New table: `hist_eval_metrics`

A new append-only Delta table written by the monitoring loop. Schema to be defined, but minimum fields:

| Column | Type | Description |
|---|---|---|
| `sim_id` | string FK | Simulation run |
| `tick` | integer | Tick at which metrics were evaluated |
| `item_id` | string FK | Item evaluated; null for run-level metrics |
| `metric_name` | string | e.g. `stockout_rate`, `holding_cost_delta` |
| `metric_value` | float | Computed value |
| `logged_at` | datetime | Wall-clock write time |

This table is owned by the monitoring loop (Pranav's scope). Pull consumers - LangFuse, MLflow, dashboards - read from it downstream. No engine-to-consumer push calls.

---

# 2. External Elements and Systems

## LLMAgentWrapper Config (`LLMAgentWrapperConfig`)

A new Pydantic model holding all LLMAgentWrapper-specific parameters, separate from `SimConfig`. Kept separate because these parameters govern the wrapper's behaviour, not the simulation's behaviour - they must be independently configurable and logged independently.

| Parameter | Default | Notes |
|---|---|---|
| `executor_trigger_every_n_ticks` | configurable | Primary trigger condition lever |
| `context_obsolescence_threshold_k` | `min_lead_time` | Stale context cutoff in ticks |
| `queue_size` | 1 | Default; drain logic implemented in full for all sizes |

## In-Process Queue

The queue accumulates `QueueMessage` objects produced by the monitoring loop each tick. It is an **in-process `collections.deque`**, not an external message broker - appropriate for simulation, where the async boundary is within the same process.

Each message in the queue is a plain dataclass:

```python
@dataclass
class QueueMessage:
    trigger_tick: int
    trigger_condition_met: bool
    assembly_timestamp: datetime
    obsolescence_threshold: int   # K
    context: AgentContext
    sim_id: str                   # for LangFuse trace attachment (Her Majesty Reshma the Boss's use)
```

`sim_id` and `tick` (available from `context.tick`) are included so that Her Majesty Reshma the Boss can attach them to LangFuse traces on the executor side without an additional query.

The queue is sized by `queue_size` (default 1). The executor always drains to the latest non-outdated message regardless of queue size - the drain logic must be implemented in full and not short-circuited for the `queue_size=1` case, so that larger queue sizes work without code changes.

## Async Executor and Shared Result Slot

> **Added design component**: The executor runs as a **background thread**, not inline in `decide()`. This correctly reflects the spec's async/sync split: the monitoring loop and trigger check are tick-synchronous; the executor (LLM call + validation) runs independently and can take as long as it needs without blocking the tick loop. In simulation, "separate system" is implemented as a background thread within the same process - the simplest representation of the async boundary that is honest about the decoupling.

The executor thread writes its result to a **shared result slot** on the `LLMAgentWrapper` instance. The sync side checks this slot each tick. Three shared state attributes:

```python
self._result_slot: ExecutorResult | None = None   # written by executor thread on completion
self._executor_busy: bool = False                  # True from dispatch until slot is written
self._last_committed: list[ReorderDecision] | None = None  # last valid result consumed by decide()
```

The result written to the slot:

```python
@dataclass
class ExecutorResult:
    decisions: list[ReorderDecision]
    produced_at_tick: int
    fallback_used: bool
    fallback_type: str | None   # "FALLBACK_STRUCTURAL" | "FALLBACK_LOGICAL" | None
```

On each call to `decide()`, the sync side:

1. Checks `_result_slot` - if populated, consumes it (updates `_last_committed`, clears slot, marks executor not busy)
2. Assembles `QueueMessage` and pushes to queue; writes `hist_eval_metrics`
3. Checks trigger condition - if met and executor not busy, snapshots the queue and dispatches a new executor thread
4. Returns `_last_committed` (hold-all if none yet committed)

## MLflow - Run-level parameter logging

MLflow is used to log LLMAgentWrapper parameters per simulation run. This is called once at run start, not per tick.

Parameters logged:

| Parameter | Source |
|---|---|
| `agent_history_window_ticks` | `SimConfig` |
| `executor_trigger_every_n_ticks` | `LLMAgentWrapperConfig` |
| `context_obsolescence_threshold_k` | `LLMAgentWrapperConfig` |
| `queue_size` | `LLMAgentWrapperConfig` |
| `agent_version` | `BaseAgent.agent_version()` |
| `random_seed` | `SimConfig` |

No per-tick MLflow calls at this stage. Per-tick tracing (LangFuse) is Her Majesty Reshma the Boss's scope.

> **Based on comparison with spec (see: `reasoningIntegration-3-1`)**: The spec requires LLM call frequency and per-call token cost to be logged per simulation run. Call frequency is covered by `executor_trigger_every_n_ticks`. Token cost logging is not included here - it requires the actual LLM response to carry token usage metadata, which is only available once Her Majesty Reshma the Boss's integration is in place. This should be added as a named MLflow metric at that point, not retrofitted later.

## Event log - Two new event types

The existing `event_log` table gains two new `event_type` values, written by the LLMAgentWrapper:

| Event Type | Fired When | Key Payload Fields |
|---|---|---|
| `FALLBACK_STRUCTURAL` | LLM response fails to parse or has wrong format | `tick`, `item_id`, `raw_response`, `error` |
| `FALLBACK_LOGICAL` | LLM response is structurally valid but logically invalid | `tick`, `item_id`, `violation_type`, `offending_value` |

Both cases result in rule-based decisions being substituted. Both are logged before decisions reach the runner, so `_validate_decisions` never sees an invalid response.

> **Based on comparison with spec (see: `reasoningIntegration-3-1`)**: The spec's KEY POINT 1 calls for UC functions to handle Delta table writes, moving the implementation closer to PROD level. The LLMAgentWrapper's async boundary (background thread → shared result slot) is structurally analogous to the PROD decoupled-process pattern, but UC write function definitions are not part of this implementation phase. This is the natural next PROD-facing step once UC read tools are in place and the stub phase is complete.

---

# 3. Expected Flow - Stub Agent Phase

The stub phase replaces the LLM call with a `StubLLMAgent` that returns predictable, valid decisions. This allows the full LLMAgentWrapper machinery - monitoring loop, queue, executor thread, shared result slot, obsolescence check, fallback path, MLflow logging, `hist_eval_metrics` writes - to be tested end-to-end before Her Majesty Reshma the Boss's LLM integration is ready.

## Structural overview

> **Added design component**: The executor is shown as a separate async thread communicating back via `_result_slot`. The sync `decide()` call never blocks waiting for it - it returns `_last_committed` immediately and picks up the result on a future tick. This matches the spec's async/sync split and means the runner tick loop is never stalled by LLM latency.

```
SimRunner (unchanged)
    |
    | agent.decide(context)              <- same call as always; never blocks
    v
LLMAgentWrapper.decide(context)          <- sync, called every tick
    |
    +-- [1] check _result_slot
    |       |
    |       +--> populated => consume: update _last_committed, clear slot, mark executor idle
    |
    +-- [2] monitoring loop
    |       |
    |       +--> assemble QueueMessage
    |       +--> push to queue (deque, capped at queue_size)
    |       +--> write hist_eval_metrics (evaluation tool calls)
    |
    +-- [3] trigger check
    |       |
    |       +--> condition not met OR executor busy  =>  skip dispatch
    |       |
    |       +--> condition met AND executor idle
    |               |
    |               +--> snapshot queue, dispatch executor thread, mark executor busy
    |
    +-- [4] return _last_committed (hold-all if none yet)

                    ...  (async, in background thread)  ...

    EXECUTOR THREAD
        |
        +--> drain queue snapshot to latest non-outdated QueueMessage
        |       (drain logic always runs in full, regardless of queue_size)
        |
        +--> [LLM call]  <-- STUB in this phase
        |       |
        |       +--> StubLLMAgent.respond(context) -> structured response
        |
        +--> [pre-flight validation]
        |       |
        |       +--> structural check  -> FALLBACK_STRUCTURAL if fail
        |       +--> logical check     -> FALLBACK_LOGICAL if fail
        |       +--> on fallback: RuleBasedAgent.decide(context)
        |
        +--> write ExecutorResult to _result_slot, mark executor idle
```

## The stub agent

`StubLLMAgent` is not a `BaseAgent` subclass - it lives inside the LLMAgentWrapper, standing in for the HTTP call to the LLM. It should cover at minimum three cases, each exercising a different code path:

| Stub mode | What it returns | Code path exercised |
|---|---|---|
| `valid` | Correctly structured, logically valid decisions | Happy path through to runner |
| `structural_fail` | Malformed / unparseable response | `FALLBACK_STRUCTURAL` → `RuleBasedAgent` |
| `logical_fail` | Parsed but with invalid quantities or budget violation | `FALLBACK_LOGICAL` → `RuleBasedAgent` |

Stub mode is a parameter in `LLMAgentWrapperConfig`. All three modes must be tested before the stub is retired.

## What this phase validates end-to-end

- The LLMAgentWrapper satisfies the `BaseAgent` contract and the runner runs without modification (beyond the resilience wrap)
- The monitoring loop assembles and queues `QueueMessage` objects every tick
- The executor thread is dispatched on the correct ticks per `executor_trigger_every_n_ticks` and only when not already busy
- `_result_slot` is consumed correctly on the tick after the executor completes, updating `_last_committed`
- Obsolescence check correctly discards contexts older than K ticks (testable by setting a short K and a slow trigger)
- Both fallback paths fire, produce the correct event log entries, and return valid decisions to the runner
- `hist_eval_metrics` receives rows each tick from the monitoring loop
- MLflow logs the correct parameters at run start
- The `RuleBasedAgent` fallback uses the shared RNG (reproducibility check: two runs with the same seed produce identical decisions when the stub is set to always fail)
- `sim_id` and `tick` are present in every `QueueMessage` (verified before Her Majesty Reshma the Boss attaches LangFuse traces)