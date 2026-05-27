<h1>Reasoning Integration Specifications - 5</h1>

***LLMAgentWrapper Build Considerations***

> **Context files reviewed**:
> 
> - `warehouse_sim/agent/base.py` - `AgentContext`, `ReorderDecision`, `BaseAgent` definitions
> - `warehouse_sim/engine/runner.py` - `SimRunner` tick loop and context assembly
> - `warehouse_sim/world/patterns.py` - `PatternSampler` and RNG model

---

**Contents**:

- [What the Code Confirms](#what-the-code-confirms)
  - [AgentContext Schema](#agentcontext-schema)
  - [`agent_history_window_ticks`](#agent_history_window_ticks)
  - [Reproducibility (FR-07)](#reproducibility-fr-07)
  - [Existing Validation in Runner](#existing-validation-in-runner)
- [Considerations for the LLMAgentWrapper](#considerations-for-the-llm_agent_wrapper)
  - [1. Fallback Layer Must Sit Before Runner Validation](#1-fallback-layer-must-sit-before-runner-validation)
  - [2. Stale PendingOrders in Queued Contexts](#2-stale-pendingorders-in-queued-contexts)
  - [3. Open Question - Runner Resilience](#3-open-question---runner-resilience)
- [Parameter Log (MLflow)](#parameter-log-mlflow)

---

# What the Code Confirms

## AgentContext Schema
All six building blocks are frozen dataclasses - fully clear, no ambiguity:

| Component | Key Fields |
|---|---|
| `ItemState` | `stock_on_hand`, `stock_in_transit`, `expected_arrivals_next_tick`, `reorder_point`, `min_order_qty`, `max_order_qty` |
| `PendingOrder` | `order_id`, `item_id`, `supplier_id`, `order_tick`, `expected_arrival_tick`, `order_qty` |
| `DemandRecord` | `tick`, `item_id`, `raw_demand`, `disrupted_demand`, `fulfilled`, `unmet` |
| `ActiveDisruption` | `disruption_id`, `item_id`, `disruption_type`, `effective_magnitude`, `is_active_this_tick` |
| `CostSnapshot` | all cumulative cost components, `remaining_budget` |
| `AgentContext` | all of the above, plus `sim_id`, `tick`, `remaining_budget` |

The context is rich enough for full LLM reasoning: stock state, in-transit state, next-tick expected arrivals, windowed demand history, active disruptions, cumulative costs, and budget.

## `agent_history_window_ticks`
Already wired into `_build_agent_context` via `self._config`. This is the primary lever controlling cross-tick reasoning depth and token cost. It is the first parameter to externalise and log per MLflow run.

## Reproducibility (FR-07)
`PatternSampler` uses a single seeded numpy RNG passed through every stochastic draw in a fixed global order controlled by `runner.py`. **The LLMAgentWrapper must not introduce stochastic draws outside this RNG.** If the rule-based fallback involves any randomness, it must use the same `PatternSampler` instance - not its own RNG.

## Existing Validation in Runner
`_validate_decisions` in the runner enforces:
- One decision per item (raises `ValueError` on missing items)
- `min_order_qty <= order_qty <= max_order_qty` for reorders (raises `ValueError` on violation)

This overlaps with the LLMAgentWrapper's planned logical invalidity fallback. The distinction to preserve: **the LLMAgentWrapper must intercept and handle invalidity before passing decisions to the runner**, so fallback to rule-based happens gracefully rather than the runner raising a `ValueError` mid-tick and halting the simulation.

---

# Considerations for the LLMAgentWrapper

## 1. Fallback Layer Must Sit Before Runner Validation
The runner's `_validate_decisions` raises `ValueError` with no recovery path - it halts the tick. The LLMAgentWrapper's validation and fallback must therefore be a pre-flight check: validate the LLM's response structurally and logically, and substitute rule-based decisions before `_run_tick` ever calls `_validate_decisions`. The runner's own validation then serves as a redundant safety net, not the primary fallback trigger.

## 2. Stale PendingOrders in Queued Contexts
`_build_agent_context` calls `fetch_pending_orders` at step [4] of each tick (it is also called at step [1]). When the LLMAgentWrapper queues a context assembled at tick T for execution at tick T+N, the `PendingOrder` objects in that context will be stale - orders may have arrived, new orders may have been placed. Specifically, `expected_arrival_tick` values that are now in the past are particularly misleading to an LLM reasoner. **The obsolescence threshold K should be set conservatively relative to the minimum lead time** - a context containing `PendingOrder`s whose `expected_arrival_tick` has already passed is unreliable by definition.

## 3. Open Question - Runner Resilience
`_run_tick` currently has no try/except around `self._agent.decide(context)`. A bad agent response raises `ValueError` and halts the simulation. This needs to be confirmed, as it determines exactly where the LLMAgentWrapper's validation layer must sit:

- **If no try/except exists**: The LLMAgentWrapper must catch all exceptions from the LLM call and handle them entirely before returning decisions to the runner. The runner must never see an exception from the agent layer.
- **If a try/except exists with recovery**: The LLMAgentWrapper's fallback can be more loosely coupled, relying on the runner's recovery path as a backstop.

Current assumption: no try/except exists. LLMAgentWrapper design proceeds on this basis.

---

# Parameter Log (MLflow)

The following parameters should be logged per simulation run from the outset:

| Parameter | Source | Notes |
|---|---|---|
| `agent_history_window_ticks` | `SimConfig` | Primary token cost lever |
| `executor_trigger_every_n_ticks` | LLMAgentWrapper config | Trigger condition parameter |
| `context_obsolescence_threshold_k` | LLMAgentWrapper config | Default tied to min lead time |
| `queue_size` | LLMAgentWrapper config | Default 1 |
| `agent_version` | `BaseAgent.agent_version()` | Already written to `hist_reorder_decisions` |
| `random_seed` | `SimConfig` | Already in `SIM_STARTED` event |