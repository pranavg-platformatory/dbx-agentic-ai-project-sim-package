<h1>Integration Test 1:<br><i>Rule-Based vs. LLM Agent for Finite Ticks</i></h1>

---

**Contents**:

- [Overview](#overview)
- [Simulation Setup](#simulation-setup)
  - [Shared Parameters](#shared-parameters)
  - [Agent Configurations](#agent-configurations)
- [Notebook Structure](#notebook-structure)
  - [Section 1: Run](#section-1-run)
  - [Section 2: Analyse](#section-2-analyse)
    - [2.1 Plots](#21-plots)
    - [2.2 Summary Tables](#22-summary-tables)
    - [2.3 Evaluation Queries](#23-evaluation-queries)
- [Design Notes](#design-notes)

---

# Overview

A single Databricks notebook that runs both agents against an identical world configuration and produces a structured comparison of their behaviour and cost outcomes. The notebook is split into two sections - **Run** and **Analyse** - so that plots and queries can be re-executed without re-running the simulation.

Both agents run for 20 ticks (tick unit: 1 hour) using the same random seed, the same world config, and the same `sim_id` prefix. Each agent gets its own `sim_id` so their Delta table rows never mix.

---

# Simulation Setup

## Shared Parameters

Set once at the top of the notebook as explicit variables. All subsequent cells reference these.

| Parameter | Value | Notes |
|---|---|---|
| `SIM_SEED` | (chosen at test time) | Passed to both `SimRunner` instantiations. Must be identical for a fair comparison. |
| `NUM_TICKS` | `20` | |
| `TICK_UNIT` | `1 hour` | Cosmetic label; affects axis labels in plots. |
| `SIM_ID_RULEBASED` | `'sim_rulebased_001'` | Adjust suffix for repeated runs. |
| `SIM_ID_LLM` | `'sim_llm_001'` | |
| `CATALOG` | `'hackathon_of_the_century'` | |

## Agent Configurations

**Agent 1 - `RuleBasedAgent`**:
- Instantiated directly; no wrapper config needed.
- Passed to `SimRunner` as-is.

**Agent 2 - `LLMAgentWrapper` (with `LLMReorderAgent`)**:
- `stub_mode = None` (live LLM call).
- `suppress_write_tools = True` (default; runner owns `hist_reorder_decisions` writes).
- `llm_agent_config_override`: set `warehouse_id` and `llm_endpoint` as appropriate for the test environment.
- Ensure `/Workspace/Shared/reorder-llm-agent` is on `sys.path` before instantiation (see `[DEP-5]` in [`warehouse_sim/agent/llm_agent_wrapper.py`](../warehouse_sim/agent/llm_agent_wrapper.py)).

---

# Notebook Structure

## Section 1: Run

All simulation execution happens here. No plots, no queries.

```
[Cell 1]  Parameters - SIM_SEED, NUM_TICKS, SIM_ID_*, CATALOG
[Cell 2]  Imports and sys.path setup
[Cell 3]  World config - build SimWorld (shared; used by both agents)
[Cell 4]  Run Agent 1 - RuleBasedAgent → SimRunner(SIM_ID_RULEBASED)
[Cell 5]  Run Agent 2 - LLMAgentWrapper → SimRunner(SIM_ID_LLM)
```

`SimRunner` for Agent 2 will take longer due to LLM call latency per executor trigger. Section 2 can be re-run independently once both runs have completed.

## Section 2: Analyse

All queries and plots. Reads only from Delta tables - no simulation code.

### 2.1 Plots

Six plots, rendered for both `sim_id`s side-by-side (or as separate figures with shared x-axis tick range).

---

**Plot 1 - Disruptions across ticks**

*Source*: `ops_active_disruptions`

One subplot per disruption type (`demand_spike`, `demand_suppression`, `transit_delay`, `transit_loss`). Each subplot shows `effective_magnitude` per tick, with ticks where `is_active_this_tick = FALSE` shown as zero. Shaded bands highlight the disruption's scheduled window.

Since both agents run against the same world and seed, this plot should be identical for both `sim_id`s - use it as a sanity check that the shared seed is working correctly.

---

**Plot 2 - Demand across ticks**

*Source*: `hist_demand_actuals`

Line graph. One line per item: `raw_demand` (dashed) and `disrupted_demand` (solid). Disruption-active ticks shaded in translucent red (from `ops_active_disruptions` where `is_active_this_tick = TRUE` and `disruption_type IN ('demand_spike', 'demand_suppression')`).

Shown separately per agent, or overlaid with different colours, to reveal whether the agents' ordering behaviour caused any visible secondary effects (e.g. demand draw is identical; only fulfilment differs).

---

**Plot 3 - Actual average lead time per item**

*Source*: `ops_pending_orders`

Bar chart. Per item: `AVG(expected_arrival_tick - order_tick)`. This reflects actual lead times including any `transit_delay` disruption multiplier applied at order placement - not the configured baseline. Grouped bars, one group per item, one bar per agent. Items with no orders placed show no bar (note this in the chart).

---

**Plot 4 - Cost accrued per item**

*Source*: `ops_cost_accumulator` at `MAX(tick)`

Stacked bar chart. Per item: `cumulative_holding_cost`, `cumulative_stockout_cost`, `cumulative_order_cost`, `cumulative_transit_loss_cost`. Grouped bars, one group per item, one bar per agent. Allows visual identification of which cost component drives the difference between agents.

---

**Plot 5 - Demand quantities: fulfilled vs. unmet per tick**

*Source*: `hist_demand_actuals`

Line graph. Two lines per item: `fulfilled` and `unmet`. Ticks where `STOCKOUT_OCCURRED` was logged shaded in red. One figure per agent. The gap between `fulfilled` and `fulfilled + unmet` is the stockout magnitude per tick.

---

**Plot 6 - Reorder decisions per tick**

*Source*: `hist_reorder_decisions`

Shown as a separate subplot beneath Plot 5 (shared x-axis). Markers at ticks where `decision = 'reorder'`, sized by `order_qty`. Hold ticks shown as a flat line at zero. One figure per agent. This makes the relationship between reorder timing and subsequent stockouts visually traceable.

> Plot 5 and Plot 6 share an x-axis and are rendered together as a two-panel figure per agent. They are specified separately here for clarity.

---

### 2.2 Summary Tables

Five summary tables, rendered as pandas DataFrames via `display()`.

---

**Table 1 - Disruption summary**

*Source*: `ops_active_disruptions`, `env_disruption_schedule`

One row per disruption per agent. Columns: `disruption_id`, `disruption_type`, `scheduled_start_tick`, `scheduled_end_tick`, `ticks_active` (count of ticks where `is_active_this_tick = TRUE`), `avg_effective_magnitude`, `max_effective_magnitude`. Since disruptions are seeded identically, this table primarily confirms correct world replication between agents.

---

**Table 2 - Reorder decisions**

*Source*: `hist_reorder_decisions`

One row per `(sim_id, tick, item_id)`. Columns: `sim_id`, `tick`, `item_id`, `decision`, `order_qty`, `stock_on_hand_at_decision`, `stock_in_transit_at_decision`, `agent_reasoning` (LLM run only; NULL for rule-based). Sorted by `sim_id`, `tick`, `item_id`.

---

**Table 3 - Demand fulfilment summary**

*Source*: `hist_demand_actuals`

One row per `(sim_id, item_id)`. Columns: `sim_id`, `item_id`, `total_raw_demand`, `total_disrupted_demand`, `total_fulfilled`, `total_unmet`, `fulfilment_rate` (= `total_fulfilled / total_disrupted_demand`).

---

**Table 4 - Stockout summary**

*Source*: `event_log` (`STOCKOUT_OCCURRED` events)

One row per `(sim_id, item_id)`. Columns: `sim_id`, `item_id`, `stockout_ticks` (count of ticks with unmet demand > 0), `total_unmet_demand` (sum), `total_stockout_cost` (sum from payload).

---

**Table 5 - Agent health summary** *(LLM run only)*

*Source*: `event_log`

Counts of diagnostic events for `SIM_ID_LLM`. Columns: `event_type`, `count`. Event types: `FALLBACK_STRUCTURAL`, `FALLBACK_LOGICAL`, `EXECUTOR_ALL_STALE`, `AGENT_ERROR`. Zero rows = clean run. Any non-zero count means the cost comparison is not a pure LLM vs. rule-based comparison for those ticks - the fallback `RuleBasedAgent` contributed.

---

**Table 6 - Escalation summary** *(LLM run only)*

*Source*: `ops_escalation_queue`

One row per escalation raised during `SIM_ID_LLM`. Columns: `tick`, `item_id`, `reason`, `status`, `raised_at`. If empty, display a note confirming zero escalations. An escalation means the LLM identified a situation it could not handle autonomously - substantive information about agent behaviour regardless of whether it affects the cost outcome.

---

### 2.3 Evaluation Queries

The nine SQL evaluation queries (see: [`_testNotebooks/integrationTesting/integrationTest-1/evaluationQueries.sql`](./evaluationQueries.sql)), executed in order against both `sim_id`s. Each query rendered as a `display()` DataFrame. Queries:

| # | Query | Table(s) |
|---|---|---|
| Q1 | Agent decisions per tick | `hist_reorder_decisions` |
| Q2 | Missing decision gap check | `hist_reorder_decisions` |
| Q3 | Monitoring loop / eval metrics per tick | `hist_eval_metrics` |
| Q4 | Pending orders and arrival status | `ops_pending_orders` |
| Q5 | Fallback and failure events | `event_log` |
| Q6 | Cost totals at end of run (per item) | `ops_cost_accumulator` |
| Q7 | Cost comparison: both agents | `ops_cost_accumulator` |
| Q8 | Escalations | `ops_escalation_queue` |
| Q9 | Stockout events with payload | `event_log` |

Q2 and Q5 are correctness checks - zero rows is the expected result for a healthy run. Q7 is the headline performance comparison.

---

# Design Notes

**Seed discipline**: `SIM_SEED` must be set explicitly and passed to both `SimRunner` instantiations. Do not rely on a default. The disruption plot (Plot 1) being identical for both agents is the visual confirmation that this is working.

**Re-runnability**: Section 2 reads only from Delta tables. If a plot or query fails, re-run from Section 2 without touching Section 1. This matters because a 20-tick LLM run has non-trivial wall time.

**Fallback contamination**: Table 5 (agent health summary) must be checked before interpreting the cost comparison (Q7 / Plot 4). If `FALLBACK_STRUCTURAL`, `FALLBACK_LOGICAL`, or `EXECUTOR_ALL_STALE` events are present for `SIM_ID_LLM`, note which ticks were affected - those ticks used `RuleBasedAgent` decisions, not LLM decisions. A run with many fallbacks is not a clean LLM test.

**`hist_eval_metrics` stub values**: Q3 will return rows with `metric_value = 0.0` for all metrics. This is expected - metric computation is not yet implemented (stub TODOs in `_write_eval_metrics`). The query confirms the monitoring loop ran and wrote a row every tick; the values are not yet meaningful.

**`agent_reasoning` column**: Present in `hist_reorder_decisions` and populated by `LLMReorderAgent`. NULL for the rule-based run. Include it in Table 2 for the LLM run - it is the most direct window into the LLM's decision logic and is worth reading manually for a 20-tick test.