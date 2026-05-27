<h1>Data Store Definition</h1>

- **Catalog**: `hackathon_of_the_century`
- **DDL Statements (Notebook)**: [`setup4dataStore.py`](./setup4dataStore.py)

*Brief overview of all tables across the 4 schemas used by the warehouse sim engine.*

---

**Contents**:

- [Schema: `tables4env` - Environment Tables](#schema-tables4env---environment-tables)
- [Schema: `tables4ops` - Operational Tables](#schema-tables4ops---operational-tables)
- [Schema: `tables4hist` - Historical Tables](#schema-tables4hist---historical-tables)
- [Schema: `tables4eventlog` - Event Log](#schema-tables4eventlog---event-log)
- [Schema: `agent_tools` - LLM Agent UC Functions and Tables](#schema-agent_tools---llm-agent-uc-functions-and-tables)
- [Event Types (quick reference)](#event-types-quick-reference)

---

# Schema: `tables4env` - Environment Tables

Static world configuration. Written once before the simulation runs; immutable during a run.

| Table | Description |
|---|---|
| `env_sim_config` | Top-level configuration for a simulation run. One row per `sim_id`. Defines the RNG seed, tick count, run mode, tick unit, budget cap, budget warning threshold, agent history window, and wall-clock anchor for tick 0. Root record referenced as a foreign key by every other table. |
| `env_item_types` | Catalogue of item types in the simulation. One row per item. Stores all cost parameters (holding, stockout, order fixed/variable, transit loss), stock thresholds (reorder point, min/max order qty), and the initial stock level at tick 0. |
| `env_suppliers` | Catalogue of supplier entities. One row per supplier. Stores the baseline lead time and lead time variability used to sample actual delivery delays. |
| `env_consumers` | Catalogue of consumer entities. One row per consumer. Purely a reference table - consumer identity is used for demand attribution; no computation parameters are stored here. |
| `env_supplier_item_map` | Maps each item type to its designated supplier for a given simulation run. Scoped by `sim_id`. Modelled as many-to-many but constrained in practice to one supplier per item per run. |
| `env_consumer_item_map` | Maps each item type to its designated consumer for a given simulation run. Scoped by `sim_id`. Mirrors the structure of `env_supplier_item_map` on the demand side. |
| `env_patterns` | Demand (and optionally supply) pattern configurations per item per run. Each row defines how a signal is generated - either statistically (named distribution + parameters) or from a fixed custom schedule - plus optional seasonal multipliers and Gaussian noise. |
| `env_disruption_schedule` | Pre-scheduled disruption events for a simulation run. Each row defines a disruption window (`start_tick` to `end_tick`), its type (`demand_spike`, `demand_suppression`, `transit_delay`, `transit_loss`), its magnitude, and whether it activates deterministically or probabilistically each tick. |

---

# Schema: `tables4ops` - Operational Tables

Live state during the simulation. Read and written by the engine each tick.

| Table | Description |
|---|---|
| `ops_warehouse_state` | Append-only snapshot of stock levels per item per tick. Records `stock_on_hand`, `stock_in_transit`, and `expected_arrivals_next_tick` at the end of each tick after supply arrivals and demand depletion have been applied. Current state = row with `MAX(tick)` per `(sim_id, item_id)`. |
| `ops_pending_orders` | Record of every reorder placed by the agent. Rows are inserted at order placement and updated to reflect final status (`pending` -> `arrived` / `partially_lost` / `fully_lost`) when the order is processed on its arrival tick. |
| `ops_cost_accumulator` | Append-only running cost totals per item per tick. Tracks cumulative holding, stockout, order, and transit loss costs, plus overall remaining budget. Current totals = row with `MAX(tick)` per `(sim_id, item_id)`. |
| `ops_active_disruptions` | Append-only record of every disruption's activation state each tick. All disruptions within their scheduled window appear here every tick; `is_active_this_tick` distinguishes those that had a real effect (always true for deterministic disruptions; RNG-determined for stochastic ones). |
| `ops_escalation_queue` | Human-review queue written by the LLM agent (not the simulation engine) when it encounters a situation it cannot resolve autonomously: budget breach, imminent stockout, or missing supplier info. The agent makes a concurrent HOLD decision so the simulation continues normally. Rows have a mutable `status` field (`OPEN` / `REVIEWED`) updated by human operators. Written exclusively via the `escalate_item` UC function in the `agent_tools` schema. |

---

# Schema: `tables4hist` - Historical Tables

Append-only summaries that accumulate across ticks. Used for agent context and observability.

| Table | Description |
|---|---|
| `hist_demand_actuals` | One row per item per tick recording the full demand pipeline: raw sampled demand -> disruption-adjusted demand -> fulfilled units -> unmet units (stockout volume). The agent receives a windowed slice of this table as demand history context. |
| `hist_supply_arrivals` | One row per arriving order recording ordered vs. actually received quantities and the actual lead time experienced. Makes transit loss and lead time variance fully observable after the fact. |
| `hist_reorder_decisions` | One row per item per tick for every agent decision - including hold decisions. Captures the full agent context at decision time (stock on hand, in-transit units, reasoning text, agent version), enabling audit and cross-version comparison. |
| `hist_cost_by_tick` | One row per item per tick recording the individual cost components incurred that tick (holding, stockout, order, transit loss). Non-cumulative counterpart to `ops_cost_accumulator`; use for per-tick trend analysis and cost spike debugging. |
| `hist_eval_metrics` | One row per item per tick recording a calculated metric (must be custom-defined). Written by the LLMAgentWrapper monitoring loop on every tick. |


---

# Schema: `tables4eventlog` - Event Log

| Table | Description |
|---|---|
| `event_log` | Unified, append-only, immutable event stream. Every state-changing action in the simulation writes a row here, tagged by `event_type`, `tick`, `item_id`, and a JSON `payload`. `TICK_STARTED` / `TICK_ENDED` events bookend every tick, making quiet ticks distinguishable from log gaps. Supports 16 event types spanning simulation lifecycle, demand, supply, reorders, disruptions, costs, and budget alerts. Source of truth for replay and audit. |

---

# Schema: `agent_tools` - LLM Agent UC Functions and Tables

This schema is **not part of the simulation engine**. It is owned and populated by the LLM agent (the test codebase for this agent is stored in the directory [`test_reorder_llm_agent`](../test_reorder_llm_agent/)), specifically via [`test_reorder_llm_agent/notebooks/UC_Functions.py`](../test_reorder_llm_agent/notebooks/UC_Functions.py). It is documented here because its tables interact with the simulation's data store and the full catalog layout should be visible in one place.

| Object | Type | Description |
|---|---|---|
| `get_inventory_state` | UC function | Returns the latest `ops_warehouse_state` row for one item. Used by the LLM agent to verify current stock before deciding. |
| `get_demand_history` | UC function | Returns the last N ticks of `hist_demand_actuals` for one item, plus a rolling 7-tick average. |
| `get_pending_orders` | UC function | Returns all `pending`-status rows from `ops_pending_orders` for one item. |
| `get_supplier_info` | UC function | Returns `env_suppliers` joined to `env_supplier_item_map` for one item. |
| `get_cost_snapshot` | UC function | Returns the latest `ops_cost_accumulator` row for one item. |
| `get_active_disruptions` | UC function | Returns active rows from `ops_active_disruptions` joined to `env_disruption_schedule` (adds `start_tick`/`end_tick`). |
| `get_full_context` | UC function | Single-call join of all six read functions above, returning one summary row per item. The agent calls this first before using individual tools. |
| `log_agent_decision` | UC function | Builds and validates a `hist_reorder_decisions` row. The caller (`log_agent_decision` tool in `uc_tools.py`) performs `INSERT INTO hist_reorder_decisions SELECT * FROM this_function(...)`. |
| `escalate_item` | UC function | Builds and validates an `ops_escalation_queue` row. The caller performs `INSERT INTO ops_escalation_queue SELECT * FROM this_function(...)`. |
| `llm_reorder_agent` | Registered model | The LLM agent registered in Unity Catalog as an MLflow PyFunc model, deployable to Model Serving. |

---

# Event Types (quick reference)

| Event Type | Fired When |
|---|---|
| `SIM_STARTED` | Tick 0 initialisation |
| `SIM_ENDED` | Final tick completes |
| `TICK_STARTED` | Beginning of each tick |
| `TICK_ENDED` | End of each tick |
| `DEMAND_DRAWN` | Demand sampled per item per tick |
| `SUPPLY_ARRIVED` | Pending order arrives |
| `REORDER_PLACED` | Agent places an order |
| `REORDER_HELD` | Agent decides not to reorder |
| `DISRUPTION_ACTIVATED` | Disruption begins or stochastic disruption triggers |
| `DISRUPTION_DEACTIVATED` | Disruption window ends |
| `STOCKOUT_OCCURRED` | Unmet demand > 0 in a tick |
| `BUDGET_WARNING` | Remaining budget falls below `budget_warning_threshold` |
| `BUDGET_EXHAUSTED` | Budget reaches zero |
| `COST_ACCRUED` | End-of-tick cost accumulation |
| `TRANSIT_LOSS_APPLIED` | Units lost from an in-transit order |
| `LEAD_TIME_EXTENDED` | Transit delay disruption increases a placed order's lead time |
| `EXECUTOR_ALL_STALE` | Only stale agent context instances are available for the LLM agent wrapper's executor block |
| `FALLBACK_STRUCTURAL` | Fallback to rule-based agent due to error in the structure of the LLM's response |
| `FALLBACK_LOGICAL` | Fallback to rule-based agent due to error with respect to the logical constraints of the LLM's response |