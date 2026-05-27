<h1>Devlog</h1>

---

**Contents**:

- [Coupling between Stages](#coupling-between-stages)
- [Stage 1: `config`](#stage-1-config)
  - [Pre-Development Notes](#pre-development-notes)
  - [Post-Development Notes](#post-development-notes)
- [Stage 2 \& 3: `world`](#stage-2--3-world)
  - [Why Stages 2 \& 3 Have Been Merged](#why-stages-2--3-have-been-merged)
  - [Pre-Development Notes](#pre-development-notes-1)
  - [Development Notes](#development-notes)
  - [Post-Development Notes](#post-development-notes-1)
- [Stage 5: `event_log`](#stage-5-event_log)
  - [Pre-Development Notes](#pre-development-notes-2)
  - [Post-Development Notes](#post-development-notes-2)
- [Stage 6: `agent` (*only thin implementation*)](#stage-6-agent-only-thin-implementation)
  - [Pre-Development Notes](#pre-development-notes-3)
  - [Post-Development](#post-development)
- [Stage 4: `engine`](#stage-4-engine)
  - [Pre-Development Notes](#pre-development-notes-4)
  - [Development Notes before `runner.py`](#development-notes-before-runnerpy)
  - [Development Notes for `runner.py`](#development-notes-for-runnerpy)
- [Stage 7: `viz`](#stage-7-viz)
  - [Pre-Development Notes](#pre-development-notes-5)
    - [Why Skip the `RuleBasedAgent` for Stage 6 for Now?](#why-skip-the-rulebasedagent-for-stage-6-for-now)
    - [Overview for Stage 7](#overview-for-stage-7)
  - [Post-Development Notes](#post-development-notes-3)
    - [Key Points](#key-points)
    - [Completion Status](#completion-status)
- [LLMAgentWrapper: LLM Agent Integration](#llmagentwrapper-llm-agent-integration)
  - [Pre-Development Notes](#pre-development-notes-6)
  - [Development Notes](#development-notes-1)
    - [Staging overview](#staging-overview)
    - [Stage 1 - `RuleBasedAgent`](#stage-1---rulebasedagent)
    - [Stage 2 - `LLMAgentWrapperConfig`](#stage-2---llmagentwrapperconfig)
    - [Stage 3 - `runner.py` resilience wrap](#stage-3---runnerpy-resilience-wrap)
    - [Stage 4 - `hist_eval_metrics` DDL](#stage-4---hist_eval_metrics-ddl)
    - [Stage 5 - `LLMAgentWrapper`: monitoring loop](#stage-5---llmagentwrapper-monitoring-loop)
    - [Stage 6 - `LLMAgentWrapper`: executor thread and shared result slot](#stage-6---llmagentwrapper-executor-thread-and-shared-result-slot)
    - [Stage 7 - MLflow integration](#stage-7---mlflow-integration)
  - [Completion Status](#completion-status-1)
 
---

# Coupling between Stages

```
infraLayer  ──────────────────────────────►  coreEngine
(stages 1, 2, 3, 5, 7)                            │
     ▲                                            │
     │                                            ▼
     └─────────────────────────────────────  agentLayer
```

**Stages 1-3 (models, world setup, pattern sampling) have zero knowledge of the engine or agent.** They just define data shapes and populate env tables. The engine reads from those tables, not from the infra code directly. **Stage 5 (event logger) is a pure write utility** - it receives structured event dicts and appends them. The engine calls it; it doesn't call anything back. **Stage 7 (viz/dashboard) only reads from `hist_*` and `ops_*` tables.** It's completely downstream and blind to how those rows got there.

The only real dependency edge is that the engine (stage 4) consumes the infra outputs - env tables, pattern sampler, event logger - and the agent (stage 6) consumes the contract types defined in agent/base.py, which itself only depends on the stage 1 data models.

---

TL;DR:

***We can write and test all of `infraLayer` stages (1, 2, 3, and 5) in complete isolation, with no stubs or mocks needed for the engine or agent. Stage 7 similarly just needs populated tables to query.***

# Stage 1: `config`
[`warehouse_sim/config`](./warehouse_sim/config/)

## Pre-Development Notes
This stage is the typed backbone of the entire package. Every entity in the schema - `SimConfig`, `ItemType`, `Supplier`, `Consumer`, `Pattern`, `DisruptionSchedule`, `SimWorld` - [exists as a validated Pydantic model](./README.md#pydantic). All enums are defined. Cross-field validation is enforced at construction time (e.g. `max_order_qty >= min_order_qty`, `num_ticks` required for finite runs, transit_loss magnitude clamped to `[0,1]`).

`loader.py` reads all env tables from Databricks for a given `sim_id` and assembles a `SimWorld`, the single typed object the engine will receive at startup. It also validates cross-table consistency (duplicate mappings, missing demand patterns, unknown supplier/consumer IDs).

## Post-Development Notes
A few things worth noting before we move on:

- `loader.py` is the only file in this stage with a Spark dependency; everything else is pure Python. In Databricks, `load_world(spark, sim_id)` is the single entry point; it returns a `SimWorld` and the engine never touches Spark directly after that.
- `SimWorld` is not a DB table, it is an in-memory convenience container assembled at startup. The `supplier_for()`, `consumer_for()`, and `disruptions_for_tick()` helpers are what the engine will call constantly, so keeping them on the world object avoids repeated dict lookups scattered around the codebase.
- The `loader.py` cross-reference validation (unknown supplier IDs, missing demand patterns, duplicate mappings) means any misconfigured env table setup fails fast at load time, not mid-simulation.

# Stage 2 & 3: `world`
[`warehouse_sim/world`](./warehouse_sim/world/)

## Why Stages 2 & 3 Have Been Merged
The original plan separated them as:

- Stage 2: world setup & env table population (`setup.py`)
- Stage 3: demand/supply pattern sampling (`patterns.py`)

In practice, these two concerns are inseparable at the data level. A `Pattern` is just another env entity; it lives in `env_patterns`, it is written by `write_world`, and it is loaded by `load_world` as part of the `SimWorld`. There is no meaningful sense in which you can "test world setup" without patterns, because a `SimWorld` without demand patterns fails the loader's own cross-reference validation.

More concretely:

- `PatternSampler` takes a `Pattern` model as input - a Stage 1 type. It has no Databricks dependency and no awareness of the engine. So it belongs with the infra layer, not the core engine.
- The round-trip test (write_world -> load_world -> sample) only makes sense as a single coherent test, not split across two notebooks.
- Separating them would have produced a Stage 2 notebook that wrote an incomplete world and a Stage 3 notebook that assumed patterns already existed, creating artificial sequencing friction for no gain.

The split that does make sense is the one already present within the stage: `setup.py` is Databricks-aware (writes to Delta), while `patterns.py` is pure Python (no Spark). The separation of concerns is inside the module, not between stages.

---

TL;DR:

***Pattern definition is part of world definition. You cannot have a valid simulated world without knowing how demand will be drawn.***

## Pre-Development Notes
Stage 2 sits directly on top of Stage 1 and does one thing: takes a SimWorld object and writes it into the Databricks env tables. It's the mirror image of loader.py - where the loader reads env tables into typed models, setup.py writes typed models into env tables.

```
Stage 1 produced:                    Stage 2 consumes / produces:
─────────────────                    ─────────────────────────────
Pydantic models       ──────────►    world/setup.py  writes env tables
SimWorld container                         │
loader.py (reads)     ◄────────────────────┘
                                     world/patterns.py  samples from Pattern model
```

Concretely:

- `setup.py` takes a hand-built SimWorld (or one constructed programmatically) and persists every entity into the env tables; this is the world initialisation step that runs once before the engine starts
- `patterns.py` takes a Pattern model (from Stage 1) and a tick number, and returns a sampled float demand/supply value; this is the sampling logic the engine will call every tick in Stage 4
- Neither file knows anything about the engine or the agent; they only depend on Stage 1 models

## Development Notes
Changed [`warehouse_sim/world/setup.py`](./warehouse_sim/world/setup.py):

- Old version: [`warehouse_sim/world/__recycle_bin__/setup.py`](./warehouse_sim/world/__recycle_bin__/setup.py)
- New version: [`warehouse_sim/world/setup.py`](./warehouse_sim/world/setup.py)

**Key change: Explicit DDL schemas per table**:

These prevent PySpark from inferring long instead of int, missing nullability on arrays, or ambiguous boolean/double types - same class of issues seen in the Stage 1 notebook when using `createDataFrame` without a schema.

## Post-Development Notes

Completion:

```
✓ Stage 1   - Data models & config loader
✓ Stage 2+3 - World setup & pattern sampling
  Stage 5   - Event logger          <- next
  Stage 4   - Tick engine (core)
  Stage 6   - Agent contract + rule-based agent
  Stage 7   - Full integration + visualisation
```

# Stage 5: `event_log`
[`warehouse_sim/event_log`](./warehouse_sim/event_log/)

## Pre-Development Notes
**What `event_log` is**:

A pure write utility. logging/event_log.py receives structured event data from the engine and appends a row to event_log in Databricks. It has no awareness of simulation logic, it just knows how to serialise and write events correctly.

**How it connects to prior stages**:

- Depends on Stage 1 only; it imports no world or pattern logic
- The engine (Stage 4) will call it; it never calls the engine back
- `sim_id` and `tick` come from the engine's execution context
- Event payloads are plain dicts - no Pydantic models cross this boundary, keeping the logger decoupled

**What it must handle**:

- All 16 event types defined in the spec <br> (`SIM_STARTED`, `TICK_STARTED`, `DEMAND_DRAWN`, etc.)
- UUID generation for `event_id`
- JSON serialisation of the payload field
- Append-only writes with explicit schema (same lesson from Stage 2)
- A lightweight `EventLogger` class that holds `spark`, `sim_id`, and exposes one typed method per event type, so the engine never constructs raw dicts itself

**What is tested without Spark**:

- Payload construction for every event type
- UUID uniqueness
- JSON serialisability of all payloads
- The typed method signatures

**What is tested in the notebook**:

- Actual writes to `event_log`
- Row counts, ordering, payload field presence per event type

## Post-Development Notes
Completion:

```
✓ Stage 1   - Data models & config loader
✓ Stage 2+3 - World setup & pattern sampling
✓ Stage 5   - Event logger
  Stage 4   - Tick engine (core)    <- next
  Stage 6   - Agent contract + rule-based agent
  Stage 7   - Full integration + visualisation
```

# Stage 6: `agent` (*only thin implementation*)
[`warehouse_sim/agent`](./warehouse_sim/agent/) (only `base.py`)

## Pre-Development Notes
The plan originally was to first implement stage 4 (the `engine`). However, this stage has a dependency on the `BaseAgent` class defined in `warehouse_sim/agent/base.py`. But it is a one-way, injected dependency - which is the key distinction. The main wiring script (`runner.py` in the `engine` sub-package) depends on the agent interface (`BaseAgent`), not on any concrete agent implementation. Concretely:

```
runner.py  imports  agent/base.py  (the ABC + AgentContext + ReorderDecision)
runner.py  does NOT import  agent/rule_based.py  or any future LLM agent
```

The concrete agent is instantiated outside the engine and passed in:

```python
# In the notebook or job - not in runner.py
agent  = RuleBasedAgent()
runner = SimRunner(spark, world, agent, logger, sampler)
runner.run()
```

So the dependency graph looks like this:

```
agent/base.py  (ABC + data classes)
      ▲
      │  imports
      │
engine/runner.py  ◄──── concrete agent injected at runtime, not imported
      │
      │  imports
      ▼
engine/disruptions.py
engine/supply.py
engine/demand.py
engine/state.py
engine/costs.py
```

**Practical consequence for the build order**:

`agent/base.py` needs to exist before `runner.py` is written, because the runner's type hints reference `BaseAgent`, `AgentContext`, and `ReorderDecision`. But `agent/rule_based.py` can wait until after stage 4.

## Post-Development
Create 3 modules:

- `warehouse_simagent/base.py`
- `warehouse_simagent/hold_agent.py`
- `warehouse_simagent/reoder_agent.py`


# Stage 4: `engine`
## Pre-Development Notes
**What `engine` is**:

The engine is the simulation loop. It owns the tick-by-tick orchestration - calling the right sub-modules in the right order, reading and writing the operational tables, and invoking the agent. Everything built in stages 1-3 feeds into it.

**Sub-module breakdown**:

```
engine/
├── disruptions.py   sub-step 0      - evaluate which disruptions are active this tick
├── supply.py        sub-step 1      - process pending order arrivals
├── demand.py        sub-step 2      - draw demand, deplete stock
├── state.py         sub-steps 3a/3b - read/write ops_warehouse_state
├── costs.py         sub-step 5      - accumulate costs, write ops_cost_accumulator
└── runner.py        the loop        - wires all sub-modules + calls agent + event logger
```

*Each sub-module is a pure function or small class. None of them know about the others - `runner.py` is the only file that imports all of them.*

**Tick sequence**:

```
sub-step 0   disruptions.py   evaluate stochastic disruptions -> ops_active_disruptions
sub-step 1   supply.py        arrive pending orders -> update ops_pending_orders
sub-step 2   demand.py        draw demand from pattern -> hist_demand_actuals
sub-step 3a  state.py         stock += arrived supply
sub-step 3b  state.py         stock -= fulfilled demand (floor 0) -> ops_warehouse_state
sub-step 4   agent.decide()   reorder or hold -> hist_reorder_decisions, ops_pending_orders
sub-step 5   costs.py         accumulate costs -> ops_cost_accumulator, hist_cost_by_tick
sub-step 6   event_log        COST_ACCRUED, tick-level events already fired inline
```

**NOTE**:

- Operational tables: `ops_active_disruptions`, `ops_pending_orders`, `ops_cost_accumulator`
- Historical tables: `hist_demand_actuals`, `hist_reorder_decisions`
- Event log tables: `event_log`

**Key design decisions**:

- State is passed, not fetched mid-tick. At the start of each tick, `runner.py` reads current state once from `ops_warehouse_state`. *Sub-modules receive what they need as arguments; they do not query Databricks themselves mid-tick*.
- Writes happen at the end of each sub-step.
- `PatternSampler` is shared. The single RNG instance created at startup is passed through `runner.py` to `demand.py` and `supply.py`. This is what guarantees reproducibility.
- Agent is injected. `runner.py` takes a `BaseAgent` instance as a parameter - it calls `agent.decide(context)` at sub-step 4. The agent is never imported directly by the engine; it's passed in from outside.
- Event logger is injected too. The `EventLogger` instance is passed into the runner at startup, same pattern.

## Development Notes before `runner.py`
**What has been built**:

- `agent/base.py` - the contract layer. Three frozen dataclasses (`AgentContext`, `ItemState`, `ReorderDecision` etc.) and the `BaseAgent` abstract class with the single decide method. This is the boundary between the engine and any agent implementation - the engine knows only this interface, never a concrete agent.
- `engine/disruptions.py` - sub-step 0. Evaluates which disruptions are active each tick. Deterministic disruptions are always active in their window; stochastic ones draw from the shared RNG in alphabetical `disruption_id` order (for reproducibility). Exposes three multiplier helpers - `get_demand_multiplier`, `get_lead_time_multiplier`, `get_transit_loss_fraction` - which the other sub-modules call rather than inspecting disruptions themselves.
- `engine/supply.py` - sub-step 1 + order placement. `process_arrivals` identifies orders due this tick and applies transit loss. place_order computes effective lead time (with the `max(1, ...)` floor and disruption multiplier) and returns a `PlacedOrder`. Also contains the Spark write functions for writing to the tables `ops_pending_orders` and `hist_supply_arrivals`.
- `engine/demand.py` - sub-steps 2 + 3b. Draws demand from the pattern via the shared sampler, applies the demand multiplier from disruptions, floors to int, computes fulfilled and unmet against current stock, and returns a DemandResult. Stock is never driven below zero.
- `engine/state.py` - sub-steps 3a + 3b writes. Pure mutation functions (`apply_arrivals`, `apply_demand`, `apply_new_order`) operate on an in-memory StockState dict. Spark writes append one row per item per tick to ops_warehouse_state. Also provides initialise_states for tick-0 setup and `fetch_current_states` for restart recovery.
- `engine/costs.py` - sub-step 5. Pure calculation functions for each cost component, plus an in-memory CostState accumulator. Budget check and deduction helpers live here too. Spark writes go to both `ops_cost_accumulator` (cumulative) and `hist_cost_by_tick` (per-tick breakdown).

**What is left for stage 4**:

Just `runner.py` - the loop itself. All the hard logic is already in the sub-modules above. The runner's job is purely orchestration:

```
for each tick:
    fire TICK_STARTED
    [0] evaluate_disruptions()    -> write_activations()
    [1] process_arrivals()        -> update_order_status(), write_arrivals()
    [3a] apply_arrivals()         on stock states
    [2] draw_demand()             -> write_demand_actuals()
    [3b] apply_demand()           on stock states
         write_warehouse_state()
    [4] build AgentContext        -> agent.decide()
        for each decision:
            if reorder -> place_order(), write_placed_order()
            write hist_reorder_decisions
    [5] accumulate costs          -> write_cost_accumulator(), write_cost_by_tick()
    [6] fire events               -> event_log
    fire TICK_ENDED
```

## Development Notes for `runner.py`
Just an orchestrator for the simulation.

# Stage 7: `viz`
[`warehouse_sim/viz`](./warehouse_sim/viz/)

## Pre-Development Notes
### Why Skip the `RuleBasedAgent` for Stage 6 for Now?
The `RuleBasedAgent` is functionally the least essential at this point because:

- The notebook to test stage 4 already defines two inline agents (`HoldAgent`, `ReorderAgent`) (which shall be added to the `warehouse_sim/agent` sub-package) that are sufficient to drive and test the engine end-to-end
- The `BaseAgent` contract (defined in the `warehouse_sim/agent` sub-package) is already proven - any agent, including the eventual LLM agent, just subclasses it and implements decide
- The rule-based agent is a placeholder for the LLM agent, not a dependency of anything else in the stack
- Stage 7 (visualisation) only needs populated `hist_*` and `ops_*` tables, which the stage 4 test notebook already produces

The one thing stage 6 does add is a reusable, importable `RuleBasedAgent` that lives in the package rather than inline in a notebook - useful when you want a reproducible baseline to compare against the LLM agent later. But that's a stage 6 concern, not a stage 7 one.

***So the right call is: do Stage 7 now, come back to Stage 6 when you're ready to build the LLM agent and need a clean baseline to compare it against.***

### Overview for Stage 7
Stage 7 is viz/dashboard.py - it reads purely from `hist_*` and `ops_*` tables and produces charts. No engine, no agent, no Spark writes. The key views to build:

```
1. Stock over time          ops_warehouse_state    - per item, per tick
2. Demand vs fulfilment     hist_demand_actuals    - raw vs fulfilled vs unmet
3. Cost breakdown           hist_cost_by_tick      - stacked by component
4. Cumulative cost          ops_cost_accumulator   - running total per item
5. Reorder decisions        hist_reorder_decisions - reorder vs hold over time
6. Disruption overlay       ops_active_disruptions - highlight active ticks
```

The output will be a Databricks notebook that pulls these views and renders them with `matplotlib` or `pandas` - clean enough to present as a PoC dashboard.

## Post-Development Notes
### Key Points
- `SimDashboard` is lazy-loading. Each property (stock, demand, cost_by_tick etc.) hits Spark only once and caches the result as a pandas DataFrame. Calling `plot_all()` `after print_summary()` doesn't re-query the tables.
- Disruption shading is automatic. Any chart that's per-item will shade ticks where `ops_active_disruptions.is_active_this_tick = true` in translucent red. This means the demand spike at ticks 4-6 in the stage 4 toy world will be visually obvious without any manual annotation.
- The notebook is pointed at `sim_stage4_001` by default - the run the stage 4 test notebook produces. Changing `SIM_ID` at the top of the stage 7 test notebook is all that's needed to inspect any other run.
- Section 12 in the stage 7 test notebook has three meaningful assertions beyond "does it render" - the cost consistency check (`hist_cost_by_tick sum = ops_cost_accumulator` final cumulative), the demand integrity check (`fulfilled + unmet = floor(disrupted_demand)`), and the event log completeness check. These turn the dashboard notebook into a lightweight audit as well as a visual tool.

### Completion Status

```
✓ Stage 1   - Data models & config loader
✓ Stage 2+3 - World setup & pattern sampling
✓ Stage 5   - Event logger
✓ Stage 4   - Tick engine (core)
✓ Stage 6   - Agent contract
  Stage 6   - Rule-based agent <- deferred
✓ Stage 7   - Full integration + visualisation
```
# LLMAgentWrapper: LLM Agent Integration
- [`warehouse_sim/agent`](./warehouse_sim/agent/)
    - `llm_agent_wrapper_types.py`
    - `llm_agent_wrapper.py`
- [`warehouse_sim/config`](./warehouse_sim/config/): `llm_agent_wrapper_config.py`

## Pre-Development Notes

Planning documents (in order):

- [`__docs__/reasoningIntegrationSpecs-1.md`](./__docs__/reasoningIntegrationSpecs-1.md) - initial exploration of LLM integration approaches; tentative direction established as Suggestion 2 (structured context/response, monitoring loop + async executor)
- [`__docs__/reasoningIntegrationSpecs-2.md`](./__docs__/reasoningIntegrationSpecs-2.md) - nine decision-making points raised; overall solution structure defined
- [`__docs__/reasoningIntegrationSpecs-3.md`](./__docs__/reasoningIntegrationSpecs-3.md) - open concerns and sharpening notes raised against the above
- [`__docs__/reasoningIntegrationSpecs-4.md`](./__docs__/reasoningIntegrationSpecs-4.md) - LLMAgentWrapper design (Pranav's scope) and feedback; all design-specific concerns resolved
- [`__docs__/reasoningIntegrationSpecs-5.md`](./__docs__/reasoningIntegrationSpecs-5.md) - build considerations derived from reading existing code; MLflow parameter list defined
- [`__docs__/reasoningIntegrationDevelopmentApproach-1.md`](./__docs__/reasoningIntegrationDevelopmentApproach-1.md) - implementation approach (what gets touched, external elements, stub phase flow)
- [`__docs__/reasoningIntegrationDevelopmentApproach-2.md`](./__docs__/reasoningIntegrationDevelopmentApproach-2.md) - spec compliance comparison; partially addressed points flagged
- [`__docs__/reasoningIntegrationDevelopmentApproach-3.md`](./__docs__/reasoningIntegrationDevelopmentApproach-3.md) - implementation stages with justification and dependency graph

**Scope**: Pranav's scope only. Her Majesty Reshma the Boss's scope (UC read tool definitions, LangFuse trace structure) is tracked separately as open items in `__docs__/reasoningIntegrationSpecs-4`.

---

## Development Notes

### Staging overview

Seven stages, ordered by: dependencies before dependents → sync before async → tables before writers → observability last.

```
Stage 1   agent/rule_based_agent.py          RuleBasedAgent (deferred from sim Stage 6)
Stage 2   config/llm_agent_wrapper_config.py LLMAgentWrapperConfig (Pydantic)
Stage 3   engine/runner.py                   Resilience wrap around agent.decide()
Stage 4   setup4dataStore.py                 hist_eval_metrics DDL
Stage 5   agent/llm_agent_wrapper.py         LLMAgentWrapper - monitoring loop only
Stage 6   agent/llm_agent_wrapper.py         Executor thread + shared result slot + StubLLMAgent
Stage 7   (MLflow integration)               Run-level parameter logging - not yet done
```

---

### Stage 1 - `RuleBasedAgent`

**File**: `warehouse_sim/agent/rule_based_agent.py`

Implements `RuleBasedAgent(BaseAgent)` deferred from simulation Stage 6. Rule: reorder when `stock_on_hand < reorder_point`, quantity = `min_order_qty`.

Key points:
- Fully deterministic - no stochastic draws, FR-07 reproducibility satisfied without threading the `PatternSampler` through
- Returns `order_qty=0` for hold decisions, consistent with `REORDER_HELD` event semantics
- Required as a dependency of the LLMAgentWrapper fallback path (`FALLBACK_STRUCTURAL` and `FALLBACK_LOGICAL`) before any LLMAgentWrapper code can be tested

---

### Stage 2 - `LLMAgentWrapperConfig`

**File**: `warehouse_sim/config/llm_agent_wrapper_config.py`

Pydantic model holding all LLMAgentWrapper-specific parameters, separate from `SimConfig`.

Key points:
- `executor_trigger_every_n_ticks` has no default - forced explicit configuration to prevent silent non-reproducibility
- `context_obsolescence_threshold_k` defaults to `None`; resolved to `min_lead_time` from `SimConfig` at LLMAgentWrapper `__init__` time, not at config construction. A `UserWarning` is emitted when `None` to surface the deferred resolution without blocking the workflow. A comment explains why `UserWarning` rather than a hard validation error
- `queue_size` defaults to 1; drain logic is implemented in full for all sizes
- `stub_mode` defaults to `None` (live LLM); always present and always logged to MLflow so runs are unambiguously labelled

---

### Stage 3 - `runner.py` resilience wrap

**File**: `warehouse_sim/engine/runner.py`

Single `try/except` block added around `agent.decide(context)` and `_validate_decisions()` in `_run_tick`.

Key points:
- `_validate_decisions` is inside the `try` block because it raises `ValueError` with no recovery path - it must be covered by the same wrap
- On catch: logs `AGENT_ERROR` event (exception type + message), substitutes hold-all decisions, simulation continues
- Hold-all is the fallback (not `RuleBasedAgent`) to avoid coupling the runner to the agent layer beyond the `BaseAgent` contract
- The LLMAgentWrapper's own pre-flight validation is the primary defence; this wrap is the last-resort safety net only

**Dependency flag**: `EventLogger.agent_error()` does not yet exist - must be added before this change can be tested end-to-end. Follows the same pattern as every other typed event method on the logger.

---

### Stage 4 - `hist_eval_metrics` DDL

**File**: `setup4dataStore.py` (new table added)

Append-only Delta table written by the LLMAgentWrapper monitoring loop every tick. Added between `hist_cost_by_tick` and the Event Log section, keeping all `hist_*` tables together.

Key points:
- Narrow/tall schema: one row per metric per tick (per item where applicable). Adding a new metric means a new row, not a column alter
- `item_id` is nullable: `NULL` = run-level metric; populated = item-level metric
- `metric_value` typed as `DOUBLE` - all foreseeable metrics are numeric
- Partitioned by `sim_id`, consistent with all other `hist_*` tables
- PK on `(sim_id, tick, item_id, metric_name)` - monitoring loop must write at most one run-level row per metric per tick (item_id = NULL in a PK requires this discipline from the writer)

---

### Stage 5 - `LLMAgentWrapper`: monitoring loop

**Files**: `warehouse_sim/agent/llm_agent_wrapper.py`, `warehouse_sim/agent/llm_agent_wrapper_types.py`

Sync, tick-bound half of `LLMAgentWrapper.decide()` only. Executor half is a clearly marked `# STAGE 6` block placeholder.

Key points:
- `LLMAgentWrapper` is a `BaseAgent` subclass - the runner sees it as just another agent, with no awareness of the queue, monitoring loop, or LLM call inside
- `context_obsolescence_threshold_k=None` resolved at `__init__` time from `min_lead_time` across `world.suppliers`. Resolved value stored as `self._resolved_k` - this is what gets logged to MLflow, never `None`
- `_last_committed` initialised as hold-all at construction time, not `None` - `decide()` returns a valid `list[ReorderDecision]` from tick 0 with no `None` check in the hot path
- `QueueMessage` and `ExecutorResult` live in `llm_agent_wrapper_types.py` - shared between sync and async sides without circular imports
- `_write_eval_metrics` is bracketed with `EVALUATION TOOL CALL BOUNDARY` comments marking the exact block Her Majesty Reshma the Boss will instrument with LangFuse. Metric values are `0.0` stubs with named `# TODO` comments specifying the source fields
- `SparkSession` injected at construction (not passed through `decide()`) - consistent with the runner's own pattern; `decide()` remains side-effect-free from the caller's perspective
- Four dependency flags in module docstring (`[DEP-1]` through `[DEP-4]`) covering `EventLogger.agent_error()`, `SimWorld.suppliers` structure, `hist_eval_metrics` table existence, and `SparkSession` injection

---

### Stage 6 - `LLMAgentWrapper`: executor thread and shared result slot

**File**: `warehouse_sim/agent/llm_agent_wrapper.py` (extended in place)

Async half of `LLMAgentWrapper`, filling in the `# STAGE 6` placeholder.

Key points:

**Shared state and thread safety**:
- `_result_slot`, `_executor_busy`, `_last_committed` are the three shared state attributes
- A `threading.Lock` (`_slot_lock`) guards all reads and writes of `_result_slot` and `_executor_busy`. Rationale documented in code: CPython GIL makes bare assignment practically safe, but relying on GIL behaviour is an implementation detail, not a language guarantee - the lock makes intent explicit and ensures correctness outside CPython

**Slot consumption ordering**:
- Slot consumption happens at the top of `decide()`, before the trigger check. A result that arrived while the executor was running must be committed before deciding whether to dispatch again

**Dispatch**:
- Queue snapshot taken at dispatch time - the executor works on a stable copy independent of what the monitoring loop appends on subsequent ticks
- `daemon=True` on the thread - executor will not prevent process exit if the simulation ends while running; noted as a PROD revisit point

**Executor thread (`_run_executor`)**:
- Drain logic iterates newest-to-oldest; first non-outdated message is used. Implemented in full regardless of `queue_size`
- All-stale case: logs `EXECUTOR_ALL_STALE` event, clears `_executor_busy`, returns without writing to `_result_slot` - holding is correct when every available context is stale
- Both `_result_slot` and `_executor_busy` written together inside one lock acquisition on completion, so the sync side always sees a consistent pair

**Pre-flight validation**:
- `_validate_structural`: checks response is `list[ReorderDecision]`. In stub phase, `valid`/`logical_fail` modes pass; `structural_fail` mode returns a raw string and fails here
- `_validate_logical`: checks `item_id` known, `order_qty >= 0`, `min_order_qty <= order_qty <= max_order_qty` for reorders. Hold (`order_qty=0`) always passes
- Each failure logs its distinct event (`FALLBACK_STRUCTURAL` / `FALLBACK_LOGICAL`) and substitutes `RuleBasedAgent` decisions

**`_StubLLMAgent`**:
- Private class, not a `BaseAgent` subclass - stands in for the HTTP call only
- Three modes: `valid` (happy path), `structural_fail` (FALLBACK_STRUCTURAL path), `logical_fail` (FALLBACK_LOGICAL path). All three must be tested before stub is retired

**Known gaps (from post-implementation evaluation)**:
- `_executor_busy` read in trigger check is outside the lock - inconsistent with stated rationale; acceptable for CPython but should be addressed
- No `finally` block in `_run_executor` - if `NotImplementedError` is raised (stub_mode=None), `_executor_busy` is never cleared; simulation silently stops dispatching. Needs a `finally` block
- `_write_eval_metrics` has no error handling - a failed metric write should not halt the tick; needs a `try/except` with event log fallback
- `EXECUTOR_ALL_STALE`, `FALLBACK_STRUCTURAL`, `FALLBACK_LOGICAL` event types added to `event_log` DDL ✓ (updated post-evaluation)

---

### Stage 7 - MLflow integration

Not yet implemented. To be done at run start: log `agent_history_window_ticks`, `executor_trigger_every_n_ticks`, `context_obsolescence_threshold_k` (resolved value), `queue_size`, `agent_version`, `random_seed`.

---

## Completion Status

```
✓ Stage 1   - Data models & config loader
✓ Stage 2+3 - World setup & pattern sampling
✓ Stage 5   - Event logger
✓ Stage 4   - Tick engine (core)
✓ Stage 6   - Agent contract
✓ Stage 6   - Rule-based agent (completed in LLMAgentWrapper Stage 1)
✓ Stage 7   - Full integration + visualisation

LLMAgentWrapper:
✓ LLMAgentWrapper Stage 1  - RuleBasedAgent
✓ LLMAgentWrapper Stage 2  - LLMAgentWrapperConfig
✓ LLMAgentWrapper Stage 3  - runner.py resilience wrap
✓ LLMAgentWrapper Stage 4  - hist_eval_metrics DDL
✓ LLMAgentWrapper Stage 5  - LLMAgentWrapper monitoring loop
✓ LLMAgentWrapper Stage 6  - Executor thread + shared result slot + StubLLMAgent
  LLMAgentWrapper Stage 7  - MLflow integration                    <- next
  Known gaps    - finally block, _executor_busy lock consistency, _write_eval_metrics error handling
  Open (Her Majesty Reshma the Boss) - UC read tools, LangFuse trace structure
```