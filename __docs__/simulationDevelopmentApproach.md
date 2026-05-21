<h1>Simulation - Development Approach</h1>

> **Relevant context**:
>
> - Simulation Specification: [`__docs__/simulationSpecs.md`](./simulationSpecs.md)
> - DDL Statements: [`_dataStoreDefinition`](../_dataStoreDefinition/)
> - Simulation Specification vs. DDL Statements: [`__docs__/simulationSpecs-vs-ddlStatements.md`](./simulationSpecs-vs-ddlStatements.md)

---

**Contents**:

- [Overview](#overview)
  - [Development Steps](#development-steps)
  - [Intended Implementation Structure](#intended-implementation-structure)
  - [Agent Contract](#agent-contract)
- [Conceptual Design](#conceptual-design)
  - [Sequence of Operations](#sequence-of-operations)
  - [Data Flow](#data-flow)
  - [Modules](#modules)
  - [Clarification for Indefinite Simulation Modes](#clarification-for-indefinite-simulation-modes)
    - [Loop Termination Condition](#loop-termination-condition)
    - [Cyclic Loop Definition](#cyclic-loop-definition)
- [Implementation Plan](#implementation-plan)
  - [Stages](#stages)
  - [Key Points for Implementation](#key-points-for-implementation)
  - [Coupling between Stages](#coupling-between-stages)

---

# Overview

## Development Steps

1. Configuring simulation settings
2. Simulating data production and consumption <br> => *Demand/supply patterns, env table population*
3. Tick engine <br> **The loop**: *Event sequencing, state writes, event log*
4. Simulating a basic rule-based reorder script plugs into step 3 as the agent
5. Visualising operations and data flow

## Intended Implementation Structure

- 1 Python package with modules/sub-packages for each step
- Execution can be handled in one of 2 ways:
    - Manually (via Notebook importing this package)
    - Job-scheduled
- The reordering agent should be easily swappable <br> *Since the final use-case is the LLM agent* <br> => *Treat the agent as a plugin*

## Agent Contract
As mentioned, the reodering agent should be easily swappable.

To ensure this, we can define a common agent contract:

```py
class BaseAgent(ABC):
    @abstractmethod
    def decide(self, context: AgentContext) -> list[ReorderDecision]:
        ...
```

Each agent (the basic script and the LLM agent) should:

- Be wrapped as subclasses of `BaseAgent`
- Define the `decide` method (*this is the contract*)

# Conceptual Design

## Sequence of Operations

```
SETUP PHASE
│
├── Load sim config (sim_id, seed, ticks, budget...)
├── Populate env tables (items, suppliers, consumers, patterns, disruptions)
└── Initialise warehouse state (tick 0 stock)

SIMULATION LOOP (per tick)
│
├── [0] Evaluate stochastic disruptions → ops_active_disruptions
├── [1] Process supply arrivals         → ops_pending_orders (update), ops_warehouse_state
├── [2] Draw demand                     → hist_demand_actuals
├── [3a] Apply arrivals to stock      ┐
├── [3b] Apply demand to stock        ┘ → ops_warehouse_state
├── [4] Agent decides                   → hist_reorder_decisions, ops_pending_orders (insert)
├── [5] Accumulate costs                → ops_cost_accumulator, hist_cost_by_tick
└── [6] Write event log                 → event_log

TEARDOWN
│
└── Write SIM_ENDED event, final summaries
```

## Data Flow

```
env tables (static)
        │
        │  read once at setup
        ▼
  SimConfig + World
        │
        │  feeds each tick
        ▼
┌───────────────────────────────────────────┐
│               TICK ENGINE                 │
│                                           │
│  disruptions ──► demand draw              │
│       │               │                   │
│       │          stock update             │
│       │               │                   │
│       └──► agent context ──► agent.decide │
│                                    │      │
│                             reorder placed│
└───────────────────────────────────────────┘
        │
        │  writes each tick
        ▼
ops tables          hist tables        event_log
(live state)     (append history)     (audit trail)
```

## Modules

```
warehouse_sim/
│
├── config/              # Step 1
│   └── loader.py        # Reads sim config, validates, builds SimConfig object
│
├── world/               # Step 2
│   ├── setup.py         # Populates all env tables for a sim run
│   └── patterns.py      # Demand/supply sampling (distributions + custom schedules)
│
├── engine/              # Step 3
│   ├── runner.py        # Tick loop, orchestrates sub-steps in order
│   ├── disruptions.py   # Stochastic + scheduled disruption evaluation
│   ├── supply.py        # Supply arrival processing
│   ├── demand.py        # Demand draw + stock depletion
│   ├── state.py         # Warehouse state reads/writes
│   └── costs.py         # Cost accumulation per tick
│
├── agent/               # Step 4
│   ├── base.py          # BaseAgent ABC + AgentContext + ReorderDecision
│   └── rule_based.py    # RuleBasedAgent(BaseAgent)
│
├── logging/
│   └── event_log.py     # Append-only event writer, all event types
│
└── viz/                 # Step 5
    └── dashboard.py     # Reads hist/ops tables, produces charts
```

**Connection map**:

```
config/loader ──────────────────────► engine/runner
                                            │
world/setup ────► env tables ────────────── │
world/patterns ◄──────────────────── engine/demand
                                            │
engine/disruptions ◄─────────────────────── │
engine/supply ◄──────────────────────────── │
engine/demand ◄──────────────────────────── │
engine/state ◄───────────────────────────── │
engine/costs ◄───────────────────────────── │
                                            │
agent/base ◄─────────────────────────────── │
agent/rule_based ◄───────────────────────── │
                                            │
logging/event_log ◄──────────────────────── │
                                            │
viz/dashboard ◄──── hist/ops tables ◄────── │
```

## Clarification for Indefinite Simulation Modes

In [`simulationSpecification-draft2.md#0-overview`](./simulationSpecification-draft2.md#temporal-constraints), the following simulation modes were defined:

- Running for a defined number of discrete ticks
- Running forever non-cyclically
- Running forever cyclically


***The proposed setup must work for all 3 modes.***

### Loop Termination Condition

The run_mode field in the table `env_sim_config` drives the loop termination condition in `engine/runner.py`:

```
run_mode = "finite"
│
└── loop while tick < num_ticks

run_mode = "infinite"
│
└── loop forever (no termination condition)
│   patterns sample fresh each tick (statistical)
│   or custom_schedule cycles from index 0

run_mode = "cyclic"
│
└── loop forever
    world state resets/cycles at end of each "cycle"
    (cycle length = len(custom_schedule) or user-defined)
```

### Cyclic Loop Definition

"Cycling" implies pattern cycling only, simulating a case where supply/demand patterns are consistent and periodic. Hence, only demand/supply schedules wrap back to index 0; the `custom_schedule` field in the table `env_patterns` already allows this naturally.

# Implementation Plan

## Stages

```
┌─[infraLayer]────────────────────────────────────────────────────────────────┐
│  STAGE 1 - Data models & config loader                                      │
│  Pydantic models for SimConfig, ItemType, Supplier, etc.                    │
│  config/loader.py reads env tables into typed objects                       │
│  ✦ Test: load a hand-written config dict; assert all fields parse correctly │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  ▼
┌─[infraLayer]────────────────────────────────────────────────────────────────┐
│  STAGE 2 - World setup & env table population                               │
│  world/setup.py writes all env tables for a sim run                         │
│  Depends on: stage 1 models                                                 │
│  ✦ Test: toy config (2 items, 1 supplier, 1 consumer); assert row counts    │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  ▼
┌─[infraLayer]────────────────────────────────────────────────────────────────┐
│  STAGE 3 - Demand / supply pattern sampling                                 │
│  world/patterns.py: distributions + custom schedule cycling + noise         │
│  Depends on: stage 1 models, stage 2 env tables                             │
│  ✦ Test: seed RNG; draw 100 samples per distribution; assert mean,          │
│          floor(≥ 0), schedule cycling wraps correctly                       │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  ▼
┌──[coreEngine]───────────────────────────────────────────────────────────────┐
│  STAGE 4 - Tick engine  ◀ highest complexity; test sub-modules first        │
│  Depends on: stages 1-3                                                     │
│                                                                             │
│  Sub-modules, in test order:                                                │
│                                                                             │
│  4a  engine/disruptions.py   stochastic + scheduled disruption eval         │
│       └─ Test: deterministic disruption always active; stochastic           │
│                disruption respects trigger_probability over 1000 draws      │
│                                                                             │
│  4b  engine/supply.py        pending order arrival + transit loss           │
│       └─ Test: order arrives on expected_arrival_tick; transit loss         │
│                reduces arrived_qty correctly; fully_lost status set         │
│                                                                             │
│  4c  engine/demand.py        demand draw + stock depletion                  │
│       └─ Test: fulfilled ≤ stock_on_hand; unmet = demand − fulfilled;       │
│                stock never goes negative                                    │
│                                                                             │
│  4d  engine/state.py         warehouse state reads / writes                 │
│       └─ Test: append-only; MAX(tick) row matches expected stock values     │
│                                                                             │
│  4e  engine/costs.py         cost accumulation per tick                     │
│       └─ Test: holding, stockout, order, transit loss costs all accrue;     │
│                cumulative totals match sum of hist_cost_by_tick             │
│                                                                             │
│  4f  engine/runner.py        wires 4a-4e into the tick loop                 │
│       └─ Test: 5-tick stub run (no agent); n_items × n_ticks rows in        │
│                ops_warehouse_state; TICK_STARTED/ENDED bookend every tick   │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  ▼
┌─[infraLayer]────────────────────────────────────────────────────────────────┐
│  STAGE 5 - Event logger                                                     │
│  logging/event_log.py: append-only writer, UUID gen, payload serialisation  │
│  Depends on: stage 4 (all event types exercised by the engine)              │
│  ✦ Test: fire every event type; assert ordering, no duplicate event_ids,    │
│          all required payload fields present per event type                 │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  ▼
┌──[agentLayer]───────────────────────────────────────────────────────────────┐
│  STAGE 6 - Agent contract + rule-based agent                                │
│  agent/base.py:       BaseAgent ABC, AgentContext, ReorderDecision          │
│  agent/rule_based.py: reorder when stock_on_hand < reorder_point            │
│  Depends on: stage 4 runner (agent plugs into step 4 of the tick loop)      │
│  ✦ Test: swap agent into runner; 20-tick run; assert decisions logged,      │
│          orders created, contract boundary requires zero engine changes     │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │
                                  ▼
┌─[infraLayer]────────────────────────────────────────────────────────────────┐
│  STAGE 7 - Full integration run + visualisation                             │
│  Full finite run: 30 ticks, 3 items, 1 disruption, rule-based agent         │
│  viz/dashboard.py: stock over time, cost breakdown, demand vs fulfilment,   │
│                    disruption overlay                                       │
│  Depends on: all prior stages                                               │
│  ✦ Test: SIM_ENDED event present; total_cost matches sum of                 │
│          hist_cost_by_tick; no negative stock anywhere in ops tables        │
└─────────────────────────────────────────────────────────────────────────────┘
```

**NOTE**:

- `infraLayer` = Infrastructure I/O (stages 1, 2, 3, 5, 7)
- `agentLayer` = Agent layer (swappable plugin) (stage 6)
- `coreEngine` = Core engine (highest complexity) (stage 4)

## Key Points for Implementation

**Stage 4 deserves the most care.** The tick engine is the only stage where sub-module order matters for testing - you want `disruptions.py` independently testable before `supply.py` depends on disruption state, which is testable before `demand.py` depends on post-arrival stock, and so on. Runner just orchestrates what's already proven.

**Stage 5 (event logger) is deliberately decoupled.** It could slot in during stage 4, but keeping it separate means the engine tests can run with a stub logger first and the logger gets its own assertion pass over all event types.

**Stage 6 tests the contract, not the agent.** The rule-based agent is mostly there to confirm the BaseAgent / AgentContext / ReorderDecision boundary is clean - so swapping in the LLM agent later requires zero engine changes.

## Coupling between Stages

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