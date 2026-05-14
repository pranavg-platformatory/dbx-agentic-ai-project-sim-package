<h1>DEVLOG</h1>

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
- The round-trip test (write_world → load_world → sample) only makes sense as a single coherent test, not split across two notebooks.
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
✓ Stage 1 — Data models & config loader
✓ Stage 2 — World setup & pattern sampling
  Stage 3 — Event logger          ← next
  Stage 4 — Tick engine (core)
  Stage 5 — (merged into Stage 3 in original plan — event logger)
  Stage 6 — Agent contract + rule-based agent
  Stage 7 — Full integration + visualisation
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

