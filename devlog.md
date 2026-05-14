<h1>DEVLOG</h1>

---

**Contents**:

- [Coupling between Stages](#coupling-between-stages)
- [Stage 1: `config`](#stage-1-config)
  - [Post-Development Notes](#post-development-notes)
- [Stage 2: `world`](#stage-2-world)
  - [Pre-Development Notes](#pre-development-notes)
  - [Development Notes](#development-notes)
  - [Post-Development Notes](#post-development-notes-1)
 
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

## Post-Development Notes
A few things worth noting before we move on:

- `loader.py` is the only file in this stage with a Spark dependency; everything else is pure Python. In Databricks, `load_world(spark, sim_id)` is the single entry point; it returns a `SimWorld` and the engine never touches Spark directly after that.
- `SimWorld` is not a DB table, it is an in-memory convenience container assembled at startup. The `supplier_for()`, `consumer_for()`, and `disruptions_for_tick()` helpers are what the engine will call constantly, so keeping them on the world object avoids repeated dict lookups scattered around the codebase.
- The `loader.py` cross-reference validation (unknown supplier IDs, missing demand patterns, duplicate mappings) means any misconfigured env table setup fails fast at load time, not mid-simulation.

# Stage 2: `world`
[`warehouse_sim/world`](./warehouse_sim/world/)

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

These prevent PySpark from inferring long instead of int, missing nullability on arrays, or ambiguous boolean/double types - same class of issues seen in the Stage 1 notebook when using createDataFrame without a schema.

## Post-Development Notes

