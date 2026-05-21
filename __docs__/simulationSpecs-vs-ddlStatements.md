<h1>Simulation Specification vs DDL Statements</h1>

***Simulation - Key Divergences and Resolutions***

> - Simulation Specification: [`__docs__/simulationSpecs.md`](./simulationSpecs.md)
> - DDL Statements: [`_dataStoreDefinition`](../_dataStoreDefinition/)

---

**Contents**:

- [Summary](#summary)
- [Changes by Table](#changes-by-table)
  - [`env_sim_config`](#env_sim_config)
  - [`env_patterns`](#env_patterns)
  - [`ops_warehouse_state`](#ops_warehouse_state)
  - [`ops_cost_accumulator`](#ops_cost_accumulator)
  - [`ops_pending_orders`](#ops_pending_orders)
  - [`hist_reorder_decisions`](#hist_reorder_decisions)
  - [`event_log`](#event_log)
- [Items Left Unchanged (Notable)](#items-left-unchanged-notable)

---

# Summary

The DDL is largely faithful to the spec, with most deviations being deliberate improvements that incorporate the spec's own `⚠️ SUGGESTION` annotations. The table below captures the meaningful differences; trivial implementation details (e.g. `STRING` vs `string` casing) are omitted.

---

# Changes by Table

## `env_sim_config`

| # | Spec | DDL | Reason |
|---|---|---|---|
| 1 | No `run_mode` column | Added `run_mode STRING` (`finite`, `infinite`, `cyclic`) | The spec described three temporal modes in section 1 but never surfaced them as a column. The DDL makes the mode explicit and queryable. |
| 2 | `budget_warning_threshold` typed as `integer` in the spec table | Stored as `DOUBLE` | Clearly a typo in the spec - the field is a fraction (e.g. 0.10) and the description says "float". |
| 3 | `agent_history_window_ticks` and `budget_warning_threshold` absent from original spec table | Both added | Directly incorporates the two `⚠️ SUGGESTION (important/minor)` annotations from sections 3.1 and 7. |

---

## `env_patterns`

| # | Spec | DDL | Reason |
|---|---|---|---|
| 4 | No `supplier_id` column; `supply` role was undefined | `supplier_id STRING` (nullable) added | Incorporates `⚠️ SUGGESTION (critical)` - option (b): keep `supply` in the enum but require a `supplier_id` FK when `role = supply`, enabling the pattern to be attributed to a specific supplier. |
| 5 | No uniqueness constraint on `(sim_id, item_id, role)` | Noted in column comment as enforced unique | Incorporates `⚠️ SUGGESTION (minor)` to prevent double-registration. Not a formal `UNIQUE` constraint in the DDL (Delta doesn't enforce these at write time), but documented as a rule. |

---

## `ops_warehouse_state`

| # | Spec | DDL | Reason |
|---|---|---|---|
| 6 | Write semantics ambiguous - upsert or append? | Explicitly append-only; PK on `(sim_id, tick, item_id)`; `delta.appendOnly = true` | Resolves `⚠️ SUGGESTION (critical)`. Current state = `MAX(tick)` per `(sim_id, item_id)`, documented in column comment. |

---

## `ops_cost_accumulator`

| # | Spec | DDL | Reason |
|---|---|---|---|
| 7 | Same append-vs-upsert ambiguity as `ops_warehouse_state` | Same resolution: append-only, PK on `(sim_id, tick, item_id)` | Resolves `⚠️ SUGGESTION (critical)`. |

---

## `ops_pending_orders`

| # | Spec | DDL | Reason |
|---|---|---|---|
| 8 | `status` enum had `pending`, `arrived`, `partially_lost` only | `fully_lost` added | Resolves `⚠️ SUGGESTION (important)` - needed to handle `transit_loss magnitude = 1.0` where the entire shipment is destroyed. |

---

## `hist_reorder_decisions`

| # | Spec | DDL | Reason |
|---|---|---|---|
| 9 | No `supplier_id` column | `supplier_id STRING` (nullable) added | Incorporates `⚠️ SUGGESTION (minor)` for forward compatibility if the one-supplier-per-item constraint is ever relaxed. |

---

## `event_log`

| # | Spec | DDL | Reason |
|---|---|---|---|
| 10 | `TICK_STARTED` and `TICK_ENDED` absent from original event type list | Both added to `event_type` allowed values and documented in payload | Incorporates `⚠️ SUGGESTION (important)` - needed to distinguish quiet ticks from log gaps during replay. |
| 11 | `BUDGET_WARNING` threshold was a hardcoded magic number (10%) | Now references configurable `budget_warning_threshold`; `threshold` added as a payload field | Incorporates `⚠️ SUGGESTION (minor)`. |

---

# Items Left Unchanged (Notable)

| Topic | Status |
|---|---|
| `supply` role mechanics in `env_patterns` | Still undefined in the engine. The DDL accommodates it structurally (via `supplier_id` FK) but the behaviour is deferred - consistent with the spec's own open question. |
| One-supplier / one-consumer-per-item constraint | Noted in comments; not enforced by a DB constraint. Delta doesn't support partial unique constraints, so this remains a conceptual rule upheld by the engine. |
| Lead time floor (`max(1, ...)`) | Not in the DDL (it's engine logic), but documented in the `env_suppliers.lead_time_variability` column comment as a reminder. |
| RNG draw order for reproducibility | Engine concern only; not expressible in DDL. |

---

*Generated for reference against Simulation V1 Specification and the Databricks setup notebook.*