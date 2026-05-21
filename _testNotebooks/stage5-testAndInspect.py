# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 5 - Event Logger
# MAGIC ### Notebook Test & Inspection
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Fires every event type via `EventLogger`
# MAGIC 2. Queries the `event_log` table to inspect results
# MAGIC 3. Verifies ordering, payload fields, and no duplicate `event_id`s
# MAGIC 4. Runs inline assertions
# MAGIC
# MAGIC **Depends on**: Stage 1 models (for `sim_id` context only)
# MAGIC **Run cells top to bottom. Each cell is independently re-runnable.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Install dependencies

# COMMAND ----------

# MAGIC %pip install pydantic
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Path setup

# COMMAND ----------

import sys

PACKAGE_ROOT = "/Workspace/Repos/reshmaupadhyaya5@gmail.com/dbx-agentic-ai-project-sim-package"
if PACKAGE_ROOT not in sys.path:
    sys.path.insert(0, PACKAGE_ROOT)

print("Python path updated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Constants

# COMMAND ----------

CATALOG = "hackathon_of_the_century"
SIM_ID  = "sim_stage3_001"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Clean up any prior runs for this `SIM_ID`

# COMMAND ----------

spark.sql(f"ALTER TABLE {CATALOG}.tables4eventlog.event_log SET TBLPROPERTIES (delta.appendOnly=false);")
spark.sql(f'''
    DELETE FROM {CATALOG}.tables4eventlog.event_log
    WHERE sim_id = '{SIM_ID}'
''')
spark.sql(f"ALTER TABLE {CATALOG}.tables4eventlog.event_log SET TBLPROPERTIES (delta.appendOnly=true);")
print(f"Cleared prior event_log rows for sim_id={SIM_ID!r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fire every event type via Even`tLogger
# MAGIC
# MAGIC We simulate a minimal 3-tick run (ticks 0, 1, 2) to exercise
# MAGIC all 16 event types in a realistic sequence.

# COMMAND ----------

from warehouse_sim.event_log.event_log import EventLogger

logger = EventLogger(spark, sim_id=SIM_ID)

# -- SIM_STARTED -------------------------------------------------------------
logger.sim_started(
    tick            = 0,
    config_snapshot = {
        "sim_id":    SIM_ID,
        "num_ticks": 3,
        "tick_unit": "day",
        "run_mode":  "finite",
    },
)
print("[DONE] SIM_STARTED")

# -- TICK 0 ------------------------------------------------------------------
logger.tick_started(tick=0)

logger.disruption_activated(
    tick                = 0,
    item_id             = "item_A",
    disruption_id       = "dis_001",
    disruption_type     = "demand_spike",
    effective_magnitude = 2.5,
)
logger.demand_drawn(
    tick             = 0,
    item_id          = "item_A",
    raw_demand       = 48.3,
    disrupted_demand = 120.75,
    fulfilled        = 100,
    unmet            = 20,
)
logger.stockout_occurred(
    tick          = 0,
    item_id       = "item_A",
    unmet_demand  = 20,
    stockout_cost = 40.0,
)
logger.reorder_placed(
    tick                  = 0,
    item_id               = "item_A",
    order_id              = "ord_001",
    order_qty             = 150,
    expected_arrival_tick = 3,
    order_cost            = 725.0,
)
logger.lead_time_extended(
    tick               = 0,
    item_id            = "item_A",
    order_id           = "ord_001",
    original_lead_time = 3,
    extended_lead_time = 6,
    disruption_id      = "dis_001",
)
logger.cost_accrued(
    tick              = 0,
    item_id           = "item_A",
    holding_cost      = 5.0,
    stockout_cost     = 40.0,
    order_cost        = 725.0,
    transit_loss_cost = 0.0,
    tick_total        = 770.0,
)
logger.budget_warning(
    tick             = 0,
    remaining_budget = 900.0,
    budget_limit     = 10_000.0,
    threshold        = 0.10,
)
logger.tick_ended(tick=0)
print("[DONE] TICK 0 events fired")

# -- TICK 1 ------------------------------------------------------------------
logger.tick_started(tick=1)

logger.supply_arrived(
    tick         = 1,
    item_id      = "item_B",
    order_id     = "ord_000",
    ordered_qty  = 100,
    arrived_qty  = 70,
    lost_qty     = 30,
)
logger.transit_loss_applied(
    tick          = 1,
    item_id       = "item_B",
    order_id      = "ord_000",
    lost_qty      = 30,
    arrived_qty   = 70,
    disruption_id = "dis_002",
)
logger.demand_drawn(
    tick             = 1,
    item_id          = "item_B",
    raw_demand       = 20.0,
    disrupted_demand = 20.0,
    fulfilled        = 20,
    unmet            = 0,
)
logger.reorder_held(
    tick             = 1,
    item_id          = "item_B",
    stock_on_hand    = 90,
    stock_in_transit = 0,
    reasoning        = "Stock above reorder point.",
)
logger.cost_accrued(
    tick              = 1,
    item_id           = "item_B",
    holding_cost      = 9.0,
    stockout_cost     = 0.0,
    order_cost        = 0.0,
    transit_loss_cost = 450.0,
    tick_total        = 459.0,
)
logger.tick_ended(tick=1)
print("[DONE] TICK 1 events fired")

# -- TICK 2 ------------------------------------------------------------------
logger.tick_started(tick=2)

logger.disruption_deactivated(
    tick          = 2,
    item_id       = "item_A",
    disruption_id = "dis_001",
)
logger.demand_drawn(
    tick             = 2,
    item_id          = "item_A",
    raw_demand       = 45.0,
    disrupted_demand = 45.0,
    fulfilled        = 45,
    unmet            = 0,
)
logger.cost_accrued(
    tick              = 2,
    item_id           = "item_A",
    holding_cost      = 8.0,
    stockout_cost     = 0.0,
    order_cost        = 0.0,
    transit_loss_cost = 0.0,
    tick_total        = 8.0,
)
logger.budget_exhausted(
    tick             = 2,
    remaining_budget = 0.0,
)
logger.tick_ended(tick=2)
print("[DONE] TICK 2 events fired")

# -- SIM_ENDED ---------------------------------------------------------------
logger.sim_ended(
    tick                 = 2,
    total_cost           = 1237.0,
    total_stockout_ticks = 1,
    total_reorders       = 1,
)
print("[DONE] SIM_ENDED")
print("\nAll 16 event types fired.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Inspect the `event_log` table

# COMMAND ----------

# MAGIC %md ### All events for this sim - ordered by tick then `logged_at`

# COMMAND ----------

display(spark.sql(f'''
    SELECT *
    FROM {CATALOG}.tables4eventlog.event_log
    WHERE sim_id = '{SIM_ID}'
    ORDER BY tick, logged_at
'''))

# COMMAND ----------

# MAGIC %md ### Event type counts

# COMMAND ----------

display(spark.sql(f'''
    SELECT event_type, COUNT(*) AS count
    FROM {CATALOG}.tables4eventlog.event_log
    WHERE sim_id = '{SIM_ID}'
    GROUP BY event_type
    ORDER BY event_type
'''))

# COMMAND ----------

# MAGIC %md ### Check for duplicate `event_ids` (should return 0 rows)

# COMMAND ----------

display(spark.sql(f'''
    SELECT event_id, COUNT(*) AS count
    FROM {CATALOG}.tables4eventlog.event_log
    WHERE sim_id = '{SIM_ID}'
    GROUP BY event_id
    HAVING COUNT(*) > 1
'''))

# COMMAND ----------

# MAGIC %md ### `TICK_STARTED` / `TICK_ENDED` bookending - verify every tick is bookended

# COMMAND ----------

display(spark.sql(f'''
    SELECT tick,
           SUM(CASE WHEN event_type = 'TICK_STARTED' THEN 1 ELSE 0 END) AS started,
           SUM(CASE WHEN event_type = 'TICK_ENDED'   THEN 1 ELSE 0 END) AS ended
    FROM {CATALOG}.tables4eventlog.event_log
    WHERE sim_id = '{SIM_ID}'
      AND tick IN (0, 1, 2)
    GROUP BY tick
    ORDER BY tick
'''))

# COMMAND ----------

# MAGIC %md ### Payload inspection - spot check a few event types

# COMMAND ----------

import json

rows = spark.sql(f'''
    SELECT event_type, item_id, entity_id, payload
    FROM {CATALOG}.tables4eventlog.event_log
    WHERE sim_id = '{SIM_ID}'
    ORDER BY logged_at
''').collect()

for row in rows:
    p = json.loads(row["payload"])
    print(f"[{row['event_type']:<25}] item={str(row['item_id']):<10} entity={str(row['entity_id']):<12} payload={p}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Inline assertions

# COMMAND ----------

import json

all_rows = spark.sql(f'''
    SELECT * FROM {CATALOG}.tables4eventlog.event_log
    WHERE sim_id = '{SIM_ID}'
    ORDER BY logged_at
''').collect()

event_types_logged = {r["event_type"] for r in all_rows}

# -- All 16 event types must be present --------------------------------------
from warehouse_sim.event_log.event_log import EVENT_TYPES
assert EVENT_TYPES == event_types_logged, \
    f"Missing event types: {EVENT_TYPES - event_types_logged}"

# -- No duplicate event_ids --------------------------------------------------
event_ids = [r["event_id"] for r in all_rows]
assert len(event_ids) == len(set(event_ids)), "Duplicate event_ids found"

# -- Every tick bookended by `TICK_STARTED` and `TICK_ENDED` ---------------------
for tick in [0, 1, 2]:
    tick_rows     = [r for r in all_rows if r["tick"] == tick]
    tick_types    = [r["event_type"] for r in tick_rows]
    assert "TICK_STARTED" in tick_types, f"TICK_STARTED missing for tick {tick}"
    assert "TICK_ENDED"   in tick_types, f"TICK_ENDED missing for tick {tick}"

# -- `SIM_STARTED` and `SIM_ENDED` are sim-level (no `item_id`) -------------------
for et in ("SIM_STARTED", "SIM_ENDED"):
    row = next(r for r in all_rows if r["event_type"] == et)
    assert row["item_id"] is None, f"{et} should have null item_id"

# -- `DEMAND_DRAWN` has `item_id` populated --------------------------------------
demand_rows = [r for r in all_rows if r["event_type"] == "DEMAND_DRAWN"]
assert all(r["item_id"] is not None for r in demand_rows), \
    "DEMAND_DRAWN rows must have item_id"

# -- `REORDER_PLACED` `entity_id` matches `order_id` in payload -------------------
placed_row = next(r for r in all_rows if r["event_type"] == "REORDER_PLACED")
p = json.loads(placed_row["payload"])
assert placed_row["entity_id"] == p["order_id"], \
    "entity_id must match order_id in payload for REORDER_PLACED"

# -- `STOCKOUT_OCCURRED` payload fields ----------------------------------------
stockout_row = next(r for r in all_rows if r["event_type"] == "STOCKOUT_OCCURRED")
p = json.loads(stockout_row["payload"])
assert "unmet_demand"  in p, "unmet_demand missing from STOCKOUT_OCCURRED payload"
assert "stockout_cost" in p, "stockout_cost missing from STOCKOUT_OCCURRED payload"

# -- `BUDGET_WARNING` has no `item_id` (sim-level) --------------------------------
bw_row = next(r for r in all_rows if r["event_type"] == "BUDGET_WARNING")
assert bw_row["item_id"] is None, "BUDGET_WARNING should have null item_id"
p = json.loads(bw_row["payload"])
assert "threshold" in p, "threshold missing from BUDGET_WARNING payload"

# -- All payloads are valid JSON strings -------------------------------------
for r in all_rows:
    try:
        json.loads(r["payload"])
    except json.JSONDecodeError:
        raise AssertionError(f"Invalid JSON payload for event_type={r['event_type']!r}")

print(f"[DONE] All assertions passed - {len(all_rows)} events logged, Stage 3 looks good.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Teardown (optional)

# COMMAND ----------

# spark.sql(f"ALTER TABLE {CATALOG}.tables4eventlog.event_log SET TBLPROPERTIES (delta.appendOnly=false);")
# spark.sql(f'''
#     DELETE FROM {CATALOG}.tables4eventlog.event_log
#     WHERE sim_id = '{SIM_ID}'
# ''')
# spark.sql(f"ALTER TABLE {CATALOG}.tables4eventlog.event_log SET TBLPROPERTIES (delta.appendOnly=true);")
# print("Teardown complete.")