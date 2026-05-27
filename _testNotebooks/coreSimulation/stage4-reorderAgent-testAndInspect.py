# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 4 - Tick Engine (runner.py)
# MAGIC ### ReorderAgent Notebook
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Builds and writes a toy `SimWorld` (2 items, 2 suppliers, 1 disruption)
# MAGIC 2. Runs a 10-tick finite simulation with a simple reorder agent
# MAGIC 3. Inspects all ops and hist tables
# MAGIC 4. Verifies correctness with inline assertions
# MAGIC
# MAGIC **Depends on**: Stages 1, 2, 3 + `agent/base.py`
# MAGIC
# MAGIC **Self-contained**: includes world build. Run cells top to bottom. Each cell is independently re-runnable.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Install dependencies

# COMMAND ----------
# MAGIC %pip install pydantic numpy
# MAGIC %restart_python

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Path setup

# COMMAND ----------
import sys
PACKAGE_ROOT = "/Workspace/Repos/mistermilvusmigrans@gmail.com/dbx-agentic-ai-project-sim-package"
if PACKAGE_ROOT not in sys.path:
    sys.path.insert(0, PACKAGE_ROOT)
print("Python path updated.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Constants

# COMMAND ----------
CATALOG  = "hackathon_of_the_century"
SIM_ID   = "sim_stage4_001"
N_TICKS  = 10

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Clean up prior data for this `SIM_ID`

# COMMAND ----------
for table in [
    f"{CATALOG}.tables4env.env_sim_config",
    f"{CATALOG}.tables4env.env_supplier_item_map",
    f"{CATALOG}.tables4env.env_consumer_item_map",
    f"{CATALOG}.tables4env.env_patterns",
    f"{CATALOG}.tables4env.env_disruption_schedule",
    f"{CATALOG}.tables4ops.ops_warehouse_state",
    f"{CATALOG}.tables4ops.ops_pending_orders",
    f"{CATALOG}.tables4ops.ops_cost_accumulator",
    f"{CATALOG}.tables4ops.ops_active_disruptions",
    f"{CATALOG}.tables4hist.hist_demand_actuals",
    f"{CATALOG}.tables4hist.hist_supply_arrivals",
    f"{CATALOG}.tables4hist.hist_reorder_decisions",
    f"{CATALOG}.tables4hist.hist_cost_by_tick",
    f"{CATALOG}.tables4eventlog.event_log",
]:
    try:
        spark.sql(f"DELETE FROM {table} WHERE sim_id = '{SIM_ID}'")
    except:  # Expected to happen if the table is append-only
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES (delta.appendOnly=false);")
        spark.sql(f"DELETE FROM {table} WHERE sim_id = '{SIM_ID}'")
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES (delta.appendOnly=true);")

for table, col, vals in [
    (f"{CATALOG}.tables4env.env_item_types",  "item_id",      "('item_A','item_B')"),
    (f"{CATALOG}.tables4env.env_suppliers",   "supplier_id",  "('sup_001','sup_002')"),
    (f"{CATALOG}.tables4env.env_consumers",   "consumer_id",  "('con_001')"),
]:
    spark.sql(f"DELETE FROM {table} WHERE {col} IN {vals}")

print("[DONE] Prior data cleared")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Build and write `SimWorld`

# COMMAND ----------
from datetime import datetime, timezone
from warehouse_sim.config.models import (
    Consumer, DisruptionSchedule, DisruptionType, Distribution,
    ItemType, Pattern, PatternRole, PatternType, RunMode,
    SimConfig, SimWorld, Supplier, TickUnit,
)
from warehouse_sim.world.setup import write_world

NOW = datetime.now(timezone.utc)

world = SimWorld(
    config = SimConfig(
        sim_id                     = SIM_ID,
        random_seed                = 42,
        num_ticks                  = N_TICKS,
        run_mode                   = RunMode.FINITE,
        tick_unit                  = TickUnit.DAY,
        budget_limit               = 5_000.0,
        budget_warning_threshold   = 0.20,
        agent_history_window_ticks = 5,
        start_timestamp            = NOW,
        created_at                 = NOW,
    ),
    items = {
        "item_A": ItemType(
            item_id="item_A", item_name="Widget A", unit_value=5.0,
            initial_stock=50, reorder_point=20, min_order_qty=10, max_order_qty=100,
            holding_cost_per_unit_per_tick=0.05, stockout_cost_per_unit_per_tick=2.0,
            order_fixed_cost=50.0, order_variable_cost_per_unit=4.5,
            transit_loss_cost_per_unit=6.0,
        ),
        "item_B": ItemType(
            item_id="item_B", item_name="Gadget B", unit_value=12.0,
            initial_stock=30, reorder_point=10, min_order_qty=5, max_order_qty=80,
            holding_cost_per_unit_per_tick=0.10, stockout_cost_per_unit_per_tick=5.0,
            order_fixed_cost=30.0, order_variable_cost_per_unit=10.0,
            transit_loss_cost_per_unit=15.0,
        ),
    },
    suppliers = {
        "sup_001": Supplier(supplier_id="sup_001", supplier_name="Acme Corp",
                            base_lead_time_ticks=2, lead_time_variability=0.0),
        "sup_002": Supplier(supplier_id="sup_002", supplier_name="Globex Ltd",
                            base_lead_time_ticks=3, lead_time_variability=0.0),
    },
    consumers = {
        "con_001": Consumer(consumer_id="con_001", consumer_name="Retail Division"),
    },
    supplier_item_map = {"item_A": "sup_001", "item_B": "sup_002"},
    consumer_item_map = {"item_A": "con_001", "item_B": "con_001"},
    demand_patterns = {
        "item_A": Pattern(
            pattern_id="pat_A", sim_id=SIM_ID, item_id="item_A",
            role=PatternRole.DEMAND, pattern_type=PatternType.CUSTOM,
            custom_schedule=[8.0, 10.0, 12.0, 8.0, 10.0],  # 5-tick cycle
            noise_std=0.0,
        ),
        "item_B": Pattern(
            pattern_id="pat_B", sim_id=SIM_ID, item_id="item_B",
            role=PatternRole.DEMAND, pattern_type=PatternType.CUSTOM,
            custom_schedule=[5.0, 5.0, 8.0, 5.0, 5.0],
            noise_std=0.0,
        ),
    },
    supply_patterns = {},
    disruptions = [
        DisruptionSchedule(
            disruption_id="dis_001", sim_id=SIM_ID, item_id="item_A",
            disruption_type=DisruptionType.DEMAND_SPIKE,
            start_tick=4, end_tick=6,
            magnitude=2.0, is_stochastic=False, trigger_probability=None,
        ),
    ],
)

write_world(spark, world)
print("[DONE] SimWorld written")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Define agents

# COMMAND ----------
from warehouse_sim.agent.base import AgentContext, BaseAgent, ReorderDecision

class HoldAgent(BaseAgent):
    '''Always holds - useful for verifying engine runs without orders.'''
    def decide(self, context: AgentContext) -> list[ReorderDecision]:
        return [ReorderDecision(item_id=i, order_qty=0, reasoning="Always hold.")
                for i in context.items()]
    def agent_version(self) -> str:
        return "hold_agent_v1"

class ReorderAgent(BaseAgent):
    '''
    Simple rule: reorder min_order_qty when stock_on_hand < reorder_point
    and no pending orders exist for the item.
    '''
    def decide(self, context: AgentContext) -> list[ReorderDecision]:
        decisions = []
        for item_id in context.items():
            state   = context.item_states[item_id]
            pending = context.pending_for(item_id)
            if state.stock_on_hand < state.reorder_point and not pending:
                decisions.append(ReorderDecision(
                    item_id   = item_id,
                    order_qty = state.min_order_qty,
                    reasoning = (
                        f"stock_on_hand={state.stock_on_hand} < "
                        f"reorder_point={state.reorder_point}. No pending orders."
                    ),
                ))
            else:
                decisions.append(ReorderDecision(
                    item_id   = item_id,
                    order_qty = 0,
                    reasoning = "Stock sufficient or orders pending.",
                ))
        return decisions
    def agent_version(self) -> str:
        return "reorder_agent_v1"

print("[DONE] Agents defined")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Clear ops/hist/event tables before `ReorderAgent` run

# COMMAND ----------
for table in [
    f"{CATALOG}.tables4ops.ops_warehouse_state",
    f"{CATALOG}.tables4ops.ops_pending_orders",
    f"{CATALOG}.tables4ops.ops_cost_accumulator",
    f"{CATALOG}.tables4ops.ops_active_disruptions",
    f"{CATALOG}.tables4hist.hist_demand_actuals",
    f"{CATALOG}.tables4hist.hist_supply_arrivals",
    f"{CATALOG}.tables4hist.hist_reorder_decisions",
    f"{CATALOG}.tables4hist.hist_cost_by_tick",
    f"{CATALOG}.tables4eventlog.event_log",
]:
    try:
        spark.sql(f"DELETE FROM {table} WHERE sim_id = '{SIM_ID}'")
    except:  # Expected to happen if the table is append-only
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES (delta.appendOnly=false);")
        spark.sql(f"DELETE FROM {table} WHERE sim_id = '{SIM_ID}'")
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES (delta.appendOnly=true);")

print("[DONE] Ops/hist/event tables cleared for ReorderAgent run")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Run simulation - `ReorderAgent` (10 ticks)

# COMMAND ----------
from warehouse_sim.config import load_world
from warehouse_sim.world import PatternSampler
from warehouse_sim.event_log import EventLogger
from warehouse_sim.engine import SimRunner

world_rt = load_world(spark, sim_id=SIM_ID)
sampler  = PatternSampler(seed=world_rt.config.random_seed)
logger   = EventLogger(spark, sim_id=SIM_ID)
runner   = SimRunner(spark, world_rt, ReorderAgent(), logger, sampler)
runner.run()
print(f"[DONE] ReorderAgent run complete ({N_TICKS} ticks)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Inspect ops and hist tables

# COMMAND ----------
# MAGIC %md ### Orders placed

# COMMAND ----------
display(spark.sql(f'''
    SELECT *
    FROM {CATALOG}.tables4ops.ops_pending_orders
    WHERE sim_id = '{SIM_ID}'
    ORDER BY order_tick, item_id
'''))

# COMMAND ----------
# MAGIC %md ### Reorder decisions

# COMMAND ----------
display(spark.sql(f'''
    SELECT *
    FROM {CATALOG}.tables4hist.hist_reorder_decisions
    WHERE sim_id = '{SIM_ID}'
    ORDER BY item_id, tick
'''))

# COMMAND ----------
# MAGIC %md ### Supply arrivals

# COMMAND ----------
display(spark.sql(f'''
    SELECT *
    FROM {CATALOG}.tables4hist.hist_supply_arrivals
    WHERE sim_id = '{SIM_ID}'
    ORDER BY tick, item_id
'''))

# COMMAND ----------
# MAGIC %md ### Stock levels over time (with reorders)

# COMMAND ----------
display(spark.sql(f'''
    SELECT *
    FROM {CATALOG}.tables4ops.ops_warehouse_state
    WHERE sim_id = '{SIM_ID}'
    ORDER BY item_id, tick
'''))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Assertions - `ReorderAgent` run

# COMMAND ----------
import json

# -- At least some reorders placed
n_reorders = spark.sql(f'''
    SELECT COUNT(*) AS cnt FROM {CATALOG}.tables4hist.hist_reorder_decisions
    WHERE sim_id = '{SIM_ID}' AND decision = 'reorder'
''').collect()[0]["cnt"]
assert n_reorders > 0, "ReorderAgent should have placed at least one reorder"

# -- All decisions have agent_version set
bad_version = spark.sql(f'''
    SELECT COUNT(*) AS cnt FROM {CATALOG}.tables4hist.hist_reorder_decisions
    WHERE sim_id = '{SIM_ID}' AND agent_version IS NULL
''').collect()[0]["cnt"]
assert bad_version == 0, "agent_version should never be NULL"

# -- Stock never negative
for row in spark.sql(f'''
    SELECT * FROM {CATALOG}.tables4ops.ops_warehouse_state
    WHERE sim_id = '{SIM_ID}'
''').collect():
    assert row["stock_on_hand"] >= 0, \
        f"Negative stock: tick={row['tick']} item={row['item_id']}"

# -- Costs cumulative = sum of per-tick
for item_id in world_rt.items:
    hist_total = spark.sql(f'''
        SELECT SUM(total_cost) AS s FROM {CATALOG}.tables4hist.hist_cost_by_tick
        WHERE sim_id = '{SIM_ID}' AND item_id = '{item_id}'
    ''').collect()[0]["s"] or 0.0
    cum_total = spark.sql(f'''
        SELECT cumulative_total_cost FROM {CATALOG}.tables4ops.ops_cost_accumulator
        WHERE sim_id = '{SIM_ID}' AND item_id = '{item_id}'
        ORDER BY tick DESC LIMIT 1
    ''').collect()[0]["cumulative_total_cost"]
    assert abs(hist_total - cum_total) < 0.01, \
        f"{item_id}: hist {hist_total:.4f} ≠ cumulative {cum_total:.4f}"

# -- Every tick bookended by `TICK_STARTED` and `TICK_ENDED`
el_rows = spark.sql(f'''
    SELECT tick, event_type FROM {CATALOG}.tables4eventlog.event_log
    WHERE sim_id = '{SIM_ID}'
''').collect()
for tick in range(N_TICKS):
    types = [r["event_type"] for r in el_rows if r["tick"] == tick]
    assert "TICK_STARTED" in types
    assert "TICK_ENDED"   in types

# -- `SIM_ENDED` `total_reorders` matches `hist_reorder_decisions` count
ended_payload = json.loads(
    spark.sql(f'''
        SELECT payload FROM {CATALOG}.tables4eventlog.event_log
        WHERE sim_id = '{SIM_ID}' AND event_type = 'SIM_ENDED'
    ''').collect()[0]["payload"]
)
assert ended_payload["total_reorders"] == n_reorders, \
    "SIM_ENDED total_reorders does not match hist_reorder_decisions count"

print(f"[DONE] All ReorderAgent assertions passed - {n_reorders} reorders placed")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Teardown (optional)

# COMMAND ----------
# from warehouse_sim.world.setup import teardown_world
# teardown_world(spark, sim_id=SIM_ID)
# for table in [
#     f"{CATALOG}.tables4ops.ops_warehouse_state",
#     f"{CATALOG}.tables4ops.ops_pending_orders",
#     f"{CATALOG}.tables4ops.ops_cost_accumulator",
#     f"{CATALOG}.tables4ops.ops_active_disruptions",
#     f"{CATALOG}.tables4hist.hist_demand_actuals",
#     f"{CATALOG}.tables4hist.hist_supply_arrivals",
#     f"{CATALOG}.tables4hist.hist_reorder_decisions",
#     f"{CATALOG}.tables4hist.hist_cost_by_tick",
#     f"{CATALOG}.tables4eventlog.event_log",
# ]:
#     spark.sql(f"DELETE FROM {table} WHERE sim_id = '{SIM_ID}'")
# spark.sql(f"DELETE FROM {CATALOG}.tables4env.env_item_types WHERE item_id IN ('item_A','item_B')")
# spark.sql(f"DELETE FROM {CATALOG}.tables4env.env_suppliers WHERE supplier_id IN ('sup_001','sup_002')")
# spark.sql(f"DELETE FROM {CATALOG}.tables4env.env_consumers WHERE consumer_id = 'con_001'")
# print("Teardown complete.")
