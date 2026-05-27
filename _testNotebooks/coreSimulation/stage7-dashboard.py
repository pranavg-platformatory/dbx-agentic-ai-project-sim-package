# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 7 - Simulation Dashboard
# MAGIC ### Visualisation & Observability
# MAGIC
# MAGIC Reads from `hist_*` and `ops_*` tables for a completed simulation run
# MAGIC and renders five charts:
# MAGIC
# MAGIC | # | Chart | Source table |
# MAGIC |---|---|---|
# MAGIC | 1 | Stock levels over time | `ops_warehouse_state` |
# MAGIC | 2 | Demand vs fulfilment | `hist_demand_actuals` |
# MAGIC | 3 | Cost breakdown per tick | `hist_cost_by_tick` |
# MAGIC | 4 | Cumulative cost | `ops_cost_accumulator` |
# MAGIC | 5 | Reorder decisions | `hist_reorder_decisions` |
# MAGIC
# MAGIC Disruption-active ticks are shaded red on all per-item charts.
# MAGIC
# MAGIC **No engine or agent dependency - reads only.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Install dependencies

# COMMAND ----------

# MAGIC %pip install pydantic numpy matplotlib pandas
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
# MAGIC ## 2. Choose simulation run
# MAGIC
# MAGIC Set `SIM_ID` to any completed run.
# MAGIC The Stage 4 notebook produced `sim_stage4_001` - use that by default.

# COMMAND ----------

SIM_ID  = "sim_stage4_001"   # <-- change to inspect a different run
CATALOG = "hackathon_of_the_century"

print(f"Dashboard for sim_id: {SIM_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify the run has data

# COMMAND ----------

counts = {}
for name, table in [
    ("ops_warehouse_state",    f"{CATALOG}.tables4ops.ops_warehouse_state"),
    ("hist_demand_actuals",    f"{CATALOG}.tables4hist.hist_demand_actuals"),
    ("hist_cost_by_tick",      f"{CATALOG}.tables4hist.hist_cost_by_tick"),
    ("ops_cost_accumulator",   f"{CATALOG}.tables4ops.ops_cost_accumulator"),
    ("hist_reorder_decisions", f"{CATALOG}.tables4hist.hist_reorder_decisions"),
    ("ops_active_disruptions", f"{CATALOG}.tables4ops.ops_active_disruptions"),
    ("event_log",              f"{CATALOG}.tables4eventlog.event_log"),
]:
    n = spark.sql(f"SELECT COUNT(*) AS n FROM {table} WHERE sim_id = '{SIM_ID}'") \
             .collect()[0]["n"]
    counts[name] = n
    print(f"  {name:<30} {n:>6} rows")

assert counts["ops_warehouse_state"] > 0, \
    f"No data found for sim_id={SIM_ID!r}. Run the Stage 4 notebook first."

print(f"\n[DONE] Data verified for sim_id={SIM_ID!r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Initialise dashboard

# COMMAND ----------

from warehouse_sim.viz.dashboard import SimDashboard
import matplotlib
matplotlib.rcParams["figure.dpi"] = 120

dash = SimDashboard(spark, sim_id=SIM_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Run summary (text)

# COMMAND ----------

dash.print_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Chart 1 - Stock levels over time
# MAGIC
# MAGIC - **Solid line**: stock on hand (post-arrival, post-demand)
# MAGIC - **Dashed line**: stock in transit (pending orders)
# MAGIC - **Green dotted verticals**: ticks where a reorder was placed
# MAGIC - **Red shading**: ticks where a disruption was active

# COMMAND ----------

fig = dash.plot_stock()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Chart 2 - Demand vs fulfilment
# MAGIC
# MAGIC - **Line**: disrupted demand drawn this tick
# MAGIC - **Blue fill**: fulfilled units (came from stock)
# MAGIC - **Red fill**: unmet units (stockout)
# MAGIC - **Red shading**: disruption-active ticks (demand spike will be visible here)

# COMMAND ----------

fig = dash.plot_demand()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Chart 3 - Cost breakdown per tick
# MAGIC
# MAGIC Stacked bar showing the four cost components each tick:
# MAGIC - **Blue**: holding cost
# MAGIC - **Red**: stockout penalty
# MAGIC - **Green**: order cost (fixed + variable)
# MAGIC - **Orange**: transit loss cost

# COMMAND ----------

fig = dash.plot_costs()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Chart 4 - Cumulative cost over time
# MAGIC
# MAGIC Running total cost per item.
# MAGIC Dashed horizontal line shows remaining budget at end of run (if budget was set).

# COMMAND ----------

fig = dash.plot_cumulative_cost()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Chart 5 - Reorder decisions
# MAGIC
# MAGIC - **Green bars**: order quantity placed this tick
# MAGIC - **Grey bars**: hold decisions (zero height, shown for completeness)
# MAGIC - **Dashed line** (right axis): stock on hand at decision time
# MAGIC - **Red shading**: disruption-active ticks

# COMMAND ----------

fig = dash.plot_decisions()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Raw data spot-checks
# MAGIC
# MAGIC Quick tabular views for manual verification alongside the charts.

# COMMAND ----------

# MAGIC %md ### Stock on hand - final tick per item

# COMMAND ----------

display(spark.sql(f'''
    SELECT item_id,
           MAX(tick)                                         AS final_tick,
           MAX_BY(stock_on_hand, tick)                      AS final_stock,
           MAX_BY(stock_in_transit, tick)                   AS final_in_transit
    FROM {CATALOG}.tables4ops.ops_warehouse_state
    WHERE sim_id = '{SIM_ID}'
    GROUP BY item_id
    ORDER BY item_id
'''))

# COMMAND ----------

# MAGIC %md ### Total unmet demand per item

# COMMAND ----------

display(spark.sql(f'''
    SELECT item_id,
           SUM(unmet_demand)     AS total_unmet,
           SUM(fulfilled_demand) AS total_fulfilled,
           COUNT(*)              AS ticks_with_demand
    FROM {CATALOG}.tables4hist.hist_demand_actuals
    WHERE sim_id = '{SIM_ID}'
    GROUP BY item_id
    ORDER BY item_id
'''))

# COMMAND ----------

# MAGIC %md ### Cost totals per item

# COMMAND ----------

display(spark.sql(f'''
    SELECT item_id,
           SUM(holding_cost)      AS total_holding,
           SUM(stockout_cost)     AS total_stockout,
           SUM(order_cost)        AS total_order,
           SUM(transit_loss_cost) AS total_transit_loss,
           SUM(total_cost)        AS grand_total
    FROM {CATALOG}.tables4hist.hist_cost_by_tick
    WHERE sim_id = '{SIM_ID}'
    GROUP BY item_id
    ORDER BY item_id
'''))

# COMMAND ----------

# MAGIC %md ### Decision summary per item

# COMMAND ----------

display(spark.sql(f'''
    SELECT item_id,
           decision,
           COUNT(*)        AS count,
           SUM(order_qty)  AS total_units_ordered
    FROM {CATALOG}.tables4hist.hist_reorder_decisions
    WHERE sim_id = '{SIM_ID}'
    GROUP BY item_id, decision
    ORDER BY item_id, decision
'''))

# COMMAND ----------

# MAGIC %md ### Event log summary

# COMMAND ----------

display(spark.sql(f'''
    SELECT event_type, COUNT(*) AS count
    FROM {CATALOG}.tables4eventlog.event_log
    WHERE sim_id = '{SIM_ID}'
    GROUP BY event_type
    ORDER BY count DESC
'''))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Inline assertions
# MAGIC
# MAGIC Sanity checks that the visualisation data is consistent with
# MAGIC the underlying tables.

# COMMAND ----------

# Stock never negative
min_stock = spark.sql(f'''
    SELECT MIN(stock_on_hand) AS min_stock
    FROM {CATALOG}.tables4ops.ops_warehouse_state
    WHERE sim_id = '{SIM_ID}'
''').collect()[0]["min_stock"]
assert min_stock >= 0, f"Negative stock found: {min_stock}"

# `hist_cost_by_tick` total matches `ops_cost_accumulator` final cumulative
for item_id in dash._items():
    hist_total = spark.sql(f'''
        SELECT SUM(total_cost) AS s
        FROM {CATALOG}.tables4hist.hist_cost_by_tick
        WHERE sim_id = '{SIM_ID}' AND item_id = '{item_id}'
    ''').collect()[0]["s"] or 0.0

    cum_total = spark.sql(f'''
        SELECT cumulative_total_cost
        FROM {CATALOG}.tables4ops.ops_cost_accumulator
        WHERE sim_id = '{SIM_ID}' AND item_id = '{item_id}'
        ORDER BY tick DESC LIMIT 1
    ''').collect()[0]["cumulative_total_cost"]

    assert abs(hist_total - cum_total) < 0.01, \
        f"{item_id}: per-tick sum {hist_total:.4f} ≠ cumulative {cum_total:.4f}"

# Every demand tick has `fulfilled + unmet = floor(disrupted_demand)`
bad_demand = spark.sql(f'''
    SELECT COUNT(*) AS n
    FROM {CATALOG}.tables4hist.hist_demand_actuals
    WHERE sim_id = '{SIM_ID}'
      AND (fulfilled_demand + unmet_demand) != CAST(FLOOR(disrupted_demand) AS INT)
''').collect()[0]["n"]
assert bad_demand == 0, f"{bad_demand} demand rows have inconsistent fulfilled+unmet"

# `SIM_STARTED` and `SIM_ENDED` both present
event_types = {r["event_type"] for r in spark.sql(f'''
    SELECT DISTINCT event_type FROM {CATALOG}.tables4eventlog.event_log
    WHERE sim_id = '{SIM_ID}'
''').collect()}
assert "SIM_STARTED" in event_types, "SIM_STARTED missing from event_log"
assert "SIM_ENDED"   in event_types, "SIM_ENDED missing from event_log"

print("[DONE] All dashboard assertions passed.")
print(f"  min stock on hand : {min_stock}")
print(f"  event types logged: {len(event_types)}")