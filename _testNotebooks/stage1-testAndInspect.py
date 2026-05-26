# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 1 - Data Models & Config Loader
# MAGIC ### Notebook Test & Inspection
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Installs dependencies
# MAGIC 2. Inserts toy data into the env tables
# MAGIC 3. Calls `load_world` to build a `SimWorld`
# MAGIC 4. Inspects the result at each step
# MAGIC
# MAGIC **Run cells top to bottom. Each cell is independently re-runnable.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Key implementation details for this notebook**:
# MAGIC
# MAGIC - `%restart_python` after `%pip install` - needed in Databricks to actually load the new packages
# MAGIC - Explicit schema strings on createDataFrame for tables with non-obvious types (booleans, arrays, nullable doubles) rather than relying on PySpark inference
# MAGIC - Explicit `.cast()` selects for integer columns that PySpark might infer as long
# MAGIC - f-string in teardown - the Stage 1 notebook had a bug ("`DELETE FROM {CATALOG}...`" without the f prefix on the last two lines)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Install dependencies

# COMMAND ----------

# MAGIC %pip install pydantic
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Add the package to the Python path
# MAGIC
# MAGIC Adjust `PACKAGE_ROOT` to wherever you've placed the `warehouse_sim` folder in your Databricks workspace / repo.

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

CATALOG = "hackathon_of_the_century"
SIM_ID  = "sim_test_001"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Seed env tables with toy data
# MAGIC
# MAGIC Toy world:
# MAGIC - 2 item types: `item_A`, `item_B`
# MAGIC - 1 supplier: `sup_001` (supplies both items)
# MAGIC - 1 consumer: `con_001` (demands both items)
# MAGIC - 1 statistical demand pattern per item (Poisson)
# MAGIC - 1 deterministic disruption on `item_A`
# MAGIC
# MAGIC **Re-running this cell deletes and re-inserts rows for `SIM_ID` - safe to repeat.**

# COMMAND ----------

# DBTITLE 1,Cell 9
from datetime import datetime, timezone
from pyspark.sql import Row
from pyspark.sql.functions import col

NOW_STR = datetime.now(timezone.utc).isoformat()
NOW_TS  = datetime.now(timezone.utc)


# -- env_sim_config ----------------------------------------------------------
spark.sql(f'''
    DELETE FROM {CATALOG}.tables4env.env_sim_config
    WHERE sim_id = '{SIM_ID}'
''')

spark.createDataFrame([Row(
    sim_id                     = SIM_ID,
    random_seed                = 42,
    num_ticks                  = 30,
    run_mode                   = "finite",
    tick_unit                  = "day",
    budget_limit               = 10000.0,
    budget_warning_threshold   = 0.10,
    agent_history_window_ticks = 7,
    start_timestamp            = NOW_TS,
    created_at                 = NOW_TS,
)]).select(
    col("sim_id"),
    col("random_seed"),
    col("num_ticks").cast("int").alias("num_ticks"),
    col("run_mode"),
    col("tick_unit"),
    col("budget_limit"),
    col("budget_warning_threshold"),
    col("agent_history_window_ticks").cast("int").alias("agent_history_window_ticks"),
    col("start_timestamp"),
    col("created_at"),
).write.mode("append").saveAsTable(f"{CATALOG}.tables4env.env_sim_config")

print("[DONE] env_sim_config seeded")


# -- env_item_types ----------------------------------------------------------
# (no `sim_id` column - shared across runs)
for item_id, item_name in [("item_A", "Widget A"), ("item_B", "Gadget B")]:
    spark.sql(f'''
        DELETE FROM {CATALOG}.tables4env.env_item_types
        WHERE item_id = '{item_id}'
    ''')

spark.createDataFrame([
    Row(
        item_id                         = "item_A",
        item_name                       = "Widget A",
        unit_value                      = 5.0,
        initial_stock                   = 100,
        reorder_point                   = 20,
        min_order_qty                   = 10,
        max_order_qty                   = 200,
        holding_cost_per_unit_per_tick  = 0.05,
        stockout_cost_per_unit_per_tick = 2.0,
        order_fixed_cost                = 50.0,
        order_variable_cost_per_unit    = 4.5,
        transit_loss_cost_per_unit      = 6.0,
    ),
    Row(
        item_id                         = "item_B",
        item_name                       = "Gadget B",
        unit_value                      = 12.0,
        initial_stock                   = 50,
        reorder_point                   = 10,
        min_order_qty                   = 5,
        max_order_qty                   = 100,
        holding_cost_per_unit_per_tick  = 0.10,
        stockout_cost_per_unit_per_tick = 5.0,
        order_fixed_cost                = 30.0,
        order_variable_cost_per_unit    = 10.0,
        transit_loss_cost_per_unit      = 15.0,
    ),
]).select(
    col("item_id"),
    col("item_name"),
    col("unit_value"),
    col("initial_stock").cast("int").alias("initial_stock"),
    col("reorder_point").cast("int").alias("reorder_point"),
    col("min_order_qty").cast("int").alias("min_order_qty"),
    col("max_order_qty").cast("int").alias("max_order_qty"),
    col("holding_cost_per_unit_per_tick"),
    col("stockout_cost_per_unit_per_tick"),
    col("order_fixed_cost"),
    col("order_variable_cost_per_unit"),
    col("transit_loss_cost_per_unit"),
).write.mode("append").saveAsTable(f"{CATALOG}.tables4env.env_item_types")

print("[DONE] env_item_types seeded")


# -- `env_suppliers` -----------------------------------------------------------
spark.sql(f'''
    DELETE FROM {CATALOG}.tables4env.env_suppliers
    WHERE supplier_id = 'sup_001'
''')

spark.createDataFrame([Row(
    supplier_id           = "sup_001",
    supplier_name         = "Acme Corp",
    base_lead_time_ticks  = 3,
    lead_time_variability = 0.5,
)]).select(
    col("supplier_id"),
    col("supplier_name"),
    col("base_lead_time_ticks").cast("int").alias("base_lead_time_ticks"),
    col("lead_time_variability"),
).write.mode("append").saveAsTable(f"{CATALOG}.tables4env.env_suppliers")

print("[DONE] env_suppliers seeded")


# -- env_consumers -----------------------------------------------------------
spark.sql(f'''
    DELETE FROM {CATALOG}.tables4env.env_consumers
    WHERE consumer_id = 'con_001'
''')

spark.createDataFrame([Row(
    consumer_id   = "con_001",
    consumer_name = "Retail Division",
)]).write.mode("append").saveAsTable(f"{CATALOG}.tables4env.env_consumers")

print("[DONE] env_consumers seeded")


# -- `env_supplier_item_map` ---------------------------------------------------
spark.sql(f'''
    DELETE FROM {CATALOG}.tables4env.env_supplier_item_map
    WHERE sim_id = '{SIM_ID}'
''')

spark.createDataFrame([
    Row(sim_id=SIM_ID, supplier_id="sup_001", item_id="item_A"),
    Row(sim_id=SIM_ID, supplier_id="sup_001", item_id="item_B"),
]).write.mode("append").saveAsTable(f"{CATALOG}.tables4env.env_supplier_item_map")

print("[DONE] env_supplier_item_map seeded")


# -- `env_consumer_item_map` ---------------------------------------------------
spark.sql(f'''
    DELETE FROM {CATALOG}.tables4env.env_consumer_item_map
    WHERE sim_id = '{SIM_ID}'
''')

spark.createDataFrame([
    Row(sim_id=SIM_ID, consumer_id="con_001", item_id="item_A"),
    Row(sim_id=SIM_ID, consumer_id="con_001", item_id="item_B"),
]).write.mode("append").saveAsTable(f"{CATALOG}.tables4env.env_consumer_item_map")

print("[DONE] env_consumer_item_map seeded")


# -- `env_patterns` ------------------------------------------------------------
spark.sql(f'''
    DELETE FROM {CATALOG}.tables4env.env_patterns
    WHERE sim_id = '{SIM_ID}'
''')

import json

spark.createDataFrame([
    Row(
        pattern_id                    = "pat_A_demand",
        sim_id                        = SIM_ID,
        item_id                       = "item_A",
        role                          = "demand",
        pattern_type                  = "statistical",
        distribution                  = "poisson",
        dist_params                   = json.dumps({"mu": 50}),
        custom_schedule               = None,
        seasonal_multiplier_schedule  = None,
        noise_std                     = 0.0,
        supplier_id                   = None,
    ),
    Row(
        pattern_id                    = "pat_B_demand",
        sim_id                        = SIM_ID,
        item_id                       = "item_B",
        role                          = "demand",
        pattern_type                  = "statistical",
        distribution                  = "normal",
        dist_params                   = json.dumps({"mu": 20, "sigma": 3}),
        custom_schedule               = None,
        seasonal_multiplier_schedule  = None,
        noise_std                     = 0.0,
        supplier_id                   = None,
    ),
], schema="pattern_id string, sim_id string, item_id string, role string, pattern_type string, distribution string, dist_params string, custom_schedule array<double>, seasonal_multiplier_schedule array<double>, noise_std double, supplier_id string").write.mode("append").saveAsTable(f"{CATALOG}.tables4env.env_patterns")

print("[DONE] env_patterns seeded")


# -- `env_disruption_schedule` -------------------------------------------------
spark.sql(f'''
    DELETE FROM {CATALOG}.tables4env.env_disruption_schedule
    WHERE sim_id = '{SIM_ID}'
''')

spark.createDataFrame([
    Row(
        disruption_id       = "dis_A_spike",
        sim_id              = SIM_ID,
        item_id             = "item_A",
        disruption_type     = "demand_spike",
        start_tick          = 5,
        end_tick            = 10,
        magnitude           = 2.0,
        is_stochastic       = False,
        trigger_probability = None,
    ),
], schema="disruption_id string, sim_id string, item_id string, disruption_type string, start_tick int, end_tick int, magnitude double, is_stochastic boolean, trigger_probability double").write.mode("append").saveAsTable(f"{CATALOG}.tables4env.env_disruption_schedule")

print("[DONE] env_disruption_schedule seeded")
print("\nAll env tables seeded for sim_id =", SIM_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inspect the raw env tables

# COMMAND ----------

# MAGIC %md ### `env_sim_config`

# COMMAND ----------

display(spark.sql(f'''
    SELECT * FROM {CATALOG}.tables4env.env_sim_config
    WHERE sim_id = '{SIM_ID}'
'''))

# COMMAND ----------

# MAGIC %md ### `env_item_types`

# COMMAND ----------

display(spark.sql(f'''
    SELECT * FROM {CATALOG}.tables4env.env_item_types WHERE item_id IN ('item_A', 'item_B')
'''))

# COMMAND ----------

# MAGIC %md ### `env_supplier_item_map` + `env_consumer_item_map`

# COMMAND ----------

display(spark.sql(f'''
    SELECT s.sim_id, s.item_id, s.supplier_id, sup.supplier_name,
           c.consumer_id, con.consumer_name
    FROM {CATALOG}.tables4env.env_supplier_item_map s
    JOIN {CATALOG}.tables4env.env_suppliers sup ON s.supplier_id = sup.supplier_id
    JOIN {CATALOG}.tables4env.env_consumer_item_map c ON s.sim_id = c.sim_id AND s.item_id = c.item_id
    JOIN {CATALOG}.tables4env.env_consumers con ON c.consumer_id = con.consumer_id
    WHERE s.sim_id = '{SIM_ID}'
'''))

# COMMAND ----------

# MAGIC %md ### `env_patterns`

# COMMAND ----------

display(spark.sql(f'''
    SELECT pattern_id, item_id, role, pattern_type, distribution, dist_params, noise_std
    FROM {CATALOG}.tables4env.env_patterns
    WHERE sim_id = '{SIM_ID}'
'''))

# COMMAND ----------

# MAGIC %md ### `env_disruption_schedule`

# COMMAND ----------

display(spark.sql(f'''
    SELECT * FROM {CATALOG}.tables4env.env_disruption_schedule
    WHERE sim_id = '{SIM_ID}'
'''))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load the SimWorld

# COMMAND ----------

from warehouse_sim.config.loader import load_world

world = load_world(spark, sim_id=SIM_ID)

print(f"sim_id:      {world.config.sim_id}")
print(f"run_mode:    {world.config.run_mode}")
print(f"num_ticks:   {world.config.num_ticks}")
print(f"tick_unit:   {world.config.tick_unit}")
print(f"budget:      {world.config.budget_limit}")
print(f"items:       {list(world.items.keys())}")
print(f"suppliers:   {list(world.suppliers.keys())}")
print(f"consumers:   {list(world.consumers.keys())}")
print(f"disruptions: {len(world.disruptions)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Inspect the SimWorld object

# COMMAND ----------

# MAGIC %md ### Items

# COMMAND ----------

for item_id, item in world.items.items():
    print(f"\n{item_id} - {item.item_name}")
    print(f"  initial_stock   : {item.initial_stock}")
    print(f"  reorder_point   : {item.reorder_point}")
    print(f"  order qty range : {item.min_order_qty} - {item.max_order_qty}")
    print(f"  holding cost    : {item.holding_cost_per_unit_per_tick} / unit / tick")
    print(f"  stockout cost   : {item.stockout_cost_per_unit_per_tick} / unit / tick")

# COMMAND ----------

# MAGIC %md ### Supplier and consumer resolution per item

# COMMAND ----------

for item_id in world.items:
    sup = world.supplier_for(item_id)
    con = world.consumer_for(item_id)
    print(f"{item_id}  ->  supplier: {sup.supplier_name} (lead time: {sup.base_lead_time_ticks} ticks)  |  consumer: {con.consumer_name}")

# COMMAND ----------

# MAGIC %md ### Demand patterns

# COMMAND ----------

for item_id, pat in world.demand_patterns.items():
    print(f"{item_id}  ->  {pat.pattern_type.value} / {pat.distribution} / params: {pat.dist_params}")

# COMMAND ----------

# MAGIC %md ### Disruptions

# COMMAND ----------

for d in world.disruptions:
    print(f"{d.disruption_id}  |  {d.item_id}  |  {d.disruption_type.value}  |  ticks {d.start_tick}-{d.end_tick}  |  magnitude: {d.magnitude}  |  stochastic: {d.is_stochastic}")

# COMMAND ----------

# MAGIC %md ### `disruptions_for_tick()` helper

# COMMAND ----------

for tick in [0, 5, 7, 10, 11]:
    active = world.disruptions_for_tick(tick)
    label  = ", ".join(d.disruption_id for d in active) if active else "none"
    print(f"tick {tick:>2}  ->  active disruptions: {label}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation smoke tests (inline assertions)
# MAGIC
# MAGIC These replicate the key assertions from `test_stage1_models.py` against the live-loaded world, so you can confirm the loader and the tables agree.

# COMMAND ----------

assert world.config.sim_id == SIM_ID,                        "sim_id mismatch"
assert world.config.num_ticks == 30,                         "num_ticks mismatch"
assert set(world.items.keys()) == {"item_A", "item_B"},      "unexpected item set"
assert world.supplier_for("item_A").supplier_id == "sup_001","supplier resolution failed"
assert world.supplier_for("item_B").supplier_id == "sup_001","supplier resolution failed"
assert world.consumer_for("item_A").consumer_id == "con_001","consumer resolution failed"
assert "item_A" in world.demand_patterns,                    "missing demand pattern for item_A"
assert "item_B" in world.demand_patterns,                    "missing demand pattern for item_B"
assert world.demand_patterns["item_A"].dist_params == {"mu": 50}, "dist_params mismatch"
assert len(world.disruptions) == 1,                          "expected 1 disruption"
assert world.disruptions_for_tick(5)[0].disruption_id == "dis_A_spike", "disruption tick lookup failed"
assert world.disruptions_for_tick(11) == [],                 "disruption should not be active at tick 11"
assert world.items["item_A"].max_order_qty >= world.items["item_A"].min_order_qty, "order qty constraint violated"

print("[DONE] All assertions passed - Stage 1 looks good.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Teardown (optional)
# MAGIC
# MAGIC Uncomment and run to remove the toy data inserted by this notebook.

# COMMAND ----------

spark.sql(f"DELETE FROM {CATALOG}.tables4env.env_sim_config WHERE sim_id = '{SIM_ID}'")
spark.sql(f"DELETE FROM {CATALOG}.tables4env.env_supplier_item_map WHERE sim_id = '{SIM_ID}'")
spark.sql(f"DELETE FROM {CATALOG}.tables4env.env_consumer_item_map WHERE sim_id = '{SIM_ID}'")
spark.sql(f"DELETE FROM {CATALOG}.tables4env.env_patterns WHERE sim_id = '{SIM_ID}'")
spark.sql(f"DELETE FROM {CATALOG}.tables4env.env_disruption_schedule WHERE sim_id = '{SIM_ID}'")
spark.sql(f"DELETE FROM {CATALOG}.tables4env.env_item_types WHERE item_id IN ('item_A', 'item_B')")
spark.sql("DELETE FROM {CATALOG}.tables4env.env_suppliers WHERE supplier_id = 'sup_001'")
spark.sql("DELETE FROM {CATALOG}.tables4env.env_consumers WHERE consumer_id = 'con_001'")
print("Teardown complete.")