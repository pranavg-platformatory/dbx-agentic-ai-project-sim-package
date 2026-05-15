# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 2 - World Setup & Stage 3 - Pattern Sampling
# MAGIC ### Notebook Test & Inspection
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Constructs a `SimWorld` entirely in Python (no manual SQL)
# MAGIC 2. Calls `write_world` to persist it into the env tables
# MAGIC 3. Calls `load_world` to verify the round-trip
# MAGIC 4. Exercises the `PatternSampler` across all distribution types
# MAGIC 5. Runs inline assertions to confirm correctness
# MAGIC
# MAGIC **Depends on**: Stage 1 modules (`config/models.py`, `config/loader.py`)
# MAGIC 
# MAGIC **Run cells top to bottom. Each cell is independently re-runnable.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **NOTE: Relation to stage 1**:
# MAGIC
# MAGIC The stage 1 notebook went the other way (seeded tables manually, then called `load_world`). Stage 2 + 3 flips it: you define the world in Python, call `setup.write_world(spark, world)`, then optionally call `load_world` to verify the round-trip.

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

PACKAGE_ROOT = "/Workspace/Repos/reshmaupadhyaya5@gmail.com/dbx-agentic-ai-project-sim-package"
if PACKAGE_ROOT not in sys.path:
    sys.path.insert(0, PACKAGE_ROOT)

print("Python path updated.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Constants

# COMMAND ----------

CATALOG = "hackathon_of_the_century"
SIM_ID  = "sim_stage2_001"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Build a SimWorld entirely in Python
# MAGIC
# MAGIC No SQL, no manual table writes - just construct Pydantic models
# MAGIC and hand them to `write_world`.
# MAGIC
# MAGIC Toy world:
# MAGIC - 3 items: `item_A` (Poisson demand), `item_B` (Normal demand), `item_C` (custom schedule)
# MAGIC - 2 suppliers: `sup_001` supplies A+B, `sup_002` supplies C
# MAGIC - 1 consumer: `con_001` demands all 3 items
# MAGIC - 1 stochastic disruption on `item_A`, 1 deterministic transit loss on `item_C`

# COMMAND ----------

from datetime import datetime, timezone
from warehouse_sim.config.models import (
    Consumer,
    DisruptionSchedule,
    DisruptionType,
    Distribution,
    ItemType,
    Pattern,
    PatternRole,
    PatternType,
    RunMode,
    SimConfig,
    SimWorld,
    Supplier,
    TickUnit,
)

NOW = datetime.now(timezone.utc)

# -- SimConfig ---------------------------------------------------------------
config = SimConfig(
    sim_id                     = SIM_ID,
    random_seed                = 99,
    num_ticks                  = 50,
    run_mode                   = RunMode.FINITE,
    tick_unit                  = TickUnit.DAY,
    budget_limit               = 25_000.0,
    budget_warning_threshold   = 0.10,
    agent_history_window_ticks = 10,
    start_timestamp            = NOW,
    created_at                 = NOW,
)

# -- Items -------------------------------------------------------------------
items = {
    "item_A": ItemType(
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
    "item_B": ItemType(
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
    "item_C": ItemType(
        item_id                         = "item_C",
        item_name                       = "Thingamajig C",
        unit_value                      = 3.0,
        initial_stock                   = 200,
        reorder_point                   = 30,
        min_order_qty                   = 20,
        max_order_qty                   = 300,
        holding_cost_per_unit_per_tick  = 0.02,
        stockout_cost_per_unit_per_tick = 1.0,
        order_fixed_cost                = 20.0,
        order_variable_cost_per_unit    = 2.5,
        transit_loss_cost_per_unit      = 3.5,
    ),
}

# -- Suppliers ---------------------------------------------------------------
suppliers = {
    "sup_001": Supplier(
        supplier_id           = "sup_001",
        supplier_name         = "Acme Corp",
        base_lead_time_ticks  = 3,
        lead_time_variability = 0.5,
    ),
    "sup_002": Supplier(
        supplier_id           = "sup_002",
        supplier_name         = "Globex Ltd",
        base_lead_time_ticks  = 5,
        lead_time_variability = 1.0,
    ),
}

# -- Consumers ---------------------------------------------------------------
consumers = {
    "con_001": Consumer(consumer_id="con_001", consumer_name="Retail Division"),
}

# -- Mappings ----------------------------------------------------------------
supplier_item_map = {
    "item_A": "sup_001",
    "item_B": "sup_001",
    "item_C": "sup_002",
}
consumer_item_map = {
    "item_A": "con_001",
    "item_B": "con_001",
    "item_C": "con_001",
}

# -- Demand patterns ---------------------------------------------------------
demand_patterns = {
    "item_A": Pattern(
        pattern_id                    = "pat_A_demand",
        sim_id                        = SIM_ID,
        item_id                       = "item_A",
        role                          = PatternRole.DEMAND,
        pattern_type                  = PatternType.STATISTICAL,
        distribution                  = Distribution.POISSON,
        dist_params                   = {"mu": 50},
        noise_std                     = 0.0,
        seasonal_multiplier_schedule  = [1.0, 1.2, 1.0, 0.8, 1.0, 1.3, 0.9],
    ),
    "item_B": Pattern(
        pattern_id   = "pat_B_demand",
        sim_id       = SIM_ID,
        item_id      = "item_B",
        role         = PatternRole.DEMAND,
        pattern_type = PatternType.STATISTICAL,
        distribution = Distribution.NORMAL,
        dist_params  = {"mu": 20, "sigma": 3},
        noise_std    = 1.0,
    ),
    "item_C": Pattern(
        pattern_id      = "pat_C_demand",
        sim_id          = SIM_ID,
        item_id         = "item_C",
        role            = PatternRole.DEMAND,
        pattern_type    = PatternType.CUSTOM,
        custom_schedule = [10.0, 15.0, 20.0, 15.0, 10.0],
        noise_std       = 0.0,
    ),
}

# -- Disruptions -------------------------------------------------------------
disruptions = [
    DisruptionSchedule(
        disruption_id       = "dis_A_stochastic_spike",
        sim_id              = SIM_ID,
        item_id             = "item_A",
        disruption_type     = DisruptionType.DEMAND_SPIKE,
        start_tick          = 10,
        end_tick            = 20,
        magnitude           = 2.5,
        is_stochastic       = True,
        trigger_probability = 0.4,
    ),
    DisruptionSchedule(
        disruption_id       = "dis_C_transit_loss",
        sim_id              = SIM_ID,
        item_id             = "item_C",
        disruption_type     = DisruptionType.TRANSIT_LOSS,
        start_tick          = 30,
        end_tick            = 35,
        magnitude           = 0.3,
        is_stochastic       = False,
        trigger_probability = None,
    ),
]

# -- Assemble SimWorld -------------------------------------------------------
world = SimWorld(
    config            = config,
    items             = items,
    suppliers         = suppliers,
    consumers         = consumers,
    supplier_item_map = supplier_item_map,
    consumer_item_map = consumer_item_map,
    demand_patterns   = demand_patterns,
    supply_patterns   = {},
    disruptions       = disruptions,
)

print(f"SimWorld built: {len(world.items)} items, "
      f"{len(world.suppliers)} suppliers, "
      f"{len(world.disruptions)} disruptions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write the world to env tables
# MAGIC
# MAGIC `write_world` handles deleting any prior rows for this `SIM_ID` before
# MAGIC inserting, so this cell is safe to re-run.

# COMMAND ----------

from warehouse_sim.world.setup import write_world

write_world(spark, world)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Inspect the written env tables

# COMMAND ----------

# MAGIC %md ### `env_sim_config`

# COMMAND ----------

display(spark.sql(f"""
    SELECT * FROM {CATALOG}.tables4env.env_sim_config
    WHERE sim_id = '{SIM_ID}'
"""))

# COMMAND ----------

# MAGIC %md ### `env_item_types`

# COMMAND ----------

display(spark.sql(f"""
    SELECT * FROM {CATALOG}.tables4env.env_item_types
    WHERE item_id IN ('item_A', 'item_B', 'item_C')
    ORDER BY item_id
"""))

# COMMAND ----------

# MAGIC %md ### `env_suppliers`

# COMMAND ----------

display(spark.sql(f"""
    SELECT *
    FROM {CATALOG}.tables4env.env_suppliers
    WHERE supplier_id IN ('sup_001', 'sup_002')
    ORDER BY supplier_id
"""))

# COMMAND ----------

# MAGIC %md ### Supplier and consumer mappings (joined)

# COMMAND ----------

display(spark.sql(f"""
    SELECT s.item_id,
           s.supplier_id, sup.supplier_name, sup.base_lead_time_ticks,
           c.consumer_id, con.consumer_name
    FROM {CATALOG}.tables4env.env_supplier_item_map s
    JOIN {CATALOG}.tables4env.env_suppliers sup ON s.supplier_id = sup.supplier_id
    JOIN {CATALOG}.tables4env.env_consumer_item_map c
      ON s.sim_id = c.sim_id AND s.item_id = c.item_id
    JOIN {CATALOG}.tables4env.env_consumers con ON c.consumer_id = con.consumer_id
    WHERE s.sim_id = '{SIM_ID}'
    ORDER BY s.item_id
"""))

# COMMAND ----------

# MAGIC %md ### `env_patterns`

# COMMAND ----------

display(spark.sql(f"""
    SELECT *
    FROM {CATALOG}.tables4env.env_patterns
    WHERE sim_id = '{SIM_ID}'
    ORDER BY item_id
"""))

# COMMAND ----------

# MAGIC %md ### `env_disruption_schedule`

# COMMAND ----------

display(spark.sql(f"""
    SELECT *
    FROM {CATALOG}.tables4env.env_disruption_schedule
    WHERE sim_id = '{SIM_ID}'
    ORDER BY item_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Round-trip verification via load_world
# MAGIC
# MAGIC `write_world` wrote the models -> `load_world` reads them back.
# MAGIC The two must agree on every field.

# COMMAND ----------

from warehouse_sim.config.loader import load_world

world_rt = load_world(spark, sim_id=SIM_ID)

print("Round-trip SimWorld:")
print(f"  sim_id:      {world_rt.config.sim_id}")
print(f"  num_ticks:   {world_rt.config.num_ticks}")
print(f"  items:       {sorted(world_rt.items.keys())}")
print(f"  suppliers:   {sorted(world_rt.suppliers.keys())}")
print(f"  disruptions: {len(world_rt.disruptions)}")

for item_id in sorted(world_rt.items.keys()):
    sup = world_rt.supplier_for(item_id)
    con = world_rt.consumer_for(item_id)
    pat = world_rt.demand_patterns[item_id]
    print(f"\n  {item_id}")
    print(f"    supplier : {sup.supplier_id} - {sup.supplier_name} (lead time {sup.base_lead_time_ticks} ticks)")
    print(f"    consumer : {con.consumer_id} - {con.consumer_name}")
    print(f"    pattern  : {pat.pattern_type.value} / {pat.distribution}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. `PatternSampler` - live sampling inspection

# COMMAND ----------

from warehouse_sim.world.patterns import PatternSampler

sampler = PatternSampler(seed=world_rt.config.random_seed)

# COMMAND ----------

# MAGIC %md ### `item_A` - Poisson with weekly seasonal multiplier

# COMMAND ----------

pat_a   = world_rt.demand_patterns["item_A"]
samples = [sampler.sample(pat_a, tick=t) for t in range(14)]

print("item_A demand (ticks 0-13, Poisson mu=50 + weekly seasonal multiplier):")
for t, v in enumerate(samples):
    print(f"  tick {t:>2}: {v:>4}  {'█' * (v // 5)}")

# COMMAND ----------

# MAGIC %md ### `item_B` - Normal(mu=20, sigma=3) with noise

# COMMAND ----------

pat_b   = world_rt.demand_patterns["item_B"]
samples = [sampler.sample(pat_b, tick=t) for t in range(14)]

print("item_B demand (ticks 0-13, Normal + noise_std=1.0):")
for t, v in enumerate(samples):
    print(f"  tick {t:>2}: {v:>4}  {'█' * (v // 2)}")

# COMMAND ----------

# MAGIC %md ### `item_C` - custom schedule `[10, 15, 20, 15, 10]` cycling every 5 ticks

# COMMAND ----------

pat_c   = world_rt.demand_patterns["item_C"]
samples = [sampler.sample(pat_c, tick=t) for t in range(15)]

print("item_C demand (ticks 0-14, custom schedule cycling every 5 ticks):")
for t, v in enumerate(samples):
    print(f"  tick {t:>2} [cycle pos {t % 5}]: {v:>4}  {'█' * (v // 2)}")

# COMMAND ----------

# MAGIC %md ### Lead time sampling - both suppliers

# COMMAND ----------

lt_sampler = PatternSampler(seed=42)
sup_001    = world_rt.suppliers["sup_001"]  # base=3, variability=0.5
sup_002    = world_rt.suppliers["sup_002"]  # base=5, variability=1.0

lt_001 = [lt_sampler.sample_lead_time(sup_001.base_lead_time_ticks, sup_001.lead_time_variability) for _ in range(20)]
lt_002 = [lt_sampler.sample_lead_time(sup_002.base_lead_time_ticks, sup_002.lead_time_variability) for _ in range(20)]

print(f"sup_001 lead times (base=3, var=0.5): {lt_001}")
print(f"  min={min(lt_001)}, max={max(lt_001)}, mean={sum(lt_001)/len(lt_001):.1f}")
print()
print(f"sup_002 lead times (base=5, var=1.0): {lt_002}")
print(f"  min={min(lt_002)}, max={max(lt_002)}, mean={sum(lt_002)/len(lt_002):.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Inline assertions - round-trip + sampling correctness

# COMMAND ----------

import math

# -- Config round-trip -------------------------------------------------------
assert world_rt.config.sim_id      == SIM_ID, "sim_id mismatch"
assert world_rt.config.num_ticks   == 50,     "num_ticks mismatch"
assert world_rt.config.random_seed == 99,     "random_seed mismatch"

# -- Entity sets -------------------------------------------------------------
assert set(world_rt.items.keys())     == {"item_A", "item_B", "item_C"}, "item set mismatch"
assert set(world_rt.suppliers.keys()) == {"sup_001", "sup_002"},         "supplier set mismatch"
assert set(world_rt.consumers.keys()) == {"con_001"},                    "consumer set mismatch"

# -- Mappings ----------------------------------------------------------------
assert world_rt.supplier_for("item_A").supplier_id == "sup_001", "item_A supplier wrong"
assert world_rt.supplier_for("item_B").supplier_id == "sup_001", "item_B supplier wrong"
assert world_rt.supplier_for("item_C").supplier_id == "sup_002", "item_C supplier wrong"
assert world_rt.consumer_for("item_A").consumer_id == "con_001", "item_A consumer wrong"

# -- Pattern round-trip ------------------------------------------------------
assert world_rt.demand_patterns["item_A"].distribution.value == "poisson",  "item_A dist wrong"
assert world_rt.demand_patterns["item_A"].dist_params         == {"mu": 50}, "item_A params wrong"
assert world_rt.demand_patterns["item_A"].seasonal_multiplier_schedule is not None, "seasonal schedule lost"
assert len(world_rt.demand_patterns["item_A"].seasonal_multiplier_schedule) == 7,   "seasonal schedule length wrong"
assert world_rt.demand_patterns["item_B"].noise_std           == 1.0,        "item_B noise_std wrong"
assert world_rt.demand_patterns["item_C"].pattern_type.value  == "custom",   "item_C type wrong"
assert world_rt.demand_patterns["item_C"].custom_schedule == [10.0, 15.0, 20.0, 15.0, 10.0], "schedule wrong"

# -- Disruptions -------------------------------------------------------------
assert len(world_rt.disruptions) == 2,                                           "disruption count wrong"
assert world_rt.disruptions_for_tick(10)[0].item_id      == "item_A",           "disruption tick 10 wrong"
assert world_rt.disruptions_for_tick(9)                   == [],                 "tick 9 should have no disruptions"
assert world_rt.disruptions_for_tick(30)[0].disruption_type.value == "transit_loss", "item_C disruption type wrong"
assert world_rt.disruptions_for_tick(36)                  == [],                 "tick 36 should have no disruptions"

# -- Sampling output contract: non-negative integers -------------------------
check_sampler = PatternSampler(seed=99)
for item_id, pat in world_rt.demand_patterns.items():
    for tick in range(50):
        result = check_sampler.sample(pat, tick)
        assert isinstance(result, int), f"{item_id} tick {tick}: not an int, got {type(result)}"
        assert result >= 0,             f"{item_id} tick {tick}: negative result {result}"

# -- Custom schedule cycling -------------------------------------------------
cs      = PatternSampler(seed=0)
pat_c_rt = world_rt.demand_patterns["item_C"]
assert cs.sample(pat_c_rt, tick=0) == cs.sample(pat_c_rt, tick=5), "custom schedule not cycling at offset 5"
assert cs.sample(pat_c_rt, tick=2) == cs.sample(pat_c_rt, tick=7), "custom schedule not cycling at offset 7"

# -- Lead time floor ---------------------------------------------------------
lt_check = PatternSampler(seed=0)
for _ in range(300):
    lt = lt_check.sample_lead_time(base_ticks=2, variability=10.0)
    assert lt >= 1, f"Lead time {lt} violated floor of 1"

print("[DONE] All assertions passed - Stage 2 looks good.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Teardown (optional)
# MAGIC
# MAGIC Uncomment and run to remove the toy data inserted by this notebook. Shared entities (items, suppliers, consumers) are commented out separately since they may be referenced by other sim runs.

# COMMAND ----------

# from warehouse_sim.world.setup import teardown_world
# teardown_world(spark, sim_id=SIM_ID)
# # Shared entity cleanup - only if not needed by other sim runs
# spark.sql(f"DELETE FROM {CATALOG}.tables4env.env_item_types WHERE item_id IN ('item_A', 'item_B', 'item_C')")
# spark.sql(f"DELETE FROM {CATALOG}.tables4env.env_suppliers WHERE supplier_id IN ('sup_001', 'sup_002')")
# spark.sql(f"DELETE FROM {CATALOG}.tables4env.env_consumers WHERE consumer_id = 'con_001'")
# print("Teardown complete.")