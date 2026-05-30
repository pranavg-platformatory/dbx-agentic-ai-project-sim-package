# Databricks notebook source
# MAGIC %md
# MAGIC <h1>Integration Test 1 - Run Notebook<br><i>Rule-Based vs. LLM Agent for Finite Ticks</i></h1>
# MAGIC
# MAGIC **Purpose**: Execute both simulations and write results to Delta tables.
# MAGIC Run this notebook first. The **Analyse** notebook can be re-run independently once this completes.
# MAGIC
# MAGIC **Structure**:
# MAGIC - **Section 1.1** - Install dependencies
# MAGIC - **Section 1.2** - Parameters
# MAGIC - **Section 1.3** - Imports and path setup
# MAGIC - **Section 1.4** - Clean up prior data
# MAGIC - **Section 1.5** - Build and write `SimWorld`
# MAGIC - **Section 1.6** - Run Agent 1: `RuleBasedAgent`
# MAGIC - **Section 1.7** - Run Agent 2: `LLMAgentWrapper`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Install dependencies

# COMMAND ----------

# MAGIC %pip install pydantic numpy matplotlib pandas
# MAGIC %pip install databricks-langchain langgraph
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Parameters
# MAGIC
# MAGIC Set once here. All subsequent cells reference these variables.
# MAGIC `SIM_SEED` must be identical for both agents for the comparison to be fair.
# MAGIC `executor_trigger_every_n_ticks` controls how often the LLM agent wrapper
# MAGIC dispatches its background executor. With 20 ticks and a trigger of 4, the
# MAGIC executor fires at ticks 4, 8, 12, 16, 20 - five dispatches over the run.

# COMMAND ----------

# ── Simulation ──────────────────────────────────────────────────────────────
SIM_SEED   = 42
NUM_TICKS  = 20
TICK_UNIT  = "hour"    # cosmetic label only; used in plot axis titles

# ── sim_ids - one per agent, never mixed ────────────────────────────────────
SIM_ID_RULEBASED = "integration_test_1_rulebased_001"
SIM_ID_LLM       = "integration_test_1_llm_001"

# ── Catalog ──────────────────────────────────────────────────────────────────
CATALOG = "hackathon_of_the_century"

# ── LLMAgentWrapper ──────────────────────────────────────────────────────────
# executor_trigger_every_n_ticks: how often (in ticks) the background executor
# fires. 4 gives 5 dispatches over 20 ticks - enough to observe behaviour
# without making the run prohibitively slow.
EXECUTOR_TRIGGER_N  = 4

# LLM agent package path. Must be on sys.path before LLMAgentWrapper is
# constructed with stub_mode=None. See [DEP-5] in llm_agent_wrapper.py.
LLM_AGENT_PACKAGE_PATH = "/Workspace/Shared/reorder-llm-agent"

# Override any LLMReorderAgent config.yml field here as needed.
# Set warehouse_id and llm_endpoint for the test environment.
LLM_AGENT_CONFIG_OVERRIDE = {
    # "warehouse_id": "your_warehouse_id",
    # "llm_endpoint": "databricks-meta-llama-3-3-70b-instruct",
}

print("Parameters set.")
print(f"  SIM_ID_RULEBASED : {SIM_ID_RULEBASED}")
print(f"  SIM_ID_LLM       : {SIM_ID_LLM}")
print(f"  NUM_TICKS        : {NUM_TICKS}")
print(f"  SIM_SEED         : {SIM_SEED}")
print(f"  EXECUTOR_TRIGGER : every {EXECUTOR_TRIGGER_N} ticks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Imports and path setup

# COMMAND ----------

import sys
from datetime import datetime, timezone

# Simulation package
PACKAGE_ROOT = "/Workspace/Repos/mistermilvusmigrans@gmail.com/dbx-agentic-ai-project-sim-package"
if PACKAGE_ROOT not in sys.path:
    sys.path.insert(0, PACKAGE_ROOT)

# LLM agent package - required for LLMAgentWrapper with stub_mode=None
# See [DEP-5] in warehouse_sim/agent/llm_agent_wrapper.py
if LLM_AGENT_PACKAGE_PATH not in sys.path:
    sys.path.insert(0, LLM_AGENT_PACKAGE_PATH)

from warehouse_sim.config.models import (
    Consumer, DisruptionSchedule, DisruptionType, Distribution,
    ItemType, Pattern, PatternRole, PatternType, RunMode,
    SimConfig, SimWorld, Supplier, TickUnit,
)
from warehouse_sim.world.setup import write_world
from warehouse_sim.config.loader import load_world
from warehouse_sim.world.patterns import PatternSampler
from warehouse_sim.event_log.event_log import EventLogger
from warehouse_sim.engine.runner import SimRunner
from warehouse_sim.agent.rule_based_agent import RuleBasedAgent
from warehouse_sim.agent.llm_agent_wrapper import LLMAgentWrapper
from warehouse_sim.config.llm_agent_wrapper_config import LLMAgentWrapperConfig

print("Imports resolved.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Clean up prior data for both sim_ids
# MAGIC
# MAGIC Deletes all rows for `SIM_ID_RULEBASED` and `SIM_ID_LLM` from every
# MAGIC ops, hist, and event table. Append-only tables are temporarily unlocked
# MAGIC for the delete and re-locked immediately after.
# MAGIC
# MAGIC Idempotent: safe to re-run if the simulation has already been run.

# COMMAND ----------

_APPEND_ONLY_TABLES = [
    f"{CATALOG}.tables4ops.ops_warehouse_state",
    f"{CATALOG}.tables4ops.ops_active_disruptions",
    f"{CATALOG}.tables4ops.ops_cost_accumulator",
    f"{CATALOG}.tables4hist.hist_demand_actuals",
    f"{CATALOG}.tables4hist.hist_supply_arrivals",
    f"{CATALOG}.tables4hist.hist_reorder_decisions",
    f"{CATALOG}.tables4hist.hist_cost_by_tick",
    f"{CATALOG}.tables4hist.hist_eval_metrics",
    f"{CATALOG}.tables4eventlog.event_log",
]
_MUTABLE_TABLES = [
    f"{CATALOG}.tables4ops.ops_pending_orders",
    f"{CATALOG}.tables4ops.ops_escalation_queue",
]
_ENV_TABLES_WITH_SIM_ID = [
    f"{CATALOG}.tables4env.env_sim_config",
    f"{CATALOG}.tables4env.env_supplier_item_map",
    f"{CATALOG}.tables4env.env_consumer_item_map",
    f"{CATALOG}.tables4env.env_patterns",
    f"{CATALOG}.tables4env.env_disruption_schedule",
]

for sim_id in [SIM_ID_RULEBASED, SIM_ID_LLM]:
    print(f"Clearing data for sim_id='{sim_id}'...")

    for table in _APPEND_ONLY_TABLES:
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.appendOnly' = 'false')")
        spark.sql(f"DELETE FROM {table} WHERE sim_id = '{sim_id}'")
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.appendOnly' = 'true')")

    for table in _MUTABLE_TABLES:
        try:
            spark.sql(f"DELETE FROM {table} WHERE sim_id = '{sim_id}'")
        except Exception:
            pass  # table may not exist yet if escalation queue was never written

    for table in _ENV_TABLES_WITH_SIM_ID:
        spark.sql(f"DELETE FROM {table} WHERE sim_id = '{sim_id}'")

print("[DONE] Prior data cleared for both sim_ids.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Build and write `SimWorld`
# MAGIC
# MAGIC A single `SimWorld` spec is written for each agent's `sim_id`.
# MAGIC Both agents load from their own env tables but share the same world
# MAGIC definition and seed.
# MAGIC
# MAGIC **World spec**:
# MAGIC - 2 items (Widget A, Gadget B), 2 suppliers, 1 consumer
# MAGIC - 5-tick cyclic custom demand patterns with no noise for determinism
# MAGIC - Deterministic lead times (variability=0) for fair comparison
# MAGIC - 3 disruptions: 1 demand spike, 1 demand suppression, 1 transit delay
# MAGIC - Budget: 10,000

# COMMAND ----------

NOW = datetime.now(timezone.utc)

def _build_world(sim_id: str) -> SimWorld:
    return SimWorld(
        config = SimConfig(
            sim_id                     = sim_id,
            random_seed                = SIM_SEED,
            num_ticks                  = NUM_TICKS,
            run_mode                   = RunMode.FINITE,
            tick_unit                  = TickUnit.HOUR,
            budget_limit               = 10_000.0,
            budget_warning_threshold   = 0.20,
            agent_history_window_ticks = 10,
            start_timestamp            = NOW,
            created_at                 = NOW,
        ),
        items = {
            "item_A": ItemType(
                item_id                         = "item_A",
                item_name                       = "Widget A",
                unit_value                      = 5.0,
                initial_stock                   = 40,
                reorder_point                   = 15,
                min_order_qty                   = 10,
                max_order_qty                   = 80,
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
                initial_stock                   = 25,
                reorder_point                   = 10,
                min_order_qty                   = 5,
                max_order_qty                   = 60,
                holding_cost_per_unit_per_tick  = 0.10,
                stockout_cost_per_unit_per_tick = 5.0,
                order_fixed_cost                = 30.0,
                order_variable_cost_per_unit    = 10.0,
                transit_loss_cost_per_unit      = 15.0,
            ),
        },
        suppliers = {
            "sup_001": Supplier(
                supplier_id           = "sup_001",
                supplier_name         = "Acme Corp",
                base_lead_time_ticks  = 3,
                lead_time_variability = 0.0,  # deterministic: required for fair comparison
            ),
            "sup_002": Supplier(
                supplier_id           = "sup_002",
                supplier_name         = "Globex Ltd",
                base_lead_time_ticks  = 4,
                lead_time_variability = 0.0,
            ),
        },
        consumers = {
            "con_001": Consumer(consumer_id="con_001", consumer_name="Retail Division"),
        },
        supplier_item_map = {"item_A": "sup_001", "item_B": "sup_002"},
        consumer_item_map = {"item_A": "con_001", "item_B": "con_001"},
        demand_patterns = {
            "item_A": Pattern(
                pattern_id   = f"{sim_id}__item_A__demand",
                sim_id       = sim_id,
                item_id      = "item_A",
                role         = PatternRole.DEMAND,
                pattern_type = PatternType.CUSTOM,
                # 5-tick cycle: moderate baseline with a mid-cycle peak
                custom_schedule = [8.0, 10.0, 13.0, 10.0, 8.0],
                noise_std       = 0.0,
            ),
            "item_B": Pattern(
                pattern_id   = f"{sim_id}__item_B__demand",
                sim_id       = sim_id,
                item_id      = "item_B",
                role         = PatternRole.DEMAND,
                pattern_type = PatternType.CUSTOM,
                custom_schedule = [4.0, 5.0, 7.0, 5.0, 4.0],
                noise_std       = 0.0,
            ),
        },
        supply_patterns = {},
        disruptions = [
            # Demand spike on item_A: ticks 6-9 (tests agent response to surge)
            DisruptionSchedule(
                disruption_id     = f"{sim_id}__dis_demand_spike",
                sim_id            = sim_id,
                item_id           = "item_A",
                disruption_type   = DisruptionType.DEMAND_SPIKE,
                start_tick        = 6,
                end_tick          = 9,
                magnitude         = 2.0,
                is_stochastic     = False,
                trigger_probability = None,
            ),
            # Demand suppression on item_B: ticks 12-15 (tests agent response to slowdown)
            DisruptionSchedule(
                disruption_id     = f"{sim_id}__dis_demand_suppression",
                sim_id            = sim_id,
                item_id           = "item_B",
                disruption_type   = DisruptionType.DEMAND_SUPPRESSION,
                start_tick        = 12,
                end_tick          = 15,
                magnitude         = 0.4,
                is_stochastic     = False,
                trigger_probability = None,
            ),
            # Transit delay on item_A: ticks 14-17 (tests agent response to supply disruption)
            DisruptionSchedule(
                disruption_id     = f"{sim_id}__dis_transit_delay",
                sim_id            = sim_id,
                item_id           = "item_A",
                disruption_type   = DisruptionType.TRANSIT_DELAY,
                start_tick        = 14,
                end_tick          = 17,
                magnitude         = 1.5,
                is_stochastic     = False,
                trigger_probability = None,
            ),
        ],
    )

# Write world for both sim_ids (each needs its own env_sim_config row
# and pattern/disruption rows scoped to its sim_id)
for sid in [SIM_ID_RULEBASED, SIM_ID_LLM]:
    write_world(spark, _build_world(sid))
    print(f"  Written: {sid}")

print("[DONE] SimWorld written for both sim_ids.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 Run Agent 1 - `RuleBasedAgent`

# COMMAND ----------

world_rb  = load_world(spark, sim_id=SIM_ID_RULEBASED)
sampler   = PatternSampler(seed=world_rb.config.random_seed)
logger    = EventLogger(spark, sim_id=SIM_ID_RULEBASED)
agent_rb  = RuleBasedAgent()
runner_rb = SimRunner(spark, world_rb, agent_rb, logger, sampler)

print(f"Running RuleBasedAgent for {NUM_TICKS} ticks (sim_id='{SIM_ID_RULEBASED}')...")
runner_rb.run()
print(f"[DONE] RuleBasedAgent run complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.7 Run Agent 2 - `LLMAgentWrapper`
# MAGIC
# MAGIC This run will take longer than the rule-based run due to LLM call
# MAGIC latency on each executor trigger tick. With `EXECUTOR_TRIGGER_N=4`
# MAGIC and 20 ticks, the executor fires 5 times.
# MAGIC
# MAGIC The **Analyse** notebook can be re-run independently once this cell completes.

# COMMAND ----------

world_llm   = load_world(spark, sim_id=SIM_ID_LLM)
sampler_llm = PatternSampler(seed=world_llm.config.random_seed)
logger_llm  = EventLogger(spark, sim_id=SIM_ID_LLM)

llm_wrapper_config = LLMAgentWrapperConfig(
    executor_trigger_every_n_ticks   = EXECUTOR_TRIGGER_N,
    context_obsolescence_threshold_k = None,  # resolved to min lead time (3) at init
    queue_size                       = 1,
    stub_mode                        = None,  # live LLM call via LLMReorderAgent
    suppress_write_tools             = True,  # runner owns hist_reorder_decisions writes
    llm_agent_config_override        = LLM_AGENT_CONFIG_OVERRIDE or None,
)

agent_llm = LLMAgentWrapper(
    spark  = spark,
    world  = world_llm,
    config = llm_wrapper_config,
    logger = logger_llm,
)

runner_llm = SimRunner(spark, world_llm, agent_llm, logger_llm, sampler_llm)

print(f"Running LLMAgentWrapper for {NUM_TICKS} ticks (sim_id='{SIM_ID_LLM}')...")
print(f"  executor_trigger_every_n_ticks = {EXECUTOR_TRIGGER_N}")
print(f"  resolved_k (obsolescence)      = {agent_llm._resolved_k} ticks")
runner_llm.run()
print(f"[DONE] LLMAgentWrapper run complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.8 Teardown (optional)
# MAGIC
# MAGIC Uncomment to delete all data written by this notebook.
# MAGIC Env table rows for these sim_ids are also removed.

# COMMAND ----------

# for sim_id in [SIM_ID_RULEBASED, SIM_ID_LLM]:
#     print(f"Tearing down sim_id='{sim_id}'...")
#     for table in _APPEND_ONLY_TABLES:
#         spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.appendOnly' = 'false')")
#         spark.sql(f"DELETE FROM {table} WHERE sim_id = '{sim_id}'")
#         spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.appendOnly' = 'true')")
#     for table in _MUTABLE_TABLES:
#         try:
#             spark.sql(f"DELETE FROM {table} WHERE sim_id = '{sim_id}'")
#         except Exception:
#             pass
#     for table in _ENV_TABLES_WITH_SIM_ID:
#         spark.sql(f"DELETE FROM {table} WHERE sim_id = '{sim_id}'")
# print("Teardown complete.")
