# Databricks notebook source
# MAGIC %md
# MAGIC <h1>Integration Test 1:<br><i>Rule-Based vs. LLM Agent for Finite Ticks</i></h1>
# MAGIC
# MAGIC **Test plan**: [`_testNotebooks/integrationTesting/integrationTest-1/README.md`](./README.md)
# MAGIC
# MAGIC **Structure**:
# MAGIC - **Section 1**: Run - all simulation execution. No plots, no queries.
# MAGIC - **Section 2**: Analyse - all plots, summary tables, and evaluation queries. Reads only from Delta tables. Re-runnable independently once Section 1 has completed.
# MAGIC
# MAGIC **Run cells top to bottom. Section 2 can be re-run independently.**

# COMMAND ----------

# MAGIC %md
# MAGIC # Section 1: Run

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Install dependencies

# COMMAND ----------

# MAGIC %pip install pydantic numpy matplotlib pandas
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
import json
import warnings
from datetime import datetime, timezone

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

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
# MAGIC A single `SimWorld` is written once and loaded by both agents.
# MAGIC Both agents load from the same `sim_id` env tables but write their
# MAGIC ops/hist/event data to their own `sim_id`.
# MAGIC
# MAGIC **World spec**:
# MAGIC - 2 items (Widget A, Gadget B), 2 suppliers, 1 consumer
# MAGIC - 5-tick cyclic custom demand patterns with no noise for determinism
# MAGIC - Deterministic lead times (variability=0) for fair comparison
# MAGIC - 3 disruptions: 1 demand spike, 1 demand suppression, 1 transit delay
# MAGIC - Budget: 10,000
# MAGIC
# MAGIC The env tables are shared between both sim_ids. The `sim_id` written
# MAGIC here is `SIM_ID_RULEBASED`; `SIM_ID_LLM` is a copy written below
# MAGIC so both agents can load their own world config independently.

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
# MAGIC Section 2 can be re-run independently once this cell completes.

# COMMAND ----------

world_llm  = load_world(spark, sim_id=SIM_ID_LLM)
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
# MAGIC ---
# MAGIC # Section 2: Analyse
# MAGIC
# MAGIC All cells below read only from Delta tables. No simulation code.
# MAGIC Re-run from here if any plot or query fails without re-running Section 1.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.0 Load data into pandas
# MAGIC
# MAGIC All data is loaded once here. Plot and table cells reference these DataFrames.

# COMMAND ----------

ALL_SIM_IDS    = f"('{SIM_ID_RULEBASED}', '{SIM_ID_LLM}')"
AGENT_LABELS   = {SIM_ID_RULEBASED: "Rule-Based", SIM_ID_LLM: "LLM"}
AGENT_COLOURS  = {SIM_ID_RULEBASED: "#4C72B0",    SIM_ID_LLM: "#DD8452"}
TICKS          = list(range(NUM_TICKS))

# Disruptions (same world, so only one sim_id needed - use rule-based)
df_disruptions = spark.sql(f"""
    SELECT d.sim_id, d.tick, d.disruption_id, d.disruption_type,
           d.effective_magnitude, d.is_active_this_tick,
           s.start_tick, s.end_tick, s.magnitude AS scheduled_magnitude,
           s.item_id
    FROM {CATALOG}.tables4ops.ops_active_disruptions d
    JOIN {CATALOG}.tables4env.env_disruption_schedule s
      ON d.disruption_id = s.disruption_id
    WHERE d.sim_id = '{SIM_ID_RULEBASED}'
    ORDER BY d.tick, d.disruption_id
""").toPandas()

# Demand
df_demand = spark.sql(f"""
    SELECT sim_id, tick, item_id, raw_demand, disrupted_demand,
           fulfilled_demand, unmet_demand
    FROM {CATALOG}.tables4hist.hist_demand_actuals
    WHERE sim_id IN {ALL_SIM_IDS}
    ORDER BY sim_id, tick, item_id
""").toPandas()

# Pending orders / lead times
df_orders = spark.sql(f"""
    SELECT sim_id, order_tick, item_id, order_qty, expected_arrival_tick, status,
           (expected_arrival_tick - order_tick) AS actual_lead_time
    FROM {CATALOG}.tables4ops.ops_pending_orders
    WHERE sim_id IN {ALL_SIM_IDS}
    ORDER BY sim_id, order_tick, item_id
""").toPandas()

# Cost accumulator - final tick per item per sim
df_costs_final = spark.sql(f"""
    SELECT c.*
    FROM {CATALOG}.tables4ops.ops_cost_accumulator c
    JOIN (
        SELECT sim_id, item_id, MAX(tick) AS max_tick
        FROM {CATALOG}.tables4ops.ops_cost_accumulator
        WHERE sim_id IN {ALL_SIM_IDS}
        GROUP BY sim_id, item_id
    ) latest ON c.sim_id = latest.sim_id
           AND c.item_id = latest.item_id
           AND c.tick    = latest.max_tick
""").toPandas()

# Decisions
df_decisions = spark.sql(f"""
    SELECT sim_id, tick, item_id, decision, order_qty,
           stock_on_hand_at_decision, stock_in_transit_at_decision,
           agent_version, agent_reasoning
    FROM {CATALOG}.tables4hist.hist_reorder_decisions
    WHERE sim_id IN {ALL_SIM_IDS}
    ORDER BY sim_id, tick, item_id
""").toPandas()

# Stockout events
df_stockouts = spark.sql(f"""
    SELECT sim_id, tick, item_id,
           CAST(get_json_object(payload, '$.unmet_demand')  AS DOUBLE) AS unmet_demand,
           CAST(get_json_object(payload, '$.stockout_cost') AS DOUBLE) AS stockout_cost
    FROM {CATALOG}.tables4eventlog.event_log
    WHERE sim_id IN {ALL_SIM_IDS}
      AND event_type = 'STOCKOUT_OCCURRED'
    ORDER BY sim_id, tick, item_id
""").toPandas()

# Eval metrics (monitoring loop - written every tick by LLMAgentWrapper)
df_eval_metrics = spark.sql(f"""
    SELECT sim_id, tick, item_id, metric_name, metric_value, logged_at
    FROM {CATALOG}.tables4hist.hist_eval_metrics
    WHERE sim_id IN {ALL_SIM_IDS}
    ORDER BY sim_id, tick, metric_name
""").toPandas()

# Fallback/failure events (LLM run only)
df_agent_health = spark.sql(f"""
    SELECT tick, item_id, event_type, payload, logged_at
    FROM {CATALOG}.tables4eventlog.event_log
    WHERE sim_id = '{SIM_ID_LLM}'
      AND event_type IN ('FALLBACK_STRUCTURAL', 'FALLBACK_LOGICAL',
                         'EXECUTOR_ALL_STALE', 'AGENT_ERROR')
    ORDER BY tick
""").toPandas()

# Escalations (LLM run only)
df_escalations = spark.sql(f"""
    SELECT tick, item_id, reason, status, raised_at, context_json
    FROM {CATALOG}.tables4ops.ops_escalation_queue
    WHERE sim_id = '{SIM_ID_LLM}'
    ORDER BY tick, item_id
""").toPandas()

print("Data loaded.")
print(f"  df_disruptions   : {len(df_disruptions)} rows")
print(f"  df_demand        : {len(df_demand)} rows")
print(f"  df_orders        : {len(df_orders)} rows")
print(f"  df_costs_final   : {len(df_costs_final)} rows")
print(f"  df_decisions     : {len(df_decisions)} rows")
print(f"  df_stockouts     : {len(df_stockouts)} rows")
print(f"  df_eval_metrics  : {len(df_eval_metrics)} rows")
print(f"  df_agent_health  : {len(df_agent_health)} rows")
print(f"  df_escalations   : {len(df_escalations)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Plots

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plot 1 - Disruptions across ticks
# MAGIC
# MAGIC Source: `ops_active_disruptions` joined to `env_disruption_schedule`.
# MAGIC
# MAGIC One subplot per disruption type. `effective_magnitude` per tick;
# MAGIC ticks where `is_active_this_tick=False` shown as zero. Shaded band
# MAGIC marks the scheduled window.
# MAGIC
# MAGIC **Sanity check**: this plot should be identical for both sim_ids
# MAGIC (same world, same seed). Any difference indicates a seed or world
# MAGIC config problem.

# COMMAND ----------

disruption_types = df_disruptions["disruption_type"].unique()
n_types = len(disruption_types)

fig, axes = plt.subplots(n_types, 1, figsize=(14, 3 * n_types), sharex=True)
if n_types == 1:
    axes = [axes]

fig.suptitle("Plot 1 - Disruptions Across Ticks", fontsize=13, fontweight="bold")

for ax, dtype in zip(axes, disruption_types):
    subset = df_disruptions[df_disruptions["disruption_type"] == dtype].copy()
    subset["plot_magnitude"] = subset.apply(
        lambda r: r["effective_magnitude"] if r["is_active_this_tick"] else 0.0, axis=1
    )

    for _, group in subset.groupby("disruption_id"):
        row = group.iloc[0]
        ax.axvspan(row["start_tick"], row["end_tick"], alpha=0.12, color="red",
                   label=f"Scheduled window ({row['disruption_id']})")
        ax.plot(group["tick"], group["plot_magnitude"], marker="o", linewidth=1.5,
                label=f"{row['disruption_id']} ({row['item_id']})")

    ax.set_title(dtype.replace("_", " ").title(), fontsize=10)
    ax.set_ylabel("Effective magnitude")
    ax.legend(fontsize=8)
    ax.grid(axis="y", alpha=0.3)

axes[-1].set_xlabel(f"Tick ({TICK_UNIT})")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plot 2 - Demand across ticks
# MAGIC
# MAGIC Source: `hist_demand_actuals`.
# MAGIC
# MAGIC `raw_demand` (dashed) and `disrupted_demand` (solid) per item.
# MAGIC Demand-disruption active ticks shaded red.
# MAGIC One figure per agent - demand draw is identical; fulfilment may differ.

# COMMAND ----------

demand_disruption_ticks = set(
    df_disruptions.loc[
        (df_disruptions["is_active_this_tick"]) &
        (df_disruptions["disruption_type"].isin(["demand_spike", "demand_suppression"])),
        "tick"
    ].tolist()
)

items = sorted(df_demand["item_id"].unique())
n_items = len(items)

for sim_id in [SIM_ID_RULEBASED, SIM_ID_LLM]:
    fig, axes = plt.subplots(n_items, 1, figsize=(14, 4 * n_items), sharex=True)
    if n_items == 1:
        axes = [axes]
    fig.suptitle(f"Plot 2 - Demand Across Ticks [{AGENT_LABELS[sim_id]}]",
                 fontsize=13, fontweight="bold")

    subset = df_demand[df_demand["sim_id"] == sim_id]
    for ax, item_id in zip(axes, items):
        item_data = subset[subset["item_id"] == item_id].sort_values("tick")
        for tick in demand_disruption_ticks:
            ax.axvspan(tick - 0.5, tick + 0.5, alpha=0.15, color="red")
        ax.plot(item_data["tick"], item_data["raw_demand"],
                linestyle="--", alpha=0.6, label="raw_demand", color=AGENT_COLOURS[sim_id])
        ax.plot(item_data["tick"], item_data["disrupted_demand"],
                linestyle="-", label="disrupted_demand", color=AGENT_COLOURS[sim_id])
        ax.set_title(item_id, fontsize=10)
        ax.set_ylabel("Units")
        ax.legend(fontsize=8)
        ax.grid(axis="y", alpha=0.3)

    axes[-1].set_xlabel(f"Tick ({TICK_UNIT})")
    red_patch = mpatches.Patch(color="red", alpha=0.3, label="Demand disruption active")
    fig.legend(handles=[red_patch], loc="upper right", fontsize=8)
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plot 3 - Actual average lead time per item
# MAGIC
# MAGIC Source: `ops_pending_orders`.
# MAGIC
# MAGIC `AVG(expected_arrival_tick - order_tick)` per item per agent.
# MAGIC Reflects actual lead times including any transit_delay disruption
# MAGIC multiplier applied at order placement - not the configured baseline.
# MAGIC Items with no orders placed show no bar.

# COMMAND ----------

if df_orders.empty:
    print("No orders placed by either agent. Plot 3 skipped.")
else:
    lead_time_summary = (
        df_orders.groupby(["sim_id", "item_id"])["actual_lead_time"]
        .mean()
        .reset_index()
        .rename(columns={"actual_lead_time": "avg_lead_time"})
    )

    pivot = lead_time_summary.pivot(index="item_id", columns="sim_id", values="avg_lead_time")

    fig, ax = plt.subplots(figsize=(10, 5))
    fig.suptitle("Plot 3 - Actual Average Lead Time per Item", fontsize=13, fontweight="bold")

    x      = range(len(pivot.index))
    width  = 0.35
    for i, sim_id in enumerate([SIM_ID_RULEBASED, SIM_ID_LLM]):
        if sim_id in pivot.columns:
            vals = pivot[sim_id].values
            bars = ax.bar(
                [xi + i * width for xi in x], vals, width,
                label=AGENT_LABELS[sim_id], color=AGENT_COLOURS[sim_id], alpha=0.85
            )
            for bar, val in zip(bars, vals):
                if not pd.isna(val):
                    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                            f"{val:.1f}", ha="center", va="bottom", fontsize=8)

    ax.set_xticks([xi + width / 2 for xi in x])
    ax.set_xticklabels(pivot.index)
    ax.set_xlabel("Item")
    ax.set_ylabel(f"Average lead time ({TICK_UNIT}s)")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plot 4 - Cost accrued per item (stacked)
# MAGIC
# MAGIC Source: `ops_cost_accumulator` at MAX(tick).
# MAGIC
# MAGIC Stacked bars per item per agent.
# MAGIC Identifies which cost component drives the difference between agents.
# MAGIC
# MAGIC **Check Table 5 (agent health) before interpreting this plot.**
# MAGIC If fallback events are present for the LLM run, those ticks used
# MAGIC `RuleBasedAgent` decisions - the comparison is not fully clean.

# COMMAND ----------

cost_cols = [
    ("cumulative_holding_cost",      "Holding",      "#4878CF"),
    ("cumulative_stockout_cost",     "Stockout",     "#D65F5F"),
    ("cumulative_order_cost",        "Order",        "#6ACC65"),
    ("cumulative_transit_loss_cost", "Transit Loss", "#B47CC7"),
]

items_sorted = sorted(df_costs_final["item_id"].unique())
n_items      = len(items_sorted)
fig, axes    = plt.subplots(1, n_items, figsize=(6 * n_items, 6), sharey=False)
if n_items == 1:
    axes = [axes]

fig.suptitle("Plot 4 - Cost Accrued per Item (End of Run)", fontsize=13, fontweight="bold")

bar_width = 0.35
for ax, item_id in zip(axes, items_sorted):
    subset = df_costs_final[df_costs_final["item_id"] == item_id]
    for i, sim_id in enumerate([SIM_ID_RULEBASED, SIM_ID_LLM]):
        row = subset[subset["sim_id"] == sim_id]
        if row.empty:
            continue
        row = row.iloc[0]
        bottom = 0.0
        x_pos  = i * bar_width
        for col, label, colour in cost_cols:
            val = row.get(col, 0.0) or 0.0
            ax.bar(x_pos, val, bar_width, bottom=bottom, color=colour,
                   label=label if i == 0 else "", alpha=0.85)
            bottom += val
        ax.text(x_pos, bottom + 10, f"{bottom:.0f}", ha="center", fontsize=8)

    ax.set_title(item_id, fontsize=10)
    ax.set_xticks([0, bar_width])
    ax.set_xticklabels([AGENT_LABELS[s] for s in [SIM_ID_RULEBASED, SIM_ID_LLM]], fontsize=9)
    ax.set_ylabel("Cumulative cost")
    ax.grid(axis="y", alpha=0.3)

handles = [mpatches.Patch(color=c, label=l) for _, l, c in cost_cols]
fig.legend(handles=handles, loc="upper right", fontsize=9)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Plots 5 & 6 - Demand fulfilment and reorder decisions
# MAGIC
# MAGIC Plots 5 and 6 share an x-axis and are rendered as a two-panel figure
# MAGIC per agent.
# MAGIC
# MAGIC **Plot 5** (top panel): `fulfilled_demand` and `unmet_demand` per item.
# MAGIC Ticks with `STOCKOUT_OCCURRED` events shaded red.
# MAGIC
# MAGIC **Plot 6** (bottom panel): reorder decisions. Markers at ticks where
# MAGIC `decision='reorder'`, sized by `order_qty`. Hold ticks shown at zero.
# MAGIC Makes the relationship between reorder timing and stockouts traceable.

# COMMAND ----------

for sim_id in [SIM_ID_RULEBASED, SIM_ID_LLM]:
    label  = AGENT_LABELS[sim_id]
    colour = AGENT_COLOURS[sim_id]

    demand_sub    = df_demand[df_demand["sim_id"] == sim_id]
    decision_sub  = df_decisions[df_decisions["sim_id"] == sim_id]
    stockout_ticks = set(df_stockouts[df_stockouts["sim_id"] == sim_id]["tick"].tolist())

    for item_id in items:
        fig, (ax5, ax6) = plt.subplots(
            2, 1, figsize=(14, 7), sharex=True,
            gridspec_kw={"height_ratios": [2, 1]}
        )
        fig.suptitle(
            f"Plots 5 & 6 - Fulfilment and Reorders | {label} | {item_id}",
            fontsize=12, fontweight="bold"
        )

        # ── Plot 5: demand fulfilment ─────────────────────────────────────
        d = demand_sub[demand_sub["item_id"] == item_id].sort_values("tick")
        for tick in stockout_ticks:
            ax5.axvspan(tick - 0.5, tick + 0.5, alpha=0.20, color="red")
        ax5.plot(d["tick"], d["fulfilled_demand"],
                 label="fulfilled", color=colour, linewidth=1.8)
        ax5.plot(d["tick"], d["unmet_demand"],
                 label="unmet (stockout)", color="red", linewidth=1.4, linestyle="--")
        ax5.set_ylabel("Units")
        ax5.legend(fontsize=9)
        ax5.grid(axis="y", alpha=0.3)

        # ── Plot 6: reorder decisions ─────────────────────────────────────
        dec = decision_sub[decision_sub["item_id"] == item_id].sort_values("tick")
        reorders = dec[dec["decision"] == "reorder"]
        holds    = dec[dec["decision"] == "hold"]
        ax6.plot(holds["tick"], [0] * len(holds),
                 "_", color="grey", alpha=0.4, markersize=8, label="hold")
        if not reorders.empty:
            ax6.scatter(reorders["tick"], reorders["order_qty"],
                        s=reorders["order_qty"] * 8, color=colour, alpha=0.8,
                        zorder=3, label="reorder (sized by qty)")
        ax6.set_ylabel(f"Order qty ({TICK_UNIT})")
        ax6.set_xlabel(f"Tick ({TICK_UNIT})")
        ax6.legend(fontsize=9)
        ax6.grid(axis="y", alpha=0.3)

        red_patch = mpatches.Patch(color="red", alpha=0.3, label="Stockout tick")
        ax5.legend(handles=ax5.get_legend_handles_labels()[0] + [red_patch],
                   labels=ax5.get_legend_handles_labels()[1] + ["Stockout tick"],
                   fontsize=9)
        plt.tight_layout()
        plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Summary Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 1 - Disruption summary
# MAGIC
# MAGIC Source: `ops_active_disruptions` joined to `env_disruption_schedule`.
# MAGIC
# MAGIC Since disruptions are seeded identically, this table primarily confirms
# MAGIC correct world replication between agents. Any difference indicates a problem.

# COMMAND ----------

table1 = (
    df_disruptions
    .groupby(["disruption_id", "disruption_type", "item_id", "start_tick", "end_tick"])
    .agg(
        ticks_active          = ("is_active_this_tick", "sum"),
        avg_effective_magnitude = ("effective_magnitude", "mean"),
        max_effective_magnitude = ("effective_magnitude", "max"),
    )
    .reset_index()
    .sort_values(["start_tick", "disruption_id"])
)

print("Table 1 - Disruption Summary")
display(table1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 2 - Reorder decisions
# MAGIC
# MAGIC Source: `hist_reorder_decisions`.
# MAGIC
# MAGIC `agent_reasoning` is populated by `LLMReorderAgent` (LLM run only; NULL
# MAGIC for rule-based). Worth reading manually for the LLM run - it is the most
# MAGIC direct window into the LLM's decision logic.

# COMMAND ----------

table2 = df_decisions[[
    "sim_id", "tick", "item_id", "decision", "order_qty",
    "stock_on_hand_at_decision", "stock_in_transit_at_decision",
    "agent_version", "agent_reasoning",
]].sort_values(["sim_id", "tick", "item_id"])

print("Table 2 - Reorder Decisions")
display(table2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 3 - Demand fulfilment summary
# MAGIC
# MAGIC Source: `hist_demand_actuals`.

# COMMAND ----------

table3 = (
    df_demand
    .groupby(["sim_id", "item_id"])
    .agg(
        total_raw_demand       = ("raw_demand",       "sum"),
        total_disrupted_demand = ("disrupted_demand", "sum"),
        total_fulfilled        = ("fulfilled_demand", "sum"),
        total_unmet            = ("unmet_demand",     "sum"),
    )
    .reset_index()
)
table3["fulfilment_rate"] = (
    table3["total_fulfilled"] / table3["total_disrupted_demand"].replace(0, float("nan"))
).round(4)
table3 = table3.sort_values(["sim_id", "item_id"])

print("Table 3 - Demand Fulfilment Summary")
display(table3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 4 - Stockout summary
# MAGIC
# MAGIC Source: `event_log` (`STOCKOUT_OCCURRED` events).

# COMMAND ----------

if df_stockouts.empty:
    print("Table 4 - Stockout Summary: no stockouts occurred in either run.")
else:
    table4 = (
        df_stockouts
        .groupby(["sim_id", "item_id"])
        .agg(
            stockout_ticks       = ("tick",          "count"),
            total_unmet_demand   = ("unmet_demand",  "sum"),
            total_stockout_cost  = ("stockout_cost", "sum"),
        )
        .reset_index()
        .sort_values(["sim_id", "item_id"])
    )
    print("Table 4 - Stockout Summary")
    display(table4)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 5 - Agent health summary *(LLM run only)*
# MAGIC
# MAGIC Source: `event_log` - diagnostic events for `SIM_ID_LLM` only.
# MAGIC
# MAGIC **Check this table before interpreting Plot 4 and Q7.**
# MAGIC Zero rows = clean LLM run. Any non-zero count means those ticks used
# MAGIC `RuleBasedAgent` decisions instead of LLM decisions - the cost
# MAGIC comparison is not fully clean for those ticks.

# COMMAND ----------

if df_agent_health.empty:
    print("Table 5 - Agent Health Summary (LLM run): no fallback or failure events. Clean run.")
else:
    table5 = (
        df_agent_health
        .groupby("event_type")
        .agg(count=("tick", "count"))
        .reset_index()
        .sort_values("event_type")
    )
    print("Table 5 - Agent Health Summary (LLM run) - NON-ZERO COUNTS INDICATE FALLBACKS")
    display(table5)
    print("\nFallback details (tick, event_type, payload):")
    display(df_agent_health[["tick", "item_id", "event_type", "payload"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 6 - Escalation summary *(LLM run only)*
# MAGIC
# MAGIC Source: `ops_escalation_queue`.
# MAGIC
# MAGIC Written by `LLMReorderAgent` via the `escalate_item` UC function.
# MAGIC The simulation engine never writes here.
# MAGIC An escalation means the LLM identified a situation it could not
# MAGIC handle autonomously - substantive information about agent behaviour.

# COMMAND ----------

if df_escalations.empty:
    print("Table 6 - Escalation Summary (LLM run): zero escalations raised.")
else:
    print(f"Table 6 - Escalation Summary (LLM run): {len(df_escalations)} escalation(s)")
    display(df_escalations[["tick", "item_id", "reason", "status", "raised_at"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Evaluation Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1 - Agent decisions per tick
# MAGIC
# MAGIC One row per `(sim_id, tick, item_id)`. Expect one per item per tick for
# MAGIC every tick in both runs. `agent_version` confirms which agent produced
# MAGIC each decision.

# COMMAND ----------

display(spark.sql(f"""
    SELECT sim_id, tick, item_id, decision, order_qty,
           stock_on_hand_at_decision, stock_in_transit_at_decision,
           agent_version, agent_reasoning
    FROM {CATALOG}.tables4hist.hist_reorder_decisions
    WHERE sim_id IN {ALL_SIM_IDS}
    ORDER BY sim_id, tick, item_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2 - Missing decision gap check
# MAGIC
# MAGIC **Expect: zero rows.** Any row is a tick where the runner's last-resort
# MAGIC hold-all fired and no row was written to `hist_reorder_decisions`.
# MAGIC Cross-reference with Q5 (`AGENT_ERROR` events).

# COMMAND ----------

display(spark.sql(f"""
    WITH expected_ticks AS (
        SELECT DISTINCT sim_id, tick
        FROM {CATALOG}.tables4hist.hist_reorder_decisions
        WHERE sim_id IN {ALL_SIM_IDS}
    ),
    full_range AS (
        SELECT sim_id, EXPLODE(SEQUENCE(0, {NUM_TICKS - 1})) AS tick
        FROM (SELECT DISTINCT sim_id FROM expected_ticks)
    )
    SELECT f.sim_id, f.tick AS missing_tick
    FROM full_range f
    LEFT JOIN expected_ticks e ON f.sim_id = e.sim_id AND f.tick = e.tick
    WHERE e.tick IS NULL
    ORDER BY f.sim_id, missing_tick
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3 - Monitoring loop / eval metrics per tick
# MAGIC
# MAGIC Written every tick by `LLMAgentWrapper`'s monitoring loop.
# MAGIC
# MAGIC **Note**: `metric_value` will be `0.0` for all metrics - computation
# MAGIC is not yet implemented (stub TODOs in `_write_eval_metrics`). This
# MAGIC query confirms the monitoring loop ran and wrote a row every tick;
# MAGIC the values are not yet meaningful.

# COMMAND ----------

display(spark.sql(f"""
    SELECT sim_id, tick, item_id, metric_name, metric_value, logged_at
    FROM {CATALOG}.tables4hist.hist_eval_metrics
    WHERE sim_id IN {ALL_SIM_IDS}
    ORDER BY sim_id, tick, metric_name, item_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4 - Pending orders and arrival status
# MAGIC
# MAGIC Orders should be `arrived` (or `partially_lost`/`fully_lost`) for any
# MAGIC `expected_arrival_tick` that has passed. Rows still `pending` beyond
# MAGIC their `expected_arrival_tick` at end of run indicate a supply processing bug.

# COMMAND ----------

display(spark.sql(f"""
    SELECT sim_id, order_tick, item_id, supplier_id, order_qty,
           expected_arrival_tick,
           (expected_arrival_tick - order_tick) AS actual_lead_time,
           status, disruptions_active_at_order
    FROM {CATALOG}.tables4ops.ops_pending_orders
    WHERE sim_id IN {ALL_SIM_IDS}
    ORDER BY sim_id, order_tick, item_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q5 - Fallback and failure events
# MAGIC
# MAGIC **Expect: zero rows for a clean run.**
# MAGIC Any rows here require cross-referencing with Table 5 and Plot 4:
# MAGIC those ticks used `RuleBasedAgent` decisions for the LLM run.

# COMMAND ----------

display(spark.sql(f"""
    SELECT sim_id, tick, item_id, event_type, payload, logged_at
    FROM {CATALOG}.tables4eventlog.event_log
    WHERE sim_id IN {ALL_SIM_IDS}
      AND event_type IN (
          'FALLBACK_STRUCTURAL', 'FALLBACK_LOGICAL',
          'EXECUTOR_ALL_STALE',  'AGENT_ERROR'
      )
    ORDER BY sim_id, tick
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q6 - Cost totals at end of run (per item)
# MAGIC
# MAGIC Source: `ops_cost_accumulator` at MAX(tick) per item.

# COMMAND ----------

display(spark.sql(f"""
    SELECT c.sim_id, c.item_id,
           c.cumulative_holding_cost,
           c.cumulative_stockout_cost,
           c.cumulative_order_cost,
           c.cumulative_transit_loss_cost,
           c.cumulative_total_cost,
           c.remaining_budget
    FROM {CATALOG}.tables4ops.ops_cost_accumulator c
    JOIN (
        SELECT sim_id, item_id, MAX(tick) AS max_tick
        FROM {CATALOG}.tables4ops.ops_cost_accumulator
        WHERE sim_id IN {ALL_SIM_IDS}
        GROUP BY sim_id, item_id
    ) latest ON c.sim_id = latest.sim_id
           AND c.item_id = latest.item_id
           AND c.tick    = latest.max_tick
    ORDER BY c.sim_id, c.item_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q7 - Cost comparison: both agents (headline)
# MAGIC
# MAGIC Aggregate cost totals across all items, one row per agent.
# MAGIC
# MAGIC Lower `total_stockout_cost` = better demand coverage.
# MAGIC Lower `total_holding_cost`  = less over-ordering.
# MAGIC
# MAGIC **Check Table 5 first.** If fallback events are present for the LLM
# MAGIC run, this comparison is not fully clean.

# COMMAND ----------

display(spark.sql(f"""
    SELECT c.sim_id,
           SUM(c.cumulative_stockout_cost)      AS total_stockout_cost,
           SUM(c.cumulative_holding_cost)        AS total_holding_cost,
           SUM(c.cumulative_order_cost)          AS total_order_cost,
           SUM(c.cumulative_transit_loss_cost)   AS total_transit_loss_cost,
           SUM(c.cumulative_total_cost)          AS total_cost,
           MIN(c.remaining_budget)               AS remaining_budget
    FROM {CATALOG}.tables4ops.ops_cost_accumulator c
    JOIN (
        SELECT sim_id, item_id, MAX(tick) AS max_tick
        FROM {CATALOG}.tables4ops.ops_cost_accumulator
        WHERE sim_id IN {ALL_SIM_IDS}
        GROUP BY sim_id, item_id
    ) latest ON c.sim_id = latest.sim_id
           AND c.item_id = latest.item_id
           AND c.tick    = latest.max_tick
    WHERE c.sim_id IN {ALL_SIM_IDS}
    GROUP BY c.sim_id
    ORDER BY c.sim_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q8 - Escalations *(LLM run only)*
# MAGIC
# MAGIC Written by `LLMReorderAgent` via `escalate_item` UC function.
# MAGIC The simulation engine never writes here. `OPEN` = awaiting human review.

# COMMAND ----------

display(spark.sql(f"""
    SELECT tick, item_id, reason, status, raised_at, context_json
    FROM {CATALOG}.tables4ops.ops_escalation_queue
    WHERE sim_id = '{SIM_ID_LLM}'
    ORDER BY tick, item_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q9 - Stockout events with payload
# MAGIC
# MAGIC Shows every tick where `unmet_demand > 0`.
# MAGIC Cross-reference with Q4 (pending orders) to check whether a reorder
# MAGIC placed in preceding ticks should have prevented the stockout.

# COMMAND ----------

display(spark.sql(f"""
    SELECT sim_id, tick, item_id,
           CAST(get_json_object(payload, '$.unmet_demand')  AS DOUBLE) AS unmet_demand,
           CAST(get_json_object(payload, '$.stockout_cost') AS DOUBLE) AS stockout_cost
    FROM {CATALOG}.tables4eventlog.event_log
    WHERE sim_id IN {ALL_SIM_IDS}
      AND event_type = 'STOCKOUT_OCCURRED'
    ORDER BY sim_id, tick, item_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Teardown (optional)
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