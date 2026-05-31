# Databricks notebook source
# MAGIC %md
# MAGIC # Continuous Simulation - Agent Runner
# MAGIC
# MAGIC Runs the simulation in continuous (infinite or cyclic) mode with a configurable agent. Agent selection is controlled by a single parameter at the top of the notebook - no other cell is aware of which agent was chosen.
# MAGIC
# MAGIC **Stop the simulation** by clicking **Interrupt** in the notebook toolbar. `ContinuousRunner` catches the interruption, writes `SIM_ENDED`, and prints a final summary before exiting cleanly.
# MAGIC
# MAGIC **Run simultaneously with**: [`integrationTest-4-continuousSim-liveDashboard.py`](./integrationTest-4-continuousSim-liveDashboard.py) - open in a separate tab, pointing at the same `SIM_ID`, to watch the simulation in real time. The dashboard notebook has no dependency on this one beyond the shared `SIM_ID` and the Delta tables it writes to.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Depends on**: Stages 1-7 complete (env tables exist, `ContinuousRunner` available). World is built in Section 4 below on fresh runs only.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Install dependencies

# COMMAND ----------

# MAGIC %pip install pydantic
# MAGIC %pip install databricks-langchain langgraph
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters
# MAGIC
# MAGIC All configuration is loaded from `sim_config.yaml`. No values are hardcoded
# MAGIC in this notebook. Edit the YAML to change any simulation or world parameter.
# MAGIC
# MAGIC **Shared keys** (`sim_id`, `catalog`): read by both the agent runner and the
# MAGIC live dashboard. Changing them here changes them for both notebooks.
# MAGIC
# MAGIC **Runner-only keys** (`agent`, `simulation`, `llm_agent`, `world`): read by
# MAGIC this notebook only. The dashboard's display parameters (`POLL_INTERVAL_SECONDS`,
# MAGIC `MAX_TICKS_TO_SHOW`) remain local to the dashboard notebook — they control
# MAGIC polling behaviour only and have no bearing on the simulation.
# MAGIC
# MAGIC **Note on `tick_duration_seconds`**: the dashboard polls at a configurable
# MAGIC interval. For a live feed, set `tick_duration_seconds` to at least 2–3 seconds
# MAGIC so the dashboard sees meaningful new data on each poll. `null` (full speed)
# MAGIC will produce large bursts of ticks per dashboard poll rather than a continuous stream.
# MAGIC  

# COMMAND ----------

import yaml
 
_CONFIG_PATH = "/Workspace/Repos/mistermilvusmigrans@gmail.com/dbx-agentic-ai-project-sim-package/sim_config_files/sim_config--2026-05-31--llm_agent--2.yaml"
 
with open(_CONFIG_PATH, "r") as _f:
    _cfg = yaml.safe_load(_f)
 
# ── Shared ────────────────────────────────────────────────────────────────────
SIM_ID  = _cfg["sim_id"]
CATALOG = _cfg["catalog"]
 
# ── Agent ─────────────────────────────────────────────────────────────────────
AGENT_TYPE = _cfg["agent"]["type"]
 
# ── Simulation ────────────────────────────────────────────────────────────────
RUN_MODE              = _cfg["simulation"]["run_mode"]
TICK_UNIT             = _cfg["simulation"]["tick_unit"]
TICK_DURATION_SECONDS = _cfg["simulation"]["tick_duration_seconds"]   # None if null in YAML
PRINT_EVERY_N_TICKS   = _cfg["simulation"]["print_every_n_ticks"]
SIM_SEED              = _cfg["simulation"]["seed"]

# ── LLM agent (ignored when AGENT_TYPE = "rule_based") ───────────────────────
EXECUTOR_TRIGGER_N        = _cfg["llm_agent"]["executor_trigger_n"]
LLM_AGENT_PACKAGE_PATH    = _cfg["llm_agent"]["package_path"]
LLM_AGENT_CONFIG_OVERRIDE = _cfg["llm_agent"]["config_override"] or {}
 
print(f"Config loaded from: {_CONFIG_PATH}")
print(f"  SIM_ID                : {SIM_ID}")
print(f"  CATALOG               : {CATALOG}")
print(f"  AGENT_TYPE            : {AGENT_TYPE}")
print(f"  RUN_MODE              : {RUN_MODE}")
print(f"  TICK_UNIT             : {TICK_UNIT}")
print(f"  TICK_DURATION_SECONDS : {TICK_DURATION_SECONDS}")
print(f"  SIM_SEED              : {SIM_SEED}")
if AGENT_TYPE == "llm":
    print(f"  EXECUTOR_TRIGGER_N    : {EXECUTOR_TRIGGER_N}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## [OPTIONAL] Clear previous table values
# MAGIC Uncomment the code and run it ONLY IF NECESSARY. Normally, it should remain commented.

# COMMAND ----------

# _APPEND_ONLY_TABLES = [
#     f"{CATALOG}.tables4ops.ops_warehouse_state",
#     f"{CATALOG}.tables4ops.ops_active_disruptions",
#     f"{CATALOG}.tables4ops.ops_cost_accumulator",
#     f"{CATALOG}.tables4hist.hist_demand_actuals",
#     f"{CATALOG}.tables4hist.hist_supply_arrivals",
#     f"{CATALOG}.tables4hist.hist_reorder_decisions",
#     f"{CATALOG}.tables4hist.hist_cost_by_tick",
#     f"{CATALOG}.tables4hist.hist_eval_metrics",
#     f"{CATALOG}.tables4eventlog.event_log",
# ]
# _MUTABLE_TABLES = [
#     f"{CATALOG}.tables4ops.ops_pending_orders",
#     f"{CATALOG}.tables4ops.ops_escalation_queue",
# ]
# _ENV_TABLES_WITH_SIM_ID = [
#     f"{CATALOG}.tables4env.env_sim_config",
#     f"{CATALOG}.tables4env.env_supplier_item_map",
#     f"{CATALOG}.tables4env.env_consumer_item_map",
#     f"{CATALOG}.tables4env.env_patterns",
#     f"{CATALOG}.tables4env.env_disruption_schedule",
# ]

# print(f"Clearing data for sim_id='{SIM_ID}'...")

# for table in _APPEND_ONLY_TABLES:
#     spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.appendOnly' = 'false')")
#     spark.sql(f"DELETE FROM {table} WHERE sim_id = '{SIM_ID}'")
#     spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('delta.appendOnly' = 'true')")

# for table in _MUTABLE_TABLES:
#     try:
#         spark.sql(f"DELETE FROM {table} WHERE sim_id = '{SIM_ID}'")
#     except Exception:
#         pass  # table may not exist yet if escalation queue was never written

# for table in _ENV_TABLES_WITH_SIM_ID:
#     spark.sql(f"DELETE FROM {table} WHERE sim_id = '{SIM_ID}'")

# print(f"[DONE] Prior data cleared for {SIM_ID}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports and path setup

# COMMAND ----------

import sys

PACKAGE_ROOT = "/Workspace/Repos/mistermilvusmigrans@gmail.com/dbx-agentic-ai-project-sim-package"
if PACKAGE_ROOT not in sys.path:
    sys.path.insert(0, PACKAGE_ROOT)

# LLM agent package - only needed for AGENT_TYPE="llm".
# Inserted unconditionally here so the agent selection cell can import freely without a conditional sys.path block of its own.
# NOTE: The import itself is deferred to the agent selection cell.
if AGENT_TYPE == "llm" and LLM_AGENT_PACKAGE_PATH not in sys.path:
    sys.path.insert(0, LLM_AGENT_PACKAGE_PATH)

from datetime import datetime, timezone

from warehouse_sim.config.models import (
    Consumer, DisruptionSchedule, DisruptionType,
    ItemType, Pattern, PatternRole, PatternType, Distribution,
    RunMode, SimConfig, SimWorld, Supplier, TickUnit,
)
from warehouse_sim.world.setup import write_world
from warehouse_sim.config.loader import load_world
from warehouse_sim.world.patterns import PatternSampler
from warehouse_sim.event_log.event_log import EventLogger
from warehouse_sim.engine.continuous import ContinuousRunner, ProgressConfig

print("Imports resolved.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Prior state detection
# MAGIC
# MAGIC Checks whether data already exists for this `SIM_ID` in `ops_warehouse_state`. The result drives Section 4: `write_world` is called only on a fresh run.
# MAGIC
# MAGIC **No data wipe is performed here or anywhere in this notebook.** State management (recovery, resume tick, cost continuity) is handled entirely by `ContinuousRunner` via its warm-start logic in `_initialise()`. Wiping data on restart would destroy accumulated history and defeat the purpose of warm-start. If a clean slate is ever needed, that is a deliberate out-of-band operation, not something the notebook does automatically.

# COMMAND ----------

_max_tick_row = spark.sql(f'''
    SELECT MAX(tick) AS max_tick
    FROM {CATALOG}.tables4ops.ops_warehouse_state
    WHERE sim_id = '{SIM_ID}'
''').collect()[0]

_has_prior_state = _max_tick_row["max_tick"] is not None

if _has_prior_state:
    print(f"Prior state detected for sim_id='{SIM_ID}' (max_tick={_max_tick_row['max_tick']}).")
    print(f"  write_world will be skipped. Runner will resume from tick {_max_tick_row['max_tick'] + 1}.")
else:
    print(f"No prior state found for sim_id='{SIM_ID}'. Fresh run.")
    print(f"  write_world will run to populate env tables.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build and write `SimWorld`
# MAGIC
# MAGIC **Skipped on warm-start** (`_has_prior_state = True`). The env tables already
# MAGIC hold the world definition from the original run. `load_world` in Section 5 reads
# MAGIC from those tables directly.
# MAGIC
# MAGIC **Runs on fresh start** (`_has_prior_state = False`). Constructs `SimWorld`
# MAGIC from the world definition in `sim_config.yaml` and writes it to the
# MAGIC `tables4env` schema before `load_world` can succeed.
# MAGIC
# MAGIC All world parameters (items, suppliers, demand patterns, disruptions) are
# MAGIC sourced from `_cfg["world"]`. No values are hardcoded in this cell.
# MAGIC  

# COMMAND ----------

 
if _has_prior_state:
    print(f"Warm-start: skipping write_world for sim_id='{SIM_ID}'.")
 
else:
    NOW  = datetime.now(timezone.utc)
    _w   = _cfg["world"]
    _sc  = _w["sim_config"]
 
    world_def = SimWorld(
        config = SimConfig(
            sim_id                     = SIM_ID,
            random_seed                = SIM_SEED,
            num_ticks                  = None,
            run_mode                   = RunMode(RUN_MODE),
            tick_unit                  = TickUnit(TICK_UNIT),
            budget_limit               = _sc["budget_limit"],
            budget_warning_threshold   = _sc["budget_warning_threshold"],
            agent_history_window_ticks = _sc["agent_history_window_ticks"],
            start_timestamp            = NOW,
            created_at                 = NOW,
        ),
 
        items = {
            item_id: ItemType(
                item_id                         = item_id,
                item_name                       = v["name"],
                unit_value                      = v["unit_value"],
                initial_stock                   = v["initial_stock"],
                reorder_point                   = v["reorder_point"],
                min_order_qty                   = v["min_order_qty"],
                max_order_qty                   = v["max_order_qty"],
                holding_cost_per_unit_per_tick  = v["holding_cost_per_unit_per_tick"],
                stockout_cost_per_unit_per_tick = v["stockout_cost_per_unit_per_tick"],
                order_fixed_cost                = v["order_fixed_cost"],
                order_variable_cost_per_unit    = v["order_variable_cost_per_unit"],
                transit_loss_cost_per_unit      = v["transit_loss_cost_per_unit"],
            )
            for item_id, v in _w["items"].items()
        },
 
        suppliers = {
            sup_id: Supplier(
                supplier_id           = sup_id,
                supplier_name         = v["name"],
                base_lead_time_ticks  = v["base_lead_time_ticks"],
                lead_time_variability = v["lead_time_variability"],
            )
            for sup_id, v in _w["suppliers"].items()
        },
 
        consumers = {
            con_id: Consumer(
                consumer_id   = con_id,
                consumer_name = v["name"],
            )
            for con_id, v in _w["consumers"].items()
        },
 
        supplier_item_map = _w["supplier_item_map"],
        consumer_item_map = _w["consumer_item_map"],
 
        demand_patterns = {
            item_id: (
                Pattern(
                    pattern_id      = f"{SIM_ID}__{item_id}__demand",
                    sim_id          = SIM_ID,
                    item_id         = item_id,
                    role            = PatternRole.DEMAND,
                    pattern_type    = PatternType.CUSTOM,
                    custom_schedule = v["custom_schedule"],
                    noise_std       = v["noise_std"],
                )
                if v["pattern_type"] == "custom" else
                Pattern(
                    pattern_id   = f"{SIM_ID}__{item_id}__demand",
                    sim_id       = SIM_ID,
                    item_id      = item_id,
                    role         = PatternRole.DEMAND,
                    pattern_type = PatternType.STATISTICAL,
                    distribution = Distribution[v["distribution"].upper()],
                    dist_params  = v["dist_params"],
                    noise_std    = v["noise_std"],
                )
            )
            for item_id, v in _w["demand_patterns"].items()
        },
 
        supply_patterns = {},
 
        disruptions = [
            DisruptionSchedule(
                disruption_id       = f"{SIM_ID}__{d['disruption_id']}",
                sim_id              = SIM_ID,
                item_id             = d["item_id"],
                disruption_type     = DisruptionType[d["disruption_type"].upper()],
                start_tick          = d["start_tick"],
                end_tick            = d["end_tick"],
                magnitude           = d["magnitude"],
                is_stochastic       = d["is_stochastic"],
                trigger_probability = d["trigger_probability"],
            )
            for d in _w["disruptions"]
        ],
    )
 
    write_world(spark, world_def)
    print(f"[DONE] SimWorld written for sim_id='{SIM_ID}'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load world, logger, and sampler

# COMMAND ----------

world   = load_world(spark, sim_id=SIM_ID)
sampler = PatternSampler(seed=world.config.random_seed)
logger  = EventLogger(spark, sim_id=SIM_ID)

print(f"World loaded: {len(world.items)} items, {len(world.suppliers)} suppliers.")
print(f"Sampler seeded: {world.config.random_seed}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Agent selection
# MAGIC
# MAGIC This is the only cell that branches on `AGENT_TYPE`. Both branches produce a single `agent` variable that implements `BaseAgent`. Everything downstream (Section 7) is identical regardless of which branch was taken.
# MAGIC
# MAGIC **`"rule_based"`**: `RuleBasedAgent` - instantiated directly, no config needed.
# MAGIC
# MAGIC **`"llm"`**: `LLMAgentWrapper` with `LLMReorderAgent` as the internal executor. Requires the LLM agent package on `sys.path` (set in Section 2). The wrapper is the agent the runner sees; `LLMReorderAgent` is an internal detail of the executor thread. See `warehouse_sim/agent/llm_agent_wrapper.py` for the full design, and `devlog.md → Full LLM Agent Integration` for integration decisions.

# COMMAND ----------

if AGENT_TYPE == "rule_based":
    from warehouse_sim.agent.rule_based_agent import RuleBasedAgent
    agent = RuleBasedAgent()
    print(f"Agent: RuleBasedAgent (version={agent.agent_version()})")

elif AGENT_TYPE == "llm":
    from warehouse_sim.agent.llm_agent_wrapper import LLMAgentWrapper
    from warehouse_sim.config.llm_agent_wrapper_config import LLMAgentWrapperConfig

    llm_config = LLMAgentWrapperConfig(
        executor_trigger_every_n_ticks   = EXECUTOR_TRIGGER_N,
        context_obsolescence_threshold_k = None,   # resolved to min lead time at init
        queue_size                       = 1,
        stub_mode                        = None,   # live LLM call via LLMReorderAgent
        suppress_write_tools             = True,   # runner owns hist_reorder_decisions writes
        llm_agent_config_override        = LLM_AGENT_CONFIG_OVERRIDE or None,
    )

    # NOTE: LLMAgentWrapper needs world and logger at construction time.
    # This is why Section 5 (load world + logger) must run before this cell.
    # See [DEP-4] in warehouse_sim/agent/llm_agent_wrapper.py for the SparkSession injection rationale.
    agent = LLMAgentWrapper(
        spark  = spark,
        world  = world,
        config = llm_config,
        logger = logger,
    )
    print(f"Agent: LLMAgentWrapper (version={agent.agent_version()})")
    print(f"  executor_trigger_every_n_ticks : {EXECUTOR_TRIGGER_N}")
    print(f"  resolved_k (obsolescence)      : {agent._resolved_k} ticks")

else:
    raise ValueError(
        f"Unknown AGENT_TYPE={AGENT_TYPE!r}. "
        "Set AGENT_TYPE to 'rule_based' or 'llm' in Section 1."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Run
# MAGIC
# MAGIC **This cell runs indefinitely until you click Interrupt.**
# MAGIC
# MAGIC `ContinuousRunner` inherits all simulation logic from `SimRunner` and adds:
# MAGIC - Wall-clock pacing (`TICK_DURATION_SECONDS` sleep between ticks)
# MAGIC - Live console progress line every `PRINT_EVERY_N_TICKS` ticks
# MAGIC - Graceful `KeyboardInterrupt` handling: `SIM_ENDED` is always written
# MAGIC   before the cell exits, so the event log is complete regardless of when
# MAGIC   you stop the simulation
# MAGIC
# MAGIC On warm-start, the runner resumes automatically from the last completed tick. No action is required here — `runner.run()` calls `_initialise()` internally, which detects prior state and branches accordingly.
# MAGIC
# MAGIC Open [`integrationTest-4-continuousSim-liveDashboard.py`](./integrationTest-4-continuousSim-liveDashboard.py) in a separate tab with the same `SIM_ID` to watch the simulation live while this cell is running.

# COMMAND ----------

runner = ContinuousRunner(
    spark    = spark,
    world    = world,
    agent    = agent,
    logger   = logger,
    sampler  = sampler,
    progress = ProgressConfig(
        tick_real_duration_seconds = TICK_DURATION_SECONDS,
        print_every_n_ticks        = PRINT_EVERY_N_TICKS,
        show_stockouts             = True,
        show_orders                = True,
        show_costs                 = True,
        show_disruptions           = True,
    ),
)

# Press Interrupt to stop cleanly. SIM_ENDED will be written before the cell exits.
runner.run()