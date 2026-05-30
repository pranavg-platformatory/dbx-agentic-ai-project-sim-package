# Databricks notebook source
# MAGIC %md
# MAGIC # Continuous Simulation - Agent Runner
# MAGIC
# MAGIC Runs the simulation in continuous (infinite or cyclic) mode with a configurable agent. Agent selection is controlled by a single parameter at the top of the notebook - no other cell is aware of which agent was chosen.
# MAGIC
# MAGIC **Stop the simulation** by clicking **Interrupt** in the notebook toolbar. `ContinuousRunner` catches the interruption, writes `SIM_ENDED`, and prints a final summary before exiting cleanly.
# MAGIC
# MAGIC **Run simultaneously with**: [`integrationTest-3-continuousSim-liveDashboard.py`](./integrationTest-3-continuousSim-liveDashboard.py) - open in a separate tab, pointing at the same `SIM_ID`, to watch the simulation in real time. The dashboard notebook has no dependency on this one beyond the shared `SIM_ID` and the Delta tables it writes to.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Depends on**: Stages 1-7 complete (env tables exist, `ContinuousRunner` available). World is built in Section 4 below on fresh runs only.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Install dependencies

# COMMAND ----------

# MAGIC %pip install pydantic numpy matplotlib pandas
# MAGIC %pip install databricks-langchain langgraph
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters
# MAGIC
# MAGIC All configuration lives here. Cells below reference these variables directly.
# MAGIC
# MAGIC | Parameter | Description |
# MAGIC |---|---|
# MAGIC | `AGENT_TYPE` | `"rule_based"` or `"llm"` - the only switch between agent behaviours |
# MAGIC | `SIM_ID` | Unique ID for this run. Must match `SIM_ID` in the dashboard notebook. |
# MAGIC | `RUN_MODE` | `"infinite"` or `"cyclic"` |
# MAGIC | `TICK_UNIT` | `"hour"`, `"day"`, or `"week"` (cosmetic; passed to SimConfig) |
# MAGIC | `TICK_DURATION_SECONDS` | Wall-clock seconds per tick. `None` = full speed. |
# MAGIC | `SIM_SEED` | Random seed - passed to both `SimConfig` and `PatternSampler`. |
# MAGIC | `EXECUTOR_TRIGGER_N` | LLM only: how often (in ticks) the executor dispatches. Ignored for rule-based. |
# MAGIC | `LLM_AGENT_PACKAGE_PATH` | LLM only: path inserted into `sys.path`. Ignored for rule-based. |
# MAGIC | `LLM_AGENT_CONFIG_OVERRIDE` | LLM only: forwarded to `LLMAgentWrapperConfig.llm_agent_config_override`. |
# MAGIC
# MAGIC **Note on `TICK_DURATION_SECONDS`**: the dashboard notebook polls at a configurable interval. For a live feed, set `TICK_DURATION_SECONDS` to at least 2-3 seconds so the dashboard sees meaningful new data on each poll. `None` (full speed) will produce large bursts of ticks per dashboard poll rather than a continuous stream.

# COMMAND ----------

# ── Agent selection ──────────────────────────────────────────────────────────
# "rule_based" : RuleBasedAgent - deterministic, no external dependencies
# "llm"        : LLMAgentWrapper with LLMReorderAgent - live LLM calls
AGENT_TYPE = "llm"

# ── Simulation ───────────────────────────────────────────────────────────────
SIM_ID                = "sim_continuous_001"
RUN_MODE              = "infinite"   # "infinite" or "cyclic"
TICK_UNIT             = "hour"
TICK_DURATION_SECONDS = 3.0          # 3 real seconds per simulated hour; None = full speed
PRINT_EVERY_N_TICKS   = 1
SIM_SEED              = 42

# ── LLM agent (ignored when AGENT_TYPE = "rule_based") ──────────────────────
EXECUTOR_TRIGGER_N        = 4        # executor dispatches every 4 ticks
LLM_AGENT_PACKAGE_PATH    = "/Workspace/Shared/reorder-llm-agent"
LLM_AGENT_CONFIG_OVERRIDE = {
    # "warehouse_id": "your_warehouse_id",
    # "llm_endpoint": "databricks-meta-llama-3-3-70b-instruct",
}

# ── Catalog ──────────────────────────────────────────────────────────────────
CATALOG = "hackathon_of_the_century"

print(f"Parameters set.")
print(f"  AGENT_TYPE            : {AGENT_TYPE}")
print(f"  SIM_ID                : {SIM_ID}")
print(f"  RUN_MODE              : {RUN_MODE}")
print(f"  TICK_UNIT             : {TICK_UNIT}")
print(f"  TICK_DURATION_SECONDS : {TICK_DURATION_SECONDS}")
print(f"  SIM_SEED              : {SIM_SEED}")
if AGENT_TYPE == "llm":
    print(f"  EXECUTOR_TRIGGER_N    : {EXECUTOR_TRIGGER_N}")

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
# MAGIC **Runs on fresh start** (`_has_prior_state = False`). Writes the full world definition to the `tables4env` schema before `load_world` can succeed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **World spec**:
# MAGIC - 2 items (Widget A, Gadget B), 2 suppliers, 1 consumer
# MAGIC - Widget A: 5-tick cyclic custom demand with noise; stochastic demand spike from tick 10 onward (40% chance per tick) - tests agent response to unpredictable surge
# MAGIC - Gadget B: Poisson demand (mu=8); stochastic transit delay from tick 20
# MAGIC   onward (25% chance per tick) - tests agent response to supply disruption
# MAGIC - Probabilistic lead times (non-zero variability) to create realistic supply uncertainty
# MAGIC - No budget limit: continuous runs are not time-bounded so a hard budget cap would stop the simulation unpredictably

# COMMAND ----------

if _has_prior_state:
    print(f"Warm-start: skipping write_world for sim_id='{SIM_ID}'.")

else:
    NOW = datetime.now(timezone.utc)

    world_def = SimWorld(
        config = SimConfig(
            sim_id                     = SIM_ID,
            random_seed                = SIM_SEED,
            num_ticks                  = None,             # None = run forever
            run_mode                   = RunMode(RUN_MODE),
            tick_unit                  = TickUnit(TICK_UNIT),
            budget_limit               = None,             # unlimited; hard cap not meaningful for open-ended runs
            budget_warning_threshold   = 0.10,
            agent_history_window_ticks = 10,
            start_timestamp            = NOW,
            created_at                 = NOW,
        ),
        items = {
            "item_A": ItemType(
                item_id                         = "item_A",
                item_name                       = "Widget A",
                unit_value                      = 5.0,
                initial_stock                   = 80,
                reorder_point                   = 25,
                min_order_qty                   = 20,
                max_order_qty                   = 150,
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
                initial_stock                   = 40,
                reorder_point                   = 15,
                min_order_qty                   = 10,
                max_order_qty                   = 80,
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
                lead_time_variability = 0.5,   # non-zero: realistic supply uncertainty
            ),
            "sup_002": Supplier(
                supplier_id           = "sup_002",
                supplier_name         = "Globex Ltd",
                base_lead_time_ticks  = 4,
                lead_time_variability = 1.0,
            ),
        },
        consumers = {
            "con_001": Consumer(consumer_id="con_001", consumer_name="Retail Division"),
        },
        supplier_item_map = {"item_A": "sup_001", "item_B": "sup_002"},
        consumer_item_map = {"item_A": "con_001", "item_B": "con_001"},
        demand_patterns = {
            "item_A": Pattern(
                pattern_id      = f"{SIM_ID}__item_A__demand",
                sim_id          = SIM_ID,
                item_id         = "item_A",
                role            = PatternRole.DEMAND,
                pattern_type    = PatternType.CUSTOM,
                # 5-tick cycle repeating indefinitely - gives a recognisable wave structure in the dashboard plots
                custom_schedule = [10.0, 15.0, 20.0, 12.0, 8.0],
                noise_std       = 2.0,
            ),
            "item_B": Pattern(
                pattern_id   = f"{SIM_ID}__item_B__demand",
                sim_id       = SIM_ID,
                item_id      = "item_B",
                role         = PatternRole.DEMAND,
                pattern_type = PatternType.STATISTICAL,
                distribution = Distribution.POISSON,
                dist_params  = {"mu": 8},
                noise_std    = 0.0,
            ),
        },
        supply_patterns = {},
        disruptions = [
            # Stochastic demand spike on item_A from tick 10 onward.
            # 40% activation probability per tick - tests whether the agent learns to anticipate surge rather than react to it.
            DisruptionSchedule(
                disruption_id       = f"{SIM_ID}__dis_spike_A",
                sim_id              = SIM_ID,
                item_id             = "item_A",
                disruption_type     = DisruptionType.DEMAND_SPIKE,
                start_tick          = 10,
                end_tick            = 999_999,
                magnitude           = 2.5,
                is_stochastic       = True,
                trigger_probability = 0.40,
            ),
            # Stochastic transit delay on item_B from tick 20 onward.
            # 25% activation probability per tick - creates supply uncertainty that a rule-based agent (which ignores lead time disruptions at decision time) will handle worse than the LLM agent.
            DisruptionSchedule(
                disruption_id       = f"{SIM_ID}__dis_delay_B",
                sim_id              = SIM_ID,
                item_id             = "item_B",
                disruption_type     = DisruptionType.TRANSIT_DELAY,
                start_tick          = 20,
                end_tick            = 999_999,
                magnitude           = 1.5,
                is_stochastic       = True,
                trigger_probability = 0.25,
            ),
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
# MAGIC Open [`integrationTest-3-continuousSim-liveDashboard.py`](./integrationTest-3-continuousSim-liveDashboard.py) in a separate tab with the same `SIM_ID` to watch the simulation live while this cell is running.

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