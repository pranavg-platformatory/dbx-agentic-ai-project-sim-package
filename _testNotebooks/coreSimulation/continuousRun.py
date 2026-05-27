# Databricks notebook source
# MAGIC %md
# MAGIC # Continuous Mode Runner
# MAGIC ### Live simulation with real-time pacing and progress output
# MAGIC
# MAGIC This notebook runs the simulation in continuous (infinite or cyclic) mode
# MAGIC with configurable wall-clock pacing between ticks and live progress printed
# MAGIC to the cell output.
# MAGIC
# MAGIC **Stop the simulation** by clicking **Interrupt** in the notebook toolbar.
# MAGIC The runner will catch the interruption, write `SIM_ENDED`, and print a
# MAGIC final summary before exiting cleanly.
# MAGIC
# MAGIC **Depends on**: Stages 1-4 (world must already exist in env tables,
# MAGIC or build it fresh in section 4 below).

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
# MAGIC ## 2. Configuration
# MAGIC
# MAGIC Adjust these before running:
# MAGIC
# MAGIC | Parameter | Description |
# MAGIC |---|---|
# MAGIC | `SIM_ID` | Unique ID for this run |
# MAGIC | `RUN_MODE` | `"infinite"` or `"cyclic"` |
# MAGIC | `TICK_UNIT` | `"hour"`, `"day"`, or `"week"` |
# MAGIC | `TICK_DURATION_SECONDS` | Wall-clock seconds per tick (`None` = full speed) |
# MAGIC | `PRINT_EVERY_N_TICKS` | Progress line frequency |

# COMMAND ----------

CATALOG                = "hackathon_of_the_century"
SIM_ID                 = "sim_continuous_001"
RUN_MODE               = "infinite"     # "infinite" or "cyclic"
TICK_UNIT              = "day"
TICK_DURATION_SECONDS  = 2.0            # 2 seconds per simulated day; None = full speed
PRINT_EVERY_N_TICKS    = 1              # print every tick

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Clean up any prior data for this SIM_ID

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
    except Exception:
        pass

for table, col, vals in [
    (f"{CATALOG}.tables4env.env_item_types", "item_id",     "('item_A','item_B')"),
    (f"{CATALOG}.tables4env.env_suppliers",  "supplier_id", "('sup_001','sup_002')"),
    (f"{CATALOG}.tables4env.env_consumers",  "consumer_id", "('con_001')"),
]:
    spark.sql(f"DELETE FROM {table} WHERE {col} IN {vals}")

print("✓ Prior data cleared")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Build and write SimWorld
# MAGIC
# MAGIC Cyclic demand pattern: 5-tick schedule that repeats indefinitely.
# MAGIC Stochastic disruption on `item_A` that fires ~40% of ticks from tick 10 onward.

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
        num_ticks                  = None,           # None = run forever
        run_mode                   = RunMode(RUN_MODE),
        tick_unit                  = TickUnit(TICK_UNIT),
        budget_limit               = None,           # unlimited for continuous run
        budget_warning_threshold   = 0.10,
        agent_history_window_ticks = 7,
        start_timestamp            = NOW,
        created_at                 = NOW,
    ),
    items = {
        "item_A": ItemType(
            item_id="item_A", item_name="Widget A", unit_value=5.0,
            initial_stock=80, reorder_point=25, min_order_qty=20, max_order_qty=150,
            holding_cost_per_unit_per_tick=0.05, stockout_cost_per_unit_per_tick=2.0,
            order_fixed_cost=50.0, order_variable_cost_per_unit=4.5,
            transit_loss_cost_per_unit=6.0,
        ),
        "item_B": ItemType(
            item_id="item_B", item_name="Gadget B", unit_value=12.0,
            initial_stock=40, reorder_point=15, min_order_qty=10, max_order_qty=80,
            holding_cost_per_unit_per_tick=0.10, stockout_cost_per_unit_per_tick=5.0,
            order_fixed_cost=30.0, order_variable_cost_per_unit=10.0,
            transit_loss_cost_per_unit=15.0,
        ),
    },
    suppliers = {
        "sup_001": Supplier(supplier_id="sup_001", supplier_name="Acme Corp",
                            base_lead_time_ticks=3, lead_time_variability=0.5),
        "sup_002": Supplier(supplier_id="sup_002", supplier_name="Globex Ltd",
                            base_lead_time_ticks=4, lead_time_variability=1.0),
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
            # 5-tick cycle - repeats indefinitely via cycling
            custom_schedule=[10.0, 15.0, 20.0, 12.0, 8.0],
            seasonal_multiplier_schedule=[1.0, 1.1, 1.2, 1.0, 0.9, 1.0, 0.8],
            noise_std=2.0,
        ),
        "item_B": Pattern(
            pattern_id="pat_B", sim_id=SIM_ID, item_id="item_B",
            role=PatternRole.DEMAND, pattern_type=PatternType.STATISTICAL,
            distribution=Distribution.POISSON, dist_params={"mu": 8},
            noise_std=0.0,
        ),
    },
    supply_patterns = {},
    disruptions = [
        DisruptionSchedule(
            disruption_id="dis_spike_A", sim_id=SIM_ID, item_id="item_A",
            disruption_type=DisruptionType.DEMAND_SPIKE,
            start_tick=10, end_tick=999_999,   # effectively permanent window
            magnitude=2.5, is_stochastic=True,
            trigger_probability=0.40,
        ),
        DisruptionSchedule(
            disruption_id="dis_delay_B", sim_id=SIM_ID, item_id="item_B",
            disruption_type=DisruptionType.TRANSIT_DELAY,
            start_tick=20, end_tick=999_999,
            magnitude=1.5, is_stochastic=True,
            trigger_probability=0.25,
        ),
    ],
)

write_world(spark, world)
print("✓ SimWorld written for continuous run")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Define the agent

# COMMAND ----------

from warehouse_sim.agent.base import AgentContext, BaseAgent, ReorderDecision

class ContinuousReorderAgent(BaseAgent):
    '''
    Reorder when stock_on_hand + stock_in_transit falls below
    2 × reorder_point. Orders up to max_order_qty.
    This is a slightly more defensive policy than the Stage 4 agent,
    appropriate for infinite runs where stockouts compound over time.
    '''
    def decide(self, context: AgentContext) -> list[ReorderDecision]:
        decisions = []
        for item_id in context.items():
            state    = context.item_states[item_id]
            coverage = state.stock_on_hand + state.stock_in_transit
            threshold = 2 * state.reorder_point

            if coverage < threshold:
                # Order enough to reach max_order_qty above the threshold
                qty = min(state.max_order_qty, threshold - coverage + state.min_order_qty)
                qty = max(qty, state.min_order_qty)
                decisions.append(ReorderDecision(
                    item_id   = item_id,
                    order_qty = qty,
                    reasoning = (
                        f"coverage={coverage} < threshold={threshold}. "
                        f"Ordering {qty} units."
                    ),
                ))
            else:
                decisions.append(ReorderDecision(
                    item_id   = item_id,
                    order_qty = 0,
                    reasoning = f"coverage={coverage} sufficient.",
                ))
        return decisions

    def agent_version(self) -> str:
        return "continuous_reorder_v1"

print("✓ ContinuousReorderAgent defined")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Run the simulation
# MAGIC
# MAGIC **The cell below runs indefinitely until you click Interrupt.**
# MAGIC
# MAGIC Progress is printed every `PRINT_EVERY_N_TICKS` ticks:
# MAGIC ```
# MAGIC [tick    0/∞]  0s elapsed  ETA -  │  item_A:   80  item_B:   40  cost=£0  orders= 0 pending
# MAGIC [tick    1/∞]  2s elapsed  ETA -  │  item_A:   70  item_B:   32  cost=£8  orders= 0 pending  ⚠ stockout: item_A(2)
# MAGIC ```
# MAGIC
# MAGIC Columns:
# MAGIC - `elapsed` - wall-clock time since run started
# MAGIC - `ETA` - estimated time to completion (∞ mode always shows `-`)
# MAGIC - `item_X: N` - stock on hand per item
# MAGIC - `cost` - cumulative total cost across all items
# MAGIC - `orders` - units currently in transit
# MAGIC - `⚠ stockout` - items with unmet demand this tick (and units short)
# MAGIC - `🔴 disruptions` - number of deterministic disruptions active this tick

# COMMAND ----------

from warehouse_sim.config.loader import load_world
from warehouse_sim.world.patterns import PatternSampler
from warehouse_sim.event_log.event_log import EventLogger
from warehouse_sim.engine.continuous import ContinuousRunner, ProgressConfig

world_rt = load_world(spark, sim_id=SIM_ID)
sampler  = PatternSampler(seed=world_rt.config.random_seed)
logger   = EventLogger(spark, sim_id=SIM_ID)

runner = ContinuousRunner(
    spark    = spark,
    world    = world_rt,
    agent    = ContinuousReorderAgent(),
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

# Press Interrupt to stop. SIM_ENDED will be written before the cell exits.
runner.run()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Inspect what was written
# MAGIC
# MAGIC Run these cells after interrupting to see what accumulated.

# COMMAND ----------

# MAGIC %md ### How many ticks ran?

# COMMAND ----------

display(spark.sql(f'''
    SELECT MAX(tick) AS ticks_completed,
           COUNT(DISTINCT item_id) AS items
    FROM {CATALOG}.tables4ops.ops_warehouse_state
    WHERE sim_id = '{SIM_ID}'
'''))

# COMMAND ----------

# MAGIC %md ### Stock levels - last 10 ticks

# COMMAND ----------

display(spark.sql(f'''
    SELECT tick, item_id, stock_on_hand, stock_in_transit
    FROM {CATALOG}.tables4ops.ops_warehouse_state
    WHERE sim_id = '{SIM_ID}'
      AND tick >= (
          SELECT MAX(tick) - 9
          FROM {CATALOG}.tables4ops.ops_warehouse_state
          WHERE sim_id = '{SIM_ID}'
      )
    ORDER BY tick, item_id
'''))

# COMMAND ----------

# MAGIC %md ### Disruption activations - how often did they fire?

# COMMAND ----------

display(spark.sql(f'''
    SELECT disruption_id, disruption_type, item_id,
           COUNT(*) AS ticks_in_window,
           SUM(CAST(is_active_this_tick AS INT)) AS ticks_active,
           ROUND(SUM(CAST(is_active_this_tick AS INT)) / COUNT(*), 3) AS activation_rate
    FROM {CATALOG}.tables4ops.ops_active_disruptions
    WHERE sim_id = '{SIM_ID}'
    GROUP BY disruption_id, disruption_type, item_id
    ORDER BY disruption_id
'''))

# COMMAND ----------

# MAGIC %md ### Cost accumulation - final totals per item

# COMMAND ----------

display(spark.sql(f'''
    SELECT item_id,
           MAX_BY(cumulative_holding_cost,      tick) AS holding,
           MAX_BY(cumulative_stockout_cost,     tick) AS stockout,
           MAX_BY(cumulative_order_cost,        tick) AS order_cost,
           MAX_BY(cumulative_transit_loss_cost, tick) AS transit_loss,
           MAX_BY(cumulative_total_cost,        tick) AS total
    FROM {CATALOG}.tables4ops.ops_cost_accumulator
    WHERE sim_id = '{SIM_ID}'
    GROUP BY item_id
    ORDER BY item_id
'''))

# COMMAND ----------

# MAGIC %md ### Event log - type counts

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
# MAGIC ## 8. Dashboard (after run)
# MAGIC
# MAGIC Render the full visualisation dashboard for whatever ticks completed.

# COMMAND ----------

import matplotlib
matplotlib.rcParams["figure.dpi"] = 110

from warehouse_sim.viz.dashboard import SimDashboard

dash = SimDashboard(spark, sim_id=SIM_ID)
dash.print_summary()

# COMMAND ----------

fig = dash.plot_stock()
display(fig)

# COMMAND ----------

fig = dash.plot_demand()
display(fig)

# COMMAND ----------

fig = dash.plot_costs()
display(fig)

# COMMAND ----------

fig = dash.plot_decisions()
display(fig)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Teardown (optional)

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