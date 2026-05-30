# Databricks notebook source
# MAGIC %md
# MAGIC # Continuous Simulation - Live Dashboard
# MAGIC
# MAGIC Polls the Delta tables written by the agent runner notebook and re-renders
# MAGIC a composite plot in real time. Has no dependency on the simulation engine -
# MAGIC it only reads tables.
# MAGIC
# MAGIC **Run simultaneously with**:
# MAGIC [`continuousSim-agentRunner.py`](./continuousSim-agentRunner.py) in a
# MAGIC separate tab. This notebook can be started before, during, or after the
# MAGIC runner - it renders whatever data is in the tables at each poll interval.
# MAGIC
# MAGIC **Stop** by clicking **Interrupt**. An optional final static render runs
# MAGIC in Section 4 after the poll loop exits.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Plot structure** (four stacked subplots, shared x-axis):
# MAGIC 1. Stock on hand + demand + reorder markers
# MAGIC 2. Demand fulfilment: fulfilled vs. unmet, stockout ticks shaded
# MAGIC 3. Disruptions: effective magnitude per tick per disruption
# MAGIC 4. Costs: stacked area per component per tick + cumulative total

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Install dependencies

# COMMAND ----------

# MAGIC %pip install pydantic numpy matplotlib pandas
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters
# MAGIC
# MAGIC `SIM_ID` must match the runner notebook. Everything else is dashboard behaviour.
# MAGIC
# MAGIC | Parameter | Description |
# MAGIC |---|---|
# MAGIC | `SIM_ID` | Must match `SIM_ID` in the runner notebook. |
# MAGIC | `POLL_INTERVAL_SECONDS` | Seconds between table queries and plot refreshes. 5–10s recommended. |
# MAGIC | `MAX_TICKS_TO_SHOW` | Rolling window: only the last N ticks are plotted. `None` = all ticks. Keeps the plot readable as the run extends. |

# COMMAND ----------

SIM_ID               = "sim_continuous_001"
POLL_INTERVAL_SECONDS = 5.0   # seconds between refreshes
MAX_TICKS_TO_SHOW     = 60    # rolling window; None = show all ticks
CATALOG               = "hackathon_of_the_century"

print(f"Dashboard parameters set.")
print(f"  SIM_ID               : {SIM_ID}")
print(f"  POLL_INTERVAL_SECONDS: {POLL_INTERVAL_SECONDS}")
print(f"  MAX_TICKS_TO_SHOW    : {MAX_TICKS_TO_SHOW}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports

# COMMAND ----------

import time
from datetime import datetime, timezone

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
import numpy as np
import pandas as pd
from IPython.display import clear_output, display

matplotlib.rcParams["figure.dpi"] = 110

print("Imports resolved.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Polling loop
# MAGIC
# MAGIC **This cell runs indefinitely until you click Interrupt.**
# MAGIC
# MAGIC Each iteration:
# MAGIC 1. Queries all source tables for this `SIM_ID`, optionally limited to the
# MAGIC    last `MAX_TICKS_TO_SHOW` ticks
# MAGIC 2. Clears the previous cell output and re-renders the composite figure
# MAGIC 3. Sleeps for `POLL_INTERVAL_SECONDS`
# MAGIC
# MAGIC If the runner has not started yet (no rows in the tables), the dashboard
# MAGIC prints a waiting message and retries. It does not require the runner to be
# MAGIC active - it renders whatever data exists at each poll.
# MAGIC
# MAGIC **Display behaviour**: `IPython.display.clear_output(wait=True)` replaces
# MAGIC the previous figure output with the new one in place, rather than appending
# MAGIC a new figure on each poll.

# COMMAND ----------

def _fetch_data(sim_id: str, max_ticks: int | None) -> dict[str, pd.DataFrame]:
    '''
    Query all source tables for this sim_id and return as a dict of DataFrames.

    If max_ticks is set, only the last max_ticks ticks are returned for each
    table. The tick range is derived from the max tick currently in
    ops_warehouse_state, which is written at the end of every tick by the runner.

    NOTE: Each table is fetched with a single Spark SQL query. All queries are
    independent - no joins between tables. This keeps each fetch fast and avoids
    any schema coupling between the query layer and the table layer.
    '''

    # Resolve the rolling window bounds from the latest tick in ops_warehouse_state.
    # This table is written every tick, so its MAX(tick) is the most reliable
    # proxy for "how far the simulation has progressed".
    tick_range_rows = spark.sql(f'''
        SELECT MAX(tick) AS max_tick
        FROM {CATALOG}.tables4ops.ops_warehouse_state
        WHERE sim_id = '{sim_id}'
    ''').collect()

    if not tick_range_rows or tick_range_rows[0]["max_tick"] is None:
        return {}   # no data yet - runner has not started

    max_tick  = tick_range_rows[0]["max_tick"]
    min_tick  = max(0, max_tick - max_ticks + 1) if max_ticks else 0
    tick_cond = f"AND tick >= {min_tick}"

    # ── Stock on hand ────────────────────────────────────────────────────────
    df_stock = spark.sql(f'''
        SELECT tick, item_id, stock_on_hand, stock_in_transit
        FROM {CATALOG}.tables4ops.ops_warehouse_state
        WHERE sim_id = '{sim_id}' {tick_cond}
        ORDER BY tick, item_id
    ''').toPandas()

    # ── Demand ───────────────────────────────────────────────────────────────
    df_demand = spark.sql(f'''
        SELECT tick, item_id, raw_demand, disrupted_demand,
               fulfilled_demand, unmet_demand
        FROM {CATALOG}.tables4hist.hist_demand_actuals
        WHERE sim_id = '{sim_id}' {tick_cond}
        ORDER BY tick, item_id
    ''').toPandas()

    # ── Reorder decisions ────────────────────────────────────────────────────
    df_decisions = spark.sql(f'''
        SELECT tick, item_id, decision, order_qty
        FROM {CATALOG}.tables4hist.hist_reorder_decisions
        WHERE sim_id = '{sim_id}' {tick_cond}
          AND decision = 'reorder'
        ORDER BY tick, item_id
    ''').toPandas()

    # ── Stockout events ──────────────────────────────────────────────────────
    # Fetched from event_log rather than hist_demand_actuals to get the exact
    # ticks the engine flagged as stockouts (unmet_demand > 0 after demand draw).
    df_stockouts = spark.sql(f'''
        SELECT tick,
               item_id,
               CAST(get_json_object(payload, '$.unmet_demand')  AS DOUBLE) AS unmet_demand
        FROM {CATALOG}.tables4eventlog.event_log
        WHERE sim_id = '{sim_id}'
          AND event_type = 'STOCKOUT_OCCURRED'
          {tick_cond}
        ORDER BY tick, item_id
    ''').toPandas()

    # ── Disruptions ──────────────────────────────────────────────────────────
    df_disruptions = spark.sql(f'''
        SELECT tick, disruption_id, disruption_type, item_id,
               effective_magnitude, is_active_this_tick
        FROM {CATALOG}.tables4ops.ops_active_disruptions
        WHERE sim_id = '{sim_id}' {tick_cond}
        ORDER BY tick, disruption_id
    ''').toPandas()

    # ── Costs per tick ───────────────────────────────────────────────────────
    df_costs = spark.sql(f'''
        SELECT tick, item_id, holding_cost, stockout_cost,
               order_cost, transit_loss_cost
        FROM {CATALOG}.tables4hist.hist_cost_by_tick
        WHERE sim_id = '{sim_id}' {tick_cond}
        ORDER BY tick, item_id
    ''').toPandas()

    # ── Cumulative cost (for secondary axis on subplot 4) ────────────────────
    # MAX(tick) row per item gives the latest cumulative totals.
    df_cumulative = spark.sql(f'''
        SELECT c.tick, c.item_id, c.cumulative_total_cost
        FROM {CATALOG}.tables4ops.ops_cost_accumulator c
        JOIN (
            SELECT item_id, MAX(tick) AS max_tick
            FROM {CATALOG}.tables4ops.ops_cost_accumulator
            WHERE sim_id = '{sim_id}'
            GROUP BY item_id
        ) latest ON c.item_id = latest.item_id AND c.tick = latest.max_tick
        WHERE c.sim_id = '{sim_id}'
    ''').toPandas()

    return {
        "stock":       df_stock,
        "demand":      df_demand,
        "decisions":   df_decisions,
        "stockouts":   df_stockouts,
        "disruptions": df_disruptions,
        "costs":       df_costs,
        "cumulative":  df_cumulative,
        "max_tick":    max_tick,
        "min_tick":    min_tick,
    }


def _render(data: dict, sim_id: str, poll_count: int) -> None:
    '''
    Render the four-subplot composite figure from the fetched DataFrames.

    Subplot layout (top to bottom, shared x-axis):
      1. Stock + demand + reorder markers  (tall)
      2. Demand fulfilment                 (medium)
      3. Disruptions                       (medium)
      4. Costs                             (medium)

    NOTE: plt.close("all") is called before creating the new figure to
    release memory from the previous poll's figure. Without this, repeated
    re-renders over a long run will accumulate figures in memory.
    '''

    plt.close("all")

    df_stock       = data["stock"]
    df_demand      = data["demand"]
    df_decisions   = data["decisions"]
    df_stockouts   = data["stockouts"]
    df_disruptions = data["disruptions"]
    df_costs       = data["costs"]
    max_tick       = data["max_tick"]
    min_tick       = data["min_tick"]

    items      = sorted(df_stock["item_id"].unique()) if not df_stock.empty else []
    ticks      = sorted(df_stock["tick"].unique()) if not df_stock.empty else []

    # ── Colour palette ────────────────────────────────────────────────────────
    # One colour per item, consistent across all subplots.
    # Disruptions use a separate palette so they do not clash with item colours.
    ITEM_COLOURS = {
        items[0]: "#4C72B0" if len(items) > 0 else "#4C72B0",
        items[1]: "#DD8452" if len(items) > 1 else "#4C72B0",
    } if items else {}
    DIS_COLOURS  = ["#8C3B3B", "#3B608C", "#3B8C56", "#7A3B8C"]

    # ── Figure layout ─────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(16, 18))
    gs  = gridspec.GridSpec(
        4, 1,
        figure    = fig,
        hspace    = 0.40,
        height_ratios = [3, 2, 2, 2],
    )
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1], sharex=ax1)
    ax3 = fig.add_subplot(gs[2], sharex=ax1)
    ax4 = fig.add_subplot(gs[3], sharex=ax1)

    # ── Figure header ─────────────────────────────────────────────────────────
    refreshed_at = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    fig.suptitle(
        f"Live Dashboard  │  sim_id={sim_id!r}  │  "
        f"tick={max_tick}  │  refreshed {refreshed_at}  │  poll #{poll_count}",
        fontsize=11, fontweight="bold", y=0.995,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Subplot 1: Stock on hand (left axis) + disrupted demand (right axis)
    #            + reorder markers
    # ─────────────────────────────────────────────────────────────────────────
    ax1.set_title("Stock on Hand & Demand", fontsize=10, loc="left")
    ax1_r = ax1.twinx()   # right axis for demand

    stockout_ticks_all = set(df_stockouts["tick"].tolist()) if not df_stockouts.empty else set()

    for tick in stockout_ticks_all:
        ax1.axvspan(tick - 0.5, tick + 0.5, alpha=0.18, color="red", zorder=0)

    for item_id in items:
        colour = ITEM_COLOURS.get(item_id, "grey")

        s = df_stock[df_stock["item_id"] == item_id].sort_values("tick")
        ax1.plot(s["tick"], s["stock_on_hand"],
                 linewidth=2.0, color=colour, label=f"{item_id} stock")

        d = df_demand[df_demand["item_id"] == item_id].sort_values("tick")
        ax1_r.plot(d["tick"], d["disrupted_demand"],
                   linewidth=1.2, color=colour, linestyle="--",
                   alpha=0.6, label=f"{item_id} demand")

        # Reorder markers: downward triangle, sized by order_qty
        r = df_decisions[df_decisions["item_id"] == item_id]
        if not r.empty:
            ax1.scatter(
                r["tick"], r["order_qty"],
                marker="v", s=r["order_qty"] * 5,
                color=colour, alpha=0.9, zorder=4,
                label=f"{item_id} reorder",
            )

    ax1.set_ylabel("Stock on hand (units)", fontsize=9)
    ax1_r.set_ylabel("Disrupted demand (units)", fontsize=9, color="grey")
    ax1.legend(loc="upper left", fontsize=8)
    ax1.grid(axis="y", alpha=0.3)

    # Annotate with reorder-point lines per item (horizontal reference lines)
    # NOTE: reorder_point is not in the Delta tables; omitted to avoid requiring
    # a join to env_item_types. Add if needed.

    # ─────────────────────────────────────────────────────────────────────────
    # Subplot 2: Demand fulfilment - fulfilled vs. unmet
    # ─────────────────────────────────────────────────────────────────────────
    ax2.set_title("Demand Fulfilment", fontsize=10, loc="left")

    for tick in stockout_ticks_all:
        ax2.axvspan(tick - 0.5, tick + 0.5, alpha=0.18, color="red", zorder=0)

    for item_id in items:
        colour = ITEM_COLOURS.get(item_id, "grey")
        d = df_demand[df_demand["item_id"] == item_id].sort_values("tick")
        ax2.plot(d["tick"], d["fulfilled_demand"],
                 linewidth=1.8, color=colour, label=f"{item_id} fulfilled")
        # Unmet demand filled to zero - visually distinguishes stockout depth
        ax2.fill_between(d["tick"], d["unmet_demand"], 0,
                         color="red", alpha=0.25, label=f"{item_id} unmet" if item_id == items[0] else "")

    ax2.set_ylabel("Units", fontsize=9)
    ax2.legend(loc="upper left", fontsize=8)
    ax2.grid(axis="y", alpha=0.3)

    # ─────────────────────────────────────────────────────────────────────────
    # Subplot 3: Disruptions - effective magnitude per tick per disruption
    # ─────────────────────────────────────────────────────────────────────────
    ax3.set_title("Disruptions", fontsize=10, loc="left")

    if not df_disruptions.empty:
        disruption_ids = sorted(df_disruptions["disruption_id"].unique())
        for idx, dis_id in enumerate(disruption_ids):
            colour  = DIS_COLOURS[idx % len(DIS_COLOURS)]
            subset  = df_disruptions[df_disruptions["disruption_id"] == dis_id].sort_values("tick")
            # Plot effective magnitude only when the disruption was active this tick;
            # zero otherwise - makes inactive ticks visually clear
            subset  = subset.copy()
            subset["plot_mag"] = subset.apply(
                lambda r: r["effective_magnitude"] if r["is_active_this_tick"] else 0.0,
                axis=1,
            )
            dis_type = subset["disruption_type"].iloc[0] if not subset.empty else ""
            ax3.fill_between(
                subset["tick"], subset["plot_mag"], 0,
                color=colour, alpha=0.55,
                label=f"{dis_id} ({dis_type})",
            )
            ax3.plot(subset["tick"], subset["plot_mag"],
                     linewidth=1.0, color=colour, alpha=0.8)

    ax3.set_ylabel("Effective magnitude", fontsize=9)
    ax3.legend(loc="upper left", fontsize=8)
    ax3.grid(axis="y", alpha=0.3)

    # ─────────────────────────────────────────────────────────────────────────
    # Subplot 4: Costs - stacked area per component + cumulative total step line
    # ─────────────────────────────────────────────────────────────────────────
    ax4.set_title("Cost per Tick (stacked) + Cumulative Total", fontsize=10, loc="left")
    ax4_r = ax4.twinx()   # right axis for cumulative total

    COST_COLS   = ["holding_cost", "stockout_cost", "order_cost", "transit_loss_cost"]
    COST_LABELS = ["Holding",      "Stockout",      "Order",      "Transit Loss"]
    COST_COLOURS = ["#4878CF",     "#D65F5F",        "#6ACC65",    "#B47CC7"]

    if not df_costs.empty:
        # Aggregate across all items per tick for the stacked area
        cost_agg = (
            df_costs.groupby("tick")[COST_COLS]
            .sum()
            .reset_index()
            .sort_values("tick")
        )
        ticks_c  = cost_agg["tick"].values
        bottoms  = np.zeros(len(ticks_c))
        for col, label, colour in zip(COST_COLS, COST_LABELS, COST_COLOURS):
            vals = cost_agg[col].values
            ax4.bar(ticks_c, vals, bottom=bottoms, width=0.8,
                    color=colour, alpha=0.75, label=label)
            bottoms += vals

    # Cumulative total as a step line on the right axis
    # NOTE: _cumulative holds the latest cumulative_total_cost per item.
    # Sum across items to get the run-level total. Because _cumulative only
    # has the single latest row per item (not per tick), we build a running
    # sum from df_costs instead, which has per-tick per-item data.
    if not df_costs.empty:
        running_total = (
            df_costs.groupby("tick")[COST_COLS]
            .sum()
            .sum(axis=1)
            .sort_index()
            .cumsum()
            .reset_index()
        )
        running_total.columns = ["tick", "cumulative"]
        ax4_r.step(running_total["tick"], running_total["cumulative"],
                   color="black", linewidth=1.5, linestyle="--",
                   alpha=0.7, label="Cumulative total")
        ax4_r.set_ylabel("Cumulative cost", fontsize=9)
        ax4_r.tick_params(axis="y", labelcolor="black")

    ax4.set_ylabel("Tick cost", fontsize=9)
    ax4.set_xlabel("Tick", fontsize=9)
    ax4.legend(loc="upper left", fontsize=8)
    ax4.grid(axis="y", alpha=0.3)

    cost_handles = [
        mpatches.Patch(color=c, label=l, alpha=0.75)
        for l, c in zip(COST_LABELS, COST_COLOURS)
    ]
    ax4.legend(handles=cost_handles, loc="upper left", fontsize=8)

    # ── Shared x-axis range ───────────────────────────────────────────────────
    if ticks:
        ax1.set_xlim(min_tick - 0.5, max_tick + 0.5)

    plt.setp(ax1.get_xticklabels(), visible=False)
    plt.setp(ax2.get_xticklabels(), visible=False)
    plt.setp(ax3.get_xticklabels(), visible=False)

    return fig


# ── Poll loop ─────────────────────────────────────────────────────────────────
# Runs until Interrupt. Each iteration:
#   1. Fetch data from Delta tables
#   2. Clear previous output and re-render the figure
#   3. Sleep POLL_INTERVAL_SECONDS
#
# NOTE: clear_output(wait=True) replaces the previous figure output with the
# new one in place rather than appending. Without it, each poll appends a new
# figure to the cell output, which quickly becomes unwieldy.

poll_count   = 0
waiting_shown = False

try:
    while True:
        poll_count += 1
        data = _fetch_data(sim_id=SIM_ID, max_ticks=MAX_TICKS_TO_SHOW)

        clear_output(wait=True)

        if not data:
            # Runner has not started yet or sim_id does not exist in tables.
            # Print a waiting message and retry after the poll interval.
            print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
                  f"Waiting for data from sim_id={SIM_ID!r} "
                  f"(poll #{poll_count})...")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        fig = _render(data, sim_id=SIM_ID, poll_count=poll_count)
        display(fig)
        plt.close(fig)

        time.sleep(POLL_INTERVAL_SECONDS)

except KeyboardInterrupt:
    print(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] "
          f"Dashboard interrupted after {poll_count} polls.")
    print("Run Section 4 below for a final static render.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Final static render (run after Interrupt)
# MAGIC
# MAGIC Renders the plot one final time against whatever data exists in the tables.
# MAGIC Does not poll - fetches once and renders.

# COMMAND ----------

data = _fetch_data(sim_id=SIM_ID, max_ticks=MAX_TICKS_TO_SHOW)

if not data:
    print(f"No data found for sim_id='{SIM_ID}'. Run the agent runner notebook first.")
else:
    fig = _render(data, sim_id=SIM_ID, poll_count=-1)
    display(fig)
    plt.close(fig)
    print(f"Final render complete. Max tick: {data['max_tick']}.")
