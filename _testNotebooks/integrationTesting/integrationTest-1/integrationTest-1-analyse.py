# Databricks notebook source
# MAGIC %md
# MAGIC <h1>Integration Test 1 - Analyse Notebook<br><i>Rule-Based vs. LLM Agent for Finite Ticks</i></h1>
# MAGIC
# MAGIC **Purpose**: Load results from Delta tables and produce plots, summary tables, and evaluation queries.
# MAGIC Reads only from Delta tables. Re-runnable independently once the **Run** notebook has completed.
# MAGIC
# MAGIC **Structure**:
# MAGIC - **Section 2.0** - Load data into pandas
# MAGIC - **Section 2.1** - Plots (1-6)
# MAGIC - **Section 2.2** - Summary Tables (1-6)
# MAGIC - **Section 2.3** - Evaluation Queries (Q1-Q9)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.0 Parameters and imports
# MAGIC
# MAGIC These values must match those used in the **Run** notebook exactly.

# COMMAND ----------

# MAGIC %pip install pandas matplotlib
# MAGIC %restart_python

# COMMAND ----------

# ── Simulation parameters - must match the Run notebook ─────────────────────
SIM_SEED  = 42
NUM_TICKS = 20
TICK_UNIT = "hour"

SIM_ID_RULEBASED = "integration_test_1_rulebased_001"
SIM_ID_LLM       = "integration_test_1_llm_001"

CATALOG = "hackathon_of_the_century"

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

# Shared derived values
ALL_SIM_IDS   = f"('{SIM_ID_RULEBASED}', '{SIM_ID_LLM}')"
AGENT_LABELS  = {SIM_ID_RULEBASED: "Rule-Based", SIM_ID_LLM: "LLM"}
AGENT_COLOURS = {SIM_ID_RULEBASED: "#4C72B0",    SIM_ID_LLM: "#DD8452"}
TICKS         = list(range(NUM_TICKS))

print("Parameters and imports ready.")
print(f"  SIM_ID_RULEBASED : {SIM_ID_RULEBASED}")
print(f"  SIM_ID_LLM       : {SIM_ID_LLM}")
print(f"  CATALOG          : {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.0 Load data into pandas
# MAGIC
# MAGIC All data is loaded once here. Plot and table cells reference these DataFrames.

# COMMAND ----------

# Disruptions (same world for both agents - only one sim_id needed)
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

# Demand actuals
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

# Reorder decisions
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

    x     = range(len(pivot.index))
    width = 0.35
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
        row    = row.iloc[0]
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

    demand_sub     = df_demand[df_demand["sim_id"] == sim_id]
    decision_sub   = df_decisions[df_decisions["sim_id"] == sim_id]
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
        dec      = decision_sub[decision_sub["item_id"] == item_id].sort_values("tick")
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
        ax5.legend(
            handles=ax5.get_legend_handles_labels()[0] + [red_patch],
            labels=ax5.get_legend_handles_labels()[1] + ["Stockout tick"],
            fontsize=9,
        )
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
        ticks_active            = ("is_active_this_tick", "sum"),
        avg_effective_magnitude = ("effective_magnitude",  "mean"),
        max_effective_magnitude = ("effective_magnitude",  "max"),
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
            stockout_ticks      = ("tick",          "count"),
            total_unmet_demand  = ("unmet_demand",  "sum"),
            total_stockout_cost = ("stockout_cost", "sum"),
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
    print("Table 5 - Agent Health (LLM run): no fallback or failure events. Clean run.")
else:
    table5 = (
        df_agent_health
        .groupby("event_type")
        .agg(count=("tick", "count"))
        .reset_index()
        .sort_values("event_type")
    )
    print("Table 5 - Agent Health (LLM run) - NON-ZERO COUNTS INDICATE FALLBACKS")
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
           SUM(c.cumulative_stockout_cost)    AS total_stockout_cost,
           SUM(c.cumulative_holding_cost)      AS total_holding_cost,
           SUM(c.cumulative_order_cost)        AS total_order_cost,
           SUM(c.cumulative_transit_loss_cost) AS total_transit_loss_cost,
           SUM(c.cumulative_total_cost)        AS total_cost,
           MIN(c.remaining_budget)             AS remaining_budget
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
