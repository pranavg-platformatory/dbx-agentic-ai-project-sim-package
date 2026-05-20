"""
warehouse_sim/viz/dashboard.py

Visualisation layer. Reads from hist_* and ops_* tables for a given
sim_id and produces matplotlib figures.

No engine, agent, or Spark write dependency.
All functions accept a SparkSession and return matplotlib Figure objects
so they can be rendered in a Databricks notebook or saved to disk.

Usage:
    from warehouse_sim.viz.dashboard import SimDashboard
    dash = SimDashboard(spark, sim_id="sim_001")
    dash.plot_stock()
    dash.plot_demand()
    dash.plot_costs()
    dash.plot_cumulative_cost()
    dash.plot_decisions()
    dash.plot_all()
"""

from __future__ import annotations

from typing import Optional, TYPE_CHECKING

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.figure import Figure

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Colour palette - consistent across all charts
# ---------------------------------------------------------------------------

ITEM_COLOURS = [
    "#4C72B0", "#DD8452", "#55A868", "#C44E52",
    "#8172B2", "#937860", "#DA8BC3", "#8C8C8C",
]

COST_COLOURS = {
    "holding_cost":      "#4C72B0",
    "stockout_cost":     "#C44E52",
    "order_cost":        "#55A868",
    "transit_loss_cost": "#DD8452",
}

DISRUPTION_COLOUR = "#C44E52"
REORDER_COLOUR    = "#55A868"
HOLD_COLOUR       = "#AAAAAA"


# ---------------------------------------------------------------------------
# SimDashboard
# ---------------------------------------------------------------------------

class SimDashboard:
    """
    Reads simulation output tables for one sim_id and exposes
    one plot method per view.

    All DataFrames are loaded lazily and cached on first access.
    """

    def __init__(self, spark: "SparkSession", sim_id: str) -> None:
        self._spark  = spark
        self._sim_id = sim_id
        self._cat    = "hackathon_of_the_century"

        # Lazy caches
        self._df_stock:      Optional[pd.DataFrame] = None
        self._df_demand:     Optional[pd.DataFrame] = None
        self._df_cost_tick:  Optional[pd.DataFrame] = None
        self._df_cost_cum:   Optional[pd.DataFrame] = None
        self._df_decisions:  Optional[pd.DataFrame] = None
        self._df_disruptions: Optional[pd.DataFrame] = None

    # ------------------------------------------------------------------
    # Data loaders
    # ------------------------------------------------------------------

    def _load(self, query: str) -> pd.DataFrame:
        return self._spark.sql(query).toPandas()

    @property
    def stock(self) -> pd.DataFrame:
        if self._df_stock is None:
            self._df_stock = self._load(f"""
                SELECT tick, item_id, stock_on_hand, stock_in_transit,
                       expected_arrivals_next_tick
                FROM {self._cat}.tables4ops.ops_warehouse_state
                WHERE sim_id = '{self._sim_id}'
                ORDER BY item_id, tick
            """)
        return self._df_stock

    @property
    def demand(self) -> pd.DataFrame:
        if self._df_demand is None:
            self._df_demand = self._load(f"""
                SELECT tick, item_id, raw_demand, disrupted_demand,
                       fulfilled_demand, unmet_demand
                FROM {self._cat}.tables4hist.hist_demand_actuals
                WHERE sim_id = '{self._sim_id}'
                ORDER BY item_id, tick
            """)
        return self._df_demand

    @property
    def cost_by_tick(self) -> pd.DataFrame:
        if self._df_cost_tick is None:
            self._df_cost_tick = self._load(f"""
                SELECT tick, item_id, holding_cost, stockout_cost,
                       order_cost, transit_loss_cost, total_cost
                FROM {self._cat}.tables4hist.hist_cost_by_tick
                WHERE sim_id = '{self._sim_id}'
                ORDER BY item_id, tick
            """)
        return self._df_cost_tick

    @property
    def cost_cumulative(self) -> pd.DataFrame:
        if self._df_cost_cum is None:
            self._df_cost_cum = self._load(f"""
                SELECT tick, item_id, cumulative_total_cost, remaining_budget
                FROM {self._cat}.tables4ops.ops_cost_accumulator
                WHERE sim_id = '{self._sim_id}'
                ORDER BY item_id, tick
            """)
        return self._df_cost_cum

    @property
    def decisions(self) -> pd.DataFrame:
        if self._df_decisions is None:
            self._df_decisions = self._load(f"""
                SELECT tick, item_id, decision, order_qty,
                       stock_on_hand_at_decision
                FROM {self._cat}.tables4hist.hist_reorder_decisions
                WHERE sim_id = '{self._sim_id}'
                ORDER BY item_id, tick
            """)
        return self._df_decisions

    @property
    def disruptions(self) -> pd.DataFrame:
        if self._df_disruptions is None:
            self._df_disruptions = self._load(f"""
                SELECT tick, item_id, disruption_type,
                       effective_magnitude, is_active_this_tick
                FROM {self._cat}.tables4ops.ops_active_disruptions
                WHERE sim_id = '{self._sim_id}'
                  AND is_active_this_tick = true
                ORDER BY item_id, tick
            """)
        return self._df_disruptions

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _items(self) -> list[str]:
        return sorted(self.stock["item_id"].unique())

    def _item_colour(self, item_id: str) -> str:
        items = self._items()
        idx   = items.index(item_id) if item_id in items else 0
        return ITEM_COLOURS[idx % len(ITEM_COLOURS)]

    def _shade_disruptions(
        self,
        ax:      plt.Axes,
        item_id: str,
    ) -> None:
        """Shade disruption-active ticks on an axes."""
        if self.disruptions.empty:
            return
        item_dis = self.disruptions[self.disruptions["item_id"] == item_id]
        for tick in item_dis["tick"].unique():
            ax.axvspan(tick - 0.5, tick + 0.5,
                       alpha=0.15, color=DISRUPTION_COLOUR, zorder=0)

    @staticmethod
    def _style(ax: plt.Axes, title: str, xlabel: str = "Tick",
               ylabel: str = "", legend: bool = True) -> None:
        ax.set_title(title, fontsize=11, fontweight="bold", pad=8)
        ax.set_xlabel(xlabel, fontsize=9)
        ax.set_ylabel(ylabel, fontsize=9)
        ax.tick_params(labelsize=8)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        if legend:
            ax.legend(fontsize=8, framealpha=0.7)

    # ------------------------------------------------------------------
    # 1. Stock over time
    # ------------------------------------------------------------------

    def plot_stock(self) -> Figure:
        """
        Stock on hand and stock in transit per item, per tick.
        Disruption-active ticks shaded in red.
        """
        items = self._items()
        fig, axes = plt.subplots(
            len(items), 1,
            figsize=(12, 3.5 * len(items)),
            sharex=True,
        )
        if len(items) == 1:
            axes = [axes]

        fig.suptitle(
            f"Stock Levels Over Time  |  sim_id: {self._sim_id}",
            fontsize=13, fontweight="bold", y=1.01,
        )

        for ax, item_id in zip(axes, items):
            df   = self.stock[self.stock["item_id"] == item_id].sort_values("tick")
            col  = self._item_colour(item_id)

            ax.fill_between(df["tick"], df["stock_on_hand"],
                            alpha=0.25, color=col)
            ax.plot(df["tick"], df["stock_on_hand"],
                    color=col, lw=2, label="Stock on hand")
            ax.plot(df["tick"], df["stock_in_transit"],
                    color=col, lw=1.5, ls="--", alpha=0.7,
                    label="In transit")

            # Reorder point reference line
            # (read from decisions table if available)
            rp_rows = self.decisions[self.decisions["item_id"] == item_id]
            if not rp_rows.empty:
                # reorder point not stored here - annotate reorder events instead
                reorder_ticks = rp_rows[rp_rows["decision"] == "reorder"]["tick"]
                for rt in reorder_ticks:
                    ax.axvline(rt, color=REORDER_COLOUR, lw=1, ls=":", alpha=0.7)

            self._shade_disruptions(ax, item_id)
            self._style(ax, f"{item_id} - Stock", ylabel="Units")

        fig.tight_layout()
        return fig

    # ------------------------------------------------------------------
    # 2. Demand vs fulfilment
    # ------------------------------------------------------------------

    def plot_demand(self) -> Figure:
        """
        Disrupted demand vs fulfilled demand vs unmet demand per item.
        Unmet demand shown as a stacked red area (stockout signal).
        """
        items = self._items()
        fig, axes = plt.subplots(
            len(items), 1,
            figsize=(12, 3.5 * len(items)),
            sharex=True,
        )
        if len(items) == 1:
            axes = [axes]

        fig.suptitle(
            f"Demand vs Fulfilment  |  sim_id: {self._sim_id}",
            fontsize=13, fontweight="bold", y=1.01,
        )

        for ax, item_id in zip(axes, items):
            df  = self.demand[self.demand["item_id"] == item_id].sort_values("tick")
            col = self._item_colour(item_id)

            ax.plot(df["tick"], df["disrupted_demand"],
                    color=col, lw=2, label="Disrupted demand")
            ax.fill_between(df["tick"], df["fulfilled_demand"],
                            alpha=0.3, color=col, label="Fulfilled")
            ax.fill_between(df["tick"], df["fulfilled_demand"],
                            df["disrupted_demand"],
                            alpha=0.4, color=DISRUPTION_COLOUR,
                            label="Unmet (stockout)")

            self._shade_disruptions(ax, item_id)
            self._style(ax, f"{item_id} - Demand", ylabel="Units")

        fig.tight_layout()
        return fig

    # ------------------------------------------------------------------
    # 3. Cost breakdown per tick (stacked bar)
    # ------------------------------------------------------------------

    def plot_costs(self) -> Figure:
        """
        Stacked bar chart of cost components per tick per item.
        """
        items      = self._items()
        components = ["holding_cost", "stockout_cost", "order_cost", "transit_loss_cost"]
        labels     = ["Holding", "Stockout", "Order", "Transit loss"]

        fig, axes = plt.subplots(
            len(items), 1,
            figsize=(12, 3.5 * len(items)),
            sharex=True,
        )
        if len(items) == 1:
            axes = [axes]

        fig.suptitle(
            f"Cost Breakdown Per Tick  |  sim_id: {self._sim_id}",
            fontsize=13, fontweight="bold", y=1.01,
        )

        for ax, item_id in zip(axes, items):
            df      = self.cost_by_tick[self.cost_by_tick["item_id"] == item_id] \
                          .sort_values("tick")
            ticks   = df["tick"].values
            bottoms = [0.0] * len(ticks)

            for comp, label in zip(components, labels):
                vals = df[comp].values
                ax.bar(ticks, vals, bottom=bottoms,
                       color=COST_COLOURS[comp], label=label,
                       width=0.8, alpha=0.85)
                bottoms = [b + v for b, v in zip(bottoms, vals)]

            self._shade_disruptions(ax, item_id)
            self._style(ax, f"{item_id} - Cost Per Tick", ylabel="Cost (£)")

        fig.tight_layout()
        return fig

    # ------------------------------------------------------------------
    # 4. Cumulative cost over time
    # ------------------------------------------------------------------

    def plot_cumulative_cost(self) -> Figure:
        """
        Cumulative total cost per item over the simulation.
        Remaining budget shown as a horizontal dashed line (if applicable).
        """
        items = self._items()
        fig, ax = plt.subplots(figsize=(12, 4))

        for item_id in items:
            df  = self.cost_cumulative[self.cost_cumulative["item_id"] == item_id] \
                      .sort_values("tick")
            col = self._item_colour(item_id)
            ax.plot(df["tick"], df["cumulative_total_cost"],
                    color=col, lw=2, label=item_id)

        # Remaining budget reference (take from most recent row)
        if not self.cost_cumulative.empty:
            budget_vals = self.cost_cumulative["remaining_budget"].dropna()
            if not budget_vals.empty:
                # budget_limit = remaining + spent; use first row's remaining + cum_total
                first = self.cost_cumulative.sort_values("tick").iloc[0]
                # Approximate budget_limit from config - show initial remaining_budget
                initial_budget = self.cost_cumulative.sort_values("tick") \
                                     .groupby("item_id")["remaining_budget"].first()
                # Show the global remaining_budget line from the last tick
                last_remaining = self.cost_cumulative.sort_values("tick").iloc[-1]["remaining_budget"]
                if last_remaining is not None and not pd.isna(last_remaining):
                    ax.axhline(last_remaining, color="#888888", lw=1.5,
                               ls="--", alpha=0.7, label="Remaining budget")

        self._style(ax,
            f"Cumulative Cost Over Time  |  sim_id: {self._sim_id}",
            ylabel="Cumulative Cost (£)",
        )
        fig.tight_layout()
        return fig

    # ------------------------------------------------------------------
    # 5. Reorder decisions
    # ------------------------------------------------------------------

    def plot_decisions(self) -> Figure:
        """
        Reorder decisions over time: order quantity as a bar when reorder,
        zero line for hold. Stock at decision time shown as a line overlay.
        """
        items = self._items()
        fig, axes = plt.subplots(
            len(items), 1,
            figsize=(12, 3.5 * len(items)),
            sharex=True,
        )
        if len(items) == 1:
            axes = [axes]

        fig.suptitle(
            f"Reorder Decisions  |  sim_id: {self._sim_id}",
            fontsize=13, fontweight="bold", y=1.01,
        )

        for ax, item_id in zip(axes, items):
            df  = self.decisions[self.decisions["item_id"] == item_id].sort_values("tick")
            col = self._item_colour(item_id)

            reorders = df[df["decision"] == "reorder"]
            holds    = df[df["decision"] == "hold"]

            ax.bar(reorders["tick"], reorders["order_qty"],
                   color=REORDER_COLOUR, alpha=0.8, width=0.6, label="Reorder qty")
            ax.bar(holds["tick"], [0] * len(holds),
                   color=HOLD_COLOUR, alpha=0.4, width=0.6, label="Hold")

            # Stock at decision time (secondary y-axis)
            ax2 = ax.twinx()
            ax2.plot(df["tick"], df["stock_on_hand_at_decision"],
                     color=col, lw=1.5, ls="--", alpha=0.8,
                     label="Stock at decision")
            ax2.set_ylabel("Stock on hand", fontsize=8, color=col)
            ax2.tick_params(labelsize=8, colors=col)
            ax2.spines["top"].set_visible(False)

            self._shade_disruptions(ax, item_id)
            self._style(ax, f"{item_id} - Decisions", ylabel="Order Qty")

            # Combined legend
            handles1, labels1 = ax.get_legend_handles_labels()
            handles2, labels2 = ax2.get_legend_handles_labels()
            ax.legend(handles1 + handles2, labels1 + labels2,
                      fontsize=8, framealpha=0.7)

        fig.tight_layout()
        return fig

    # ------------------------------------------------------------------
    # 6. Plot all
    # ------------------------------------------------------------------

    def plot_all(self) -> dict[str, Figure]:
        """
        Render all five charts and return them as a dict keyed by name.
        In a Databricks notebook, each figure displays automatically.
        """
        figures = {}

        print("Rendering stock levels...")
        figures["stock"]           = self.plot_stock()
        plt.show()

        print("Rendering demand vs fulfilment...")
        figures["demand"]          = self.plot_demand()
        plt.show()

        print("Rendering cost breakdown per tick...")
        figures["costs"]           = self.plot_costs()
        plt.show()

        print("Rendering cumulative cost...")
        figures["cumulative_cost"] = self.plot_cumulative_cost()
        plt.show()

        print("Rendering reorder decisions...")
        figures["decisions"]       = self.plot_decisions()
        plt.show()

        return figures

    # ------------------------------------------------------------------
    # Summary statistics (text, for notebook cell output)
    # ------------------------------------------------------------------

    def print_summary(self) -> None:
        """Print a concise run summary to stdout."""
        items = self._items()

        print(f"\n{'='*55}")
        print(f"  Simulation Summary  |  {self._sim_id}")
        print(f"{'='*55}")

        if not self.cost_cumulative.empty:
            total_cost = self.cost_cumulative.groupby("item_id")["cumulative_total_cost"].max().sum()
            print(f"  Total cost (all items):  £{total_cost:,.2f}")

        if not self.decisions.empty:
            n_reorders = (self.decisions["decision"] == "reorder").sum()
            n_holds    = (self.decisions["decision"] == "hold").sum()
            print(f"  Reorder decisions:       {n_reorders}")
            print(f"  Hold decisions:          {n_holds}")

        if not self.demand.empty:
            total_unmet = self.demand["unmet_demand"].sum()
            stockout_ticks = (self.demand.groupby(["tick", "item_id"])["unmet_demand"].sum() > 0).sum()
            print(f"  Total unmet demand:      {total_unmet} units")
            print(f"  Stockout (item×tick):    {stockout_ticks}")

        print(f"\n  Per-item breakdown:")
        for item_id in items:
            cum = self.cost_cumulative[self.cost_cumulative["item_id"] == item_id]
            dem = self.demand[self.demand["item_id"] == item_id]
            dec = self.decisions[self.decisions["item_id"] == item_id]

            item_cost   = cum["cumulative_total_cost"].max() if not cum.empty else 0.0
            item_unmet  = dem["unmet_demand"].sum() if not dem.empty else 0
            item_orders = (dec["decision"] == "reorder").sum() if not dec.empty else 0

            print(f"    {item_id:<12}  cost=£{item_cost:>10,.2f}  "
                  f"unmet={item_unmet:>5}  reorders={item_orders}")

        print(f"{'='*55}\n")