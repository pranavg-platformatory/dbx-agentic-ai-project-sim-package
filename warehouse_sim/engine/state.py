"""
warehouse_sim/engine/state.py

Warehouse state reads and writes for ops_warehouse_state.

Responsibilities:
  - Initialise tick-0 state from env_item_types.initial_stock
  - Read current stock for all items (MAX(tick) per item)
  - Write one row per item per tick (append-only)

The stock value written reflects both sub-step 3a (arrivals) and 3b
(demand depletion) — this is what the agent sees in sub-step 4.

No agent dependency.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Catalog / table
# ---------------------------------------------------------------------------

CATALOG = "hackathon_of_the_century"
_TABLE  = f"{CATALOG}.tables4ops.ops_warehouse_state"

_SCHEMA = """
    sim_id                      STRING,
    tick                        INT,
    item_id                     STRING,
    stock_on_hand               INT,
    stock_in_transit            INT,
    expected_arrivals_next_tick INT,
    updated_at                  TIMESTAMP
"""


# ---------------------------------------------------------------------------
# Stock state dataclass (in-memory per tick)
# ---------------------------------------------------------------------------

@dataclass
class StockState:
    """
    Mutable in-memory stock state for one item within a tick.
    The runner updates these fields as each sub-step completes,
    then writes a snapshot to ops_warehouse_state at the end of sub-step 3b.
    """
    item_id:                     str
    stock_on_hand:               int   # updated by arrivals (3a) and demand (3b)
    stock_in_transit:            int   # updated when orders placed/arrive
    expected_arrivals_next_tick: int   # units due to arrive at tick+1


# ---------------------------------------------------------------------------
# Core logic (pure Python — no Spark)
# ---------------------------------------------------------------------------

def apply_arrivals(state: StockState, arrived_qty: int) -> None:
    """
    Sub-step 3a: increase stock by arrived supply.
    Mutates state in place.
    """
    state.stock_on_hand    += arrived_qty
    state.stock_in_transit  = max(0, state.stock_in_transit - arrived_qty)


def apply_demand(state: StockState, fulfilled: int) -> None:
    """
    Sub-step 3b: decrease stock by fulfilled demand.
    Floored at 0 — stock never goes negative.
    Mutates state in place.
    """
    state.stock_on_hand = max(0, state.stock_on_hand - fulfilled)


def apply_new_order(state: StockState, order_qty: int) -> None:
    """
    Update in-transit count when a new order is placed (sub-step 4).
    Mutates state in place.
    """
    state.stock_in_transit += order_qty


# ---------------------------------------------------------------------------
# Spark reads and writes
# ---------------------------------------------------------------------------

def initialise_states(
    spark:  "SparkSession",
    sim_id: str,
    world,                     # SimWorld — avoids circular import
) -> dict[str, StockState]:
    """
    Build the tick-0 stock state for all items from env_item_types.initial_stock.
    Called once by the runner before the loop starts.
    Does NOT write to ops_warehouse_state — the runner does that at tick 0.
    """
    states: dict[str, StockState] = {}
    for item_id, item in world.items.items():
        states[item_id] = StockState(
            item_id                     = item_id,
            stock_on_hand               = item.initial_stock,
            stock_in_transit            = 0,
            expected_arrivals_next_tick = 0,
        )
    return states


def write_warehouse_state(
    spark:   "SparkSession",
    sim_id:  str,
    tick:    int,
    states:  dict[str, StockState],
) -> None:
    """
    Append one row per item for this tick to ops_warehouse_state.
    Called at the end of sub-step 3b each tick.
    """
    now = datetime.now(timezone.utc)
    rows = [
        {
            "sim_id":                      sim_id,
            "tick":                        tick,
            "item_id":                     s.item_id,
            "stock_on_hand":               s.stock_on_hand,
            "stock_in_transit":            s.stock_in_transit,
            "expected_arrivals_next_tick": s.expected_arrivals_next_tick,
            "updated_at":                  now,
        }
        for s in states.values()
    ]
    spark.createDataFrame(rows, schema=_SCHEMA.strip()) \
        .write.mode("append").saveAsTable(_TABLE)


def fetch_current_states(
    spark:  "SparkSession",
    sim_id: str,
) -> dict[str, StockState]:
    """
    Read the latest stock state for all items (MAX(tick) per item).
    Used by the runner to reconstruct in-memory state after a restart.
    Not called during a normal continuous run (state is kept in memory).
    """
    rows = spark.sql(f"""
        SELECT ws.*
        FROM {_TABLE} ws
        INNER JOIN (
            SELECT item_id, MAX(tick) AS max_tick
            FROM {_TABLE}
            WHERE sim_id = '{sim_id}'
            GROUP BY item_id
        ) latest ON ws.item_id = latest.item_id AND ws.tick = latest.max_tick
        WHERE ws.sim_id = '{sim_id}'
    """).collect()

    return {
        row["item_id"]: StockState(
            item_id                     = row["item_id"],
            stock_on_hand               = row["stock_on_hand"],
            stock_in_transit            = row["stock_in_transit"],
            expected_arrivals_next_tick = row["expected_arrivals_next_tick"],
        )
        for row in rows
    }
