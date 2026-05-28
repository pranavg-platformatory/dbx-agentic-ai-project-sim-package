'''
warehouse_sim/engine/state.py

Warehouse state reads and writes for the table "ops_warehouse_state".

Responsibilities:
- Initialise tick-0 state from the field `initial_stock` in "env_item_types"
- Read current stock for all items (MAX(tick) per item)
- Write one row per item per tick (append-only)

For reference, the simulation loop has the following tick sequence, i.e. steps per tick:

```
SIMULATION LOOP (per tick)
│
├── [0] Evaluate stochastic disruptions → ops_active_disruptions
├── [1] Process supply arrivals         → ops_pending_orders (update), ops_warehouse_state
├── [2] Draw demand                     → hist_demand_actuals
├── [3a] Apply arrivals to stock      ┐
├── [3b] Apply demand to stock        ┘ → ops_warehouse_state
├── [4] Agent decides                   → hist_reorder_decisions, ops_pending_orders (insert)
├── [5] Accumulate costs                → ops_cost_accumulator, hist_cost_by_tick
└── [6] Write event log                 → event_log
    The engine builds this once per tick and passes it to agent.decide().
    The agent must not mutate it.
```

---

KEY POINTS:
- The stock value written reflects both sub-step 3a (arrivals) and 3b (demand depletion)
- This is what the agent sees in sub-step 4

---

NOTE: No agent dependency.
'''

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from ..config.models import SimWorld

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

#################################################
# Catalog / table
#################################################

CATALOG = "hackathon_of_the_century"
_TABLE  = f"{CATALOG}.tables4ops.ops_warehouse_state"

_SCHEMA = '''
    sim_id                      STRING,
    tick                        INT,
    item_id                     STRING,
    stock_on_hand               INT,
    stock_in_transit            INT,
    expected_arrivals_next_tick INT,
    updated_at                  TIMESTAMP
'''

#################################################
# Stock state dataclass (in-memory per tick)
#################################################

@dataclass
class StockState:
    '''
    Mutable in-memory stock state for one item within a tick.
    
    The runner (warehouse_sim/engine/runner.py):
    - Updates these fields as each sub-step completes
    - Then writes a snapshot to ops_warehouse_state at the end of sub-step 3b
    
    NOTE: See module docstring for the reference for the simulation loop steps and sub-steps.
    '''

    item_id:                     str
    stock_on_hand:               int   # updated by arrivals (3a) and demand (3b)
    stock_in_transit:            int   # updated when orders placed/arrive
    expected_arrivals_next_tick: int   # units due to arrive at tick+1

#################################################
# Core logic (pure Python - no Spark)
#################################################

def apply_arrivals(state: StockState, arrived_qty: int) -> None:
    '''
    Sub-step 3a: increase stock by arrived supply. Mutates state in place.

    NOTE: See module docstring for the reference for the simulation loop steps and sub-steps.
    
    ---

    PARAMETERS:
    - `state` (StockState): Mutable in-memory stock state for one item within this tick
    - `arrived_qty` (int): Quantity of the item type (as specified in `state`) that has arrived in this tick
    
    RETURNS:
    - None
    '''

    state.stock_on_hand    += arrived_qty
    state.stock_in_transit  = max(0, state.stock_in_transit - arrived_qty)

def apply_demand(state: StockState, fulfilled: int) -> None:
    '''
    Sub-step 3b: decrease stock by fulfilled demand. Mutates state in place.
    
    IMPORTANT: Floored at 0 - stock never goes negative.
    
    NOTE: See module docstring for the reference for the simulation loop steps and sub-steps.
    
    ---

    PARAMETERS:
    - `state` (StockState): Mutable in-memory stock state for one item within this tick
    - `fulfilled` (int): Quantity of the fulfilled demand for the item type (as specified in `state`) that has arrived in this tick
    
    RETURNS:
    - None
    '''

    state.stock_on_hand = max(0, state.stock_on_hand - fulfilled)

def apply_new_order(state: StockState, order_qty: int) -> None:
    '''
    Update in-transit count when a new order is placed (sub-step 4). Mutates state in place.
    
    NOTE: See module docstring for the reference for the simulation loop steps and sub-steps.

    ---

    PARAMETERS:
    - `state` (StockState): Mutable in-memory stock state for one item within this tick
    - `order_qty` (int): Quantity of the item type (as specified in `state`) that has been ordered in this tick
    
    RETURNS:
    - None
    '''

    state.stock_in_transit += order_qty

#################################################
# Spark reads and writes
#################################################

def initialise_states(world: SimWorld) -> dict[str, StockState]:
    '''
    Build the tick-0 stock state for all items from the field `initial_stock` in the table "env_item_types".
    
    NOTE:
    - Called once by the runner before the loop starts
    - Does NOT write to the table "ops_warehouse_state" - the runner does that at tick 0
    
    ---

    PARAMETERS:
    - `world` (SimWorld): SimWorld instance encapsulating the configuration for the simulation
    
    RETURNS:
    - (dict[str, StockState]): Dictionary linking item IDs (which correspond to specific item types) to StockState instances (which are mutable in-memory stock states for specific item IDs within this tick)
    '''

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
    '''
    Append one row per item for this tick to the table "ops_warehouse_state".
    
    NOTE: Called at the end of sub-step 3b each tick.

    NOTE: See module docstring for the reference for the simulation loop steps and sub-steps.
    
    ---

    PARAMETERS:
    - `spark` (SparkSession): SparkSession instance handling Spark operations
    - `sim_id` (str): Simulation ID
    - `tick` (int): Simulation tick number
    - `states` (dict[str, StockState]): Dictionary linking item IDs (which correspond to specific item types) to StockState instances (which are mutable in-memory stock states for specific item IDs within this tick)

    Returns:
    - None
    '''
    
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
    '''
    Read the latest stock state for all items (MAX(tick) per item).

    NOTE:
    - Used by the runner (warehouse_sim/engine/runner.py) to reconstruct in-memory state after a restart
    - Not called during a normal continuous run (state is kept in memory)
    
    ---

    PARAMETERS:
    - `spark` (SparkSession): SparkSession instance handling Spark operations
    - `sim_id` (str): Simulation ID

    RETURNS:
    - (dict[str, StockState]): Dictionary linking item IDs (which correspond to specific item types) to the latest StockState instances (which are mutable in-memory stock states for specific item IDs within the latest tick)
    '''

    rows = spark.sql(f'''
        SELECT ws.*
        FROM {_TABLE} ws
        INNER JOIN (
            SELECT item_id, MAX(tick) AS max_tick
            FROM {_TABLE}
            WHERE sim_id = '{sim_id}'
            GROUP BY item_id
        ) latest ON ws.item_id = latest.item_id AND ws.tick = latest.max_tick
        WHERE ws.sim_id = '{sim_id}'
    ''').collect()

    return {
        row["item_id"]: StockState(
            item_id                     = row["item_id"],
            stock_on_hand               = row["stock_on_hand"],
            stock_in_transit            = row["stock_in_transit"],
            expected_arrivals_next_tick = row["expected_arrivals_next_tick"],
        )
        for row in rows
    }
