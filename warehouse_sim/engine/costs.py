'''
warehouse_sim/engine/costs.py

Sub-step 5 of the tick sequence (see reference below):
- Accumulate costs for the warehouse across items
- Write to the tables "ops_cost_accumulator" and "hist_cost_by_tick"

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

Cost components per tick (spec section 3.7, __docs__/simulationSpecs.md):
- holding      = `stock_on_hand` (end of tick) × `holding_cost_per_unit_per_tick`
- stockout     = `unmet_demand` × `stockout_cost_per_unit_per_tick`
- order        = `order_fixed_cost` + (`order_qty` × `order_variable_cost_per_unit`) \n
                   NOTE: Charged at order placement; 0 if no order placed this tick
- transit_loss = `lost_qty` × `transit_loss_cost_per_unit` \n
                   NOTE: Charged at order arrival; 0 if no transit loss this tick

---

NOTE: No agent dependency.
'''

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

from ..config.models import ItemType

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Catalog / tables
# ---------------------------------------------------------------------------

CATALOG         = "hackathon_of_the_century"
_ACCUM_TABLE    = f"{CATALOG}.tables4ops.ops_cost_accumulator"
_HIST_TABLE     = f"{CATALOG}.tables4hist.hist_cost_by_tick"

_ACCUM_SCHEMA = '''
    sim_id                      STRING,
    tick                        INT,
    item_id                     STRING,
    cumulative_holding_cost     DOUBLE,
    cumulative_stockout_cost    DOUBLE,
    cumulative_order_cost       DOUBLE,
    cumulative_transit_loss_cost DOUBLE,
    cumulative_total_cost       DOUBLE,
    remaining_budget            DOUBLE
'''

_HIST_SCHEMA = '''
    sim_id             STRING,
    tick               INT,
    item_id            STRING,
    holding_cost       DOUBLE,
    stockout_cost      DOUBLE,
    order_cost         DOUBLE,
    transit_loss_cost  DOUBLE,
    total_cost         DOUBLE
'''


# ---------------------------------------------------------------------------
# Cost state dataclass (in-memory, accumulated across ticks)
# ---------------------------------------------------------------------------

@dataclass
class CostState:
    '''
    Mutable in-memory cumulative cost totals for one item.
    
    NOTE: The runner updates these each tick and writes a snapshot.
    '''

    item_id:                      str
    cumulative_holding_cost:      float = 0.0
    cumulative_stockout_cost:     float = 0.0
    cumulative_order_cost:        float = 0.0
    cumulative_transit_loss_cost: float = 0.0

    @property
    def cumulative_total(self) -> float:
        return (
            self.cumulative_holding_cost
            + self.cumulative_stockout_cost
            + self.cumulative_order_cost
            + self.cumulative_transit_loss_cost
        )

# ---------------------------------------------------------------------------
# Per-component cost calculators (pure Python - no Spark)
# ---------------------------------------------------------------------------

def compute_holding_cost(stock_on_hand: int, item: ItemType) -> float:
    '''
    Holding cost for the specified item type on end-of-tick stock (post-arrival, post-demand).
    
    `holding_cost` = `stock_on_hand` (end of tick) × `holding_cost_per_unit_per_tick` (for the specified item)
    
    ---

    PARAMETERS:
    - `stock_in_hand` (int): The quantity of the specified item type present (in hand) in the stock at the end of the simulation tick
    - `item` (ItemType): ItemType instance encapsulating the specifications for a specific item type

    RETURNS:
    - (float): Holding cost for the item type
    '''

    return stock_on_hand * item.holding_cost_per_unit_per_tick


def compute_stockout_cost(unmet_demand: int, item: ItemType) -> float:
    '''
    Penalty per unit of unmet demand.
    
    `stockout_cost` = `unmet_demand` × `stockout_cost_per_unit_per_tick`

    ---

    PARAMETERS:
    - `unmet_demand` (int): Total Demand - Fulfilled Demand (for the specified item type)
    - `item` (ItemType): ItemType instance encapsulating the specifications for a specific item type
    
    RETURNS:
    - (float): Stockout cost for the item type
    '''

    return unmet_demand * item.stockout_cost_per_unit_per_tick


def compute_order_cost(order_qty: int, item: ItemType) -> float:
    '''
    Fixed + variable cost at placement. 0 if no order placed (order_qty=0).
    
    `order_cost` = `order_fixed_cost` + (`order_qty` × `order_variable_cost_per_unit`)
    
    NOTE: This is charged at order placement; 0 if no order placed this tick.
    
    ---

    PARAMETERS:
    - `order_qty` (int): Number of items ordered for the specified item type
    - `item` (ItemType): ItemType instance encapsulating the specifications for a specific item type
    
    RETURNS:
    - (float): Order cost (i.e. cost of placing the order) for the item type
    '''

    if order_qty == 0:
        return 0.0
    return item.order_fixed_cost + (order_qty * item.order_variable_cost_per_unit)


def compute_transit_loss_cost(lost_qty: int, item: ItemType) -> float:
    '''
    Cost per unit lost in transit. 0 if no transit loss.

    `transit_loss` = `lost_qty` × `transit_loss_cost_per_unit`
    
    NOTE: This is charged at order arrival; 0 if no transit loss this tick.

    ---

    PARAMETERS:
    - `order_qty` (int): Number of items ordered for the specified item type
    - `item` (ItemType): ItemType instance encapsulating the specifications for a specific item type
    
    RETURNS:
    - (float): Transit loss cost for the item type
    '''

    return lost_qty * item.transit_loss_cost_per_unit


def accumulate(
    cost_state:        CostState,
    holding_cost:      float,
    stockout_cost:     float,
    order_cost:        float,
    transit_loss_cost: float,
) -> tuple[float, float, float, float]:
    '''
    Add this tick's costs to the running totals.

    What it does:
    - Mutates `cost_state` in place
    - Returns the four per-tick costs (which is useful for the table :hist_cost_by_tick")

    ---

    PARAMETERS:
    - `cost_state` (CostState): CostState instance encapsulating mutable in-memory cumulative cost totals for a specific item item
    - `holding_cost` (float): Holding cost for this tick (see the docstring of `compute_holding_cost`)
    - `stockout_cost` (float): Stockout cost for this tick (see the docstring of `compute_stockout_cost`)
    - `order_cost` (float): Order cost for this tick (see the docstring of `compute_order_cost`)
    - `transit_loss_cost` (float): Holding cost for this tick (see the docstring of `compute_transit_loss_cost`)

    RETURNS:
    - (float): Holding cost for this tick
    - (float): Stockout cost for this tick
    - (float): Order cost for this tick
    - (float): Transit cost for this tick
    '''

    cost_state.cumulative_holding_cost      += holding_cost
    cost_state.cumulative_stockout_cost     += stockout_cost
    cost_state.cumulative_order_cost        += order_cost
    cost_state.cumulative_transit_loss_cost += transit_loss_cost
    return holding_cost, stockout_cost, order_cost, transit_loss_cost


def check_budget(
    remaining_budget:  Optional[float],
    order_cost:        float,
) -> bool:
    '''
    Return True if the order is affordable given the remaining budget.
    Always True when remaining_budget is None (unlimited).
    '''
    if remaining_budget is None:
        return True
    return order_cost <= remaining_budget


def deduct_budget(
    remaining_budget: Optional[float],
    cost:             float,
) -> Optional[float]:
    '''
    Deduct cost from remaining_budget; return None if unlimited.
    
    ---

    PARAMETERS:
    - `remaining_budget` (float, optional): Remaining budget for the simulation run (before cost deduction)
    - `cost` (float): Cost (can be any cost component, per tick or cumulative; this function is agnostic to such details)
    
    RETURNS:
    - (float, optional): Remaining budget for the simulation run (after cost deduction); 0 if the remaining budget (after cost deduction) is negative
    '''

    if remaining_budget is None:
        return None
    return max(0.0, remaining_budget - cost)


# ---------------------------------------------------------------------------
# Spark writes (lazy imports)
# ---------------------------------------------------------------------------

def write_cost_accumulator(
    spark:            "SparkSession",
    sim_id:           str,
    tick:             int,
    cost_states:      dict[str, CostState],
    remaining_budget: Optional[float],
) -> None:
    '''
    Append cumulative cost row per item for this tick to the table "ops_cost_accumulator".
    
    ---

    PARAMETERS:
    - `spark` (SparkSession): SparkSession instance handling Spark operations
    - `sim_id` (str): Simulation ID
    - `tick` (int): Simulation tick number
    - `cost_states` (dict[str, CostState]): Dictionary linking item type (identified by item IDs) to their respective CostState instances
    - `remaining_budget` (float, optional): 

    Returns:
    - None
    '''
    
    rows = [
        {
            "sim_id":                       sim_id,
            "tick":                         tick,
            "item_id":                      cs.item_id,
            "cumulative_holding_cost":      cs.cumulative_holding_cost,
            "cumulative_stockout_cost":     cs.cumulative_stockout_cost,
            "cumulative_order_cost":        cs.cumulative_order_cost,
            "cumulative_transit_loss_cost": cs.cumulative_transit_loss_cost,
            "cumulative_total_cost":        cs.cumulative_total,
            "remaining_budget":             remaining_budget,
        }
        for cs in cost_states.values()
    ]
    spark.createDataFrame(rows, schema=_ACCUM_SCHEMA.strip()) \
        .write.mode("append").saveAsTable(_ACCUM_TABLE)


def write_cost_by_tick(
    spark:       "SparkSession",
    sim_id:      str,
    tick:        int,
    tick_costs:  dict[str, dict],   # item_id -> {holding, stockout, order, transit_loss}
) -> None:
    '''
    Append per-tick cost breakdown per item to the table "hist_cost_by_tick".
    
    ---

    PARAMETERS:
    - `spark` (SparkSession): SparkSession instance handling Spark operations
    - `sim_id` (str): Simulation ID
    - `tick` (int): Simulation tick number
    - `tick_costs` (dict[str, dict]): Dictionary linking item type (identified by item IDs) to a dictionary containing the cost components for this tick \n
      NOTE: The dictionary of cost components links cost component names to their values for this tick, e.g.: `"stockout_cost": 4`

    Returns:
    - None
    '''

    rows = [
        {
            "sim_id":            sim_id,
            "tick":              tick,
            "item_id":           item_id,
            "holding_cost":      tc["holding"],
            "stockout_cost":     tc["stockout"],
            "order_cost":        tc["order"],
            "transit_loss_cost": tc["transit_loss"],
            "total_cost":        tc["holding"] + tc["stockout"] + tc["order"] + tc["transit_loss"],
        }
        for item_id, tc in tick_costs.items()
    ]
    spark.createDataFrame(rows, schema=_HIST_SCHEMA.strip()) \
        .write.mode("append").saveAsTable(_HIST_TABLE)
