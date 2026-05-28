'''
warehouse_sim/engine/supply.py

Sub-step 1 of the tick sequence (see reference below): process pending order arrivals.

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
- For each order whose `expected_arrival_tick` == current tick:
    - Apply transit loss fraction (if any active transit_loss disruption)
    - Compute `arrived_qty` and `lost_qty`
    - Update table "ops_pending_orders" status
    - Write to table "hist_supply_arrivals"
- Also provides `place_order()` - called at sub-step 4 by the runner when the agent decides to reorder
- Computes effective lead time (with disruption multiplier and floor), inserts into "ops_pending_orders"

---

NOTE: No agent dependency.
'''

from __future__ import annotations

import math
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..world.patterns import PatternSampler
from .disruptions import DisruptionActivation, get_lead_time_multiplier, get_transit_loss_fraction

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

#################################################
# Catalog / tables
#################################################

CATALOG = "hackathon_of_the_century"

_PENDING_TABLE  = f"{CATALOG}.tables4ops.ops_pending_orders"
_ARRIVALS_TABLE = f"{CATALOG}.tables4hist.hist_supply_arrivals"

_PENDING_SCHEMA = '''
    order_id                  STRING,
    sim_id                    STRING,
    item_id                   STRING,
    supplier_id               STRING,
    order_tick                INT,
    expected_arrival_tick     INT,
    order_qty                 INT,
    status                    STRING,
    disruptions_active_at_order ARRAY<STRING>
'''

_ARRIVALS_SCHEMA = '''
    sim_id                STRING,
    tick                  INT,
    order_id              STRING,
    item_id               STRING,
    supplier_id           STRING,
    ordered_qty           INT,
    arrived_qty           INT,
    lost_qty              INT,
    actual_lead_time_ticks INT
'''

#################################################
# Result dataclasses
#################################################

@dataclass(frozen=True)
class ArrivalResult:
    '''The outcome of processing one arriving order.'''

    order_id:    str
    item_id:     str
    supplier_id: str
    order_tick:  int
    ordered_qty: int
    arrived_qty: int
    lost_qty:    int
    status:      str   # "arrived", "partially_lost", "fully_lost"
    actual_lead_time_ticks: int

@dataclass(frozen=True)
class PlacedOrder:
    '''A newly placed reorder, returned by `place_order()`.'''

    order_id:              str
    item_id:               str
    supplier_id:           str
    order_tick:            int
    expected_arrival_tick: int
    order_qty:             int
    disruptions_active:    list[str]   # disruption_ids active at placement

#################################################
# Core logic (pure Python - no Spark)
#################################################

def process_arrivals(
    tick:           int,
    pending_orders: list[dict],        # raw dicts from ops_pending_orders
    activations:    list[DisruptionActivation],
) -> list[ArrivalResult]:
    '''
    Identify orders arriving this tick and apply transit loss.

    ---

    PARAMETERS:
    - `tick` (int): Current simulation tick
    - `pending_orders` (list[dict]): List of pending order dicts (status == "pending")
    - `activations` (list[DisruptionActivation]): Disruption activations for this tick (from sub-step 0)
    
    NOTE: See module docstring for the reference for the simulation loop steps and sub-steps.

    RETURNS:
    - (list[ArrivalResult]): List of ArrivalResult instances for orders arriving this tick
    '''

    results: list[ArrivalResult] = []

    for order in pending_orders:
        if order["expected_arrival_tick"] != tick:
            continue
        if order["status"] != "pending":
            continue

        item_id     = order["item_id"]
        ordered_qty = order["order_qty"]
        loss_frac   = get_transit_loss_fraction(item_id, activations)

        lost_qty    = math.floor(ordered_qty * loss_frac)
        arrived_qty = ordered_qty - lost_qty

        if arrived_qty == 0:
            status = "fully_lost"
        elif lost_qty > 0:
            status = "partially_lost"
        else:
            status = "arrived"

        actual_lead_time = tick - order["order_tick"]

        results.append(ArrivalResult(
            order_id               = order["order_id"],
            item_id                = item_id,
            supplier_id            = order["supplier_id"],
            order_tick             = order["order_tick"],
            ordered_qty            = ordered_qty,
            arrived_qty            = arrived_qty,
            lost_qty               = lost_qty,
            status                 = status,
            actual_lead_time_ticks = actual_lead_time,
        ))

    return results

def place_order(
    tick:                 int,
    item_id:              str,
    supplier_id:          str,
    order_qty:            int,
    base_lead_time_ticks: int,
    lead_time_variability: float,
    activations:          list[DisruptionActivation],
    sampler:              PatternSampler,
) -> PlacedOrder:
    '''
    Compute effective lead time and construct a PlacedOrder.

    Lead time formula (spec section 3.6, __docs__/simulationSpecs.md):
        actual    = `max(1, round(Normal(base_lead_time_ticks, lead_time_variability)))`
        effective = actual × max(1.0, lead_time_multiplier)

    ---

    PARAMETERS:
    - `tick` (int): Simulation tick number
    - `item_id` (str): Item ID (which corresponds to a specific item type)
    - `supplier_id` (str): Supplier ID (from whom items are to be ordered)
    - `base_lead_time_ticks` (int): Base lead time ticks (as configured for the supplier ID in the table "env_suppliers")
    - `lead_time_variability` (float): Variability in the lead time (as configured for the supplier ID in the table "env_suppliers")
    - `activations` (list[DisruptionActivation]): Disruption activations for this tick (from sub-step 0)
    - `sampler` (PatternSampler): Stateful sampler that wraps a seeded numpy RNG to sample from the specified demand pattern

    RETURNS:
    - (PlacedOrder): PlacedOrder instance; the runner (warehouse_sim/engine/runner.py) writes it to the table "ops_pending_orders"
    '''
    actual_lt    = sampler.sample_lead_time(base_lead_time_ticks, lead_time_variability)
    lt_mult      = get_lead_time_multiplier(item_id, activations)
    effective_lt = math.ceil(actual_lt * lt_mult)   # ceil so multiplier never rounds to 0

    active_disruption_ids = [
        a.disruption_id for a in activations
        if a.item_id == item_id and a.is_active_this_tick
    ]

    return PlacedOrder(
        order_id              = str(uuid.uuid4()),
        item_id               = item_id,
        supplier_id           = supplier_id,
        order_tick            = tick,
        expected_arrival_tick = tick + effective_lt,
        order_qty             = order_qty,
        disruptions_active    = active_disruption_ids,
    )

#################################################
# Spark writes (lazy imports)
#################################################

def write_placed_order(
    spark:  "SparkSession",
    sim_id: str,
    order:  PlacedOrder,
) -> None:
    '''
    Insert a new pending order into the table "ops_pending_orders".
    
    ---

    PARAMETERS:
    - `spark` (SparkSession): SparkSession instance handling Spark operations
    - `sim_id` (str): Simulation ID
    - `order` (PlacedOrder): PlacedOrder instance encapsulating the details of the order placed in this time tick
    
    RETURNS:
    - None
    '''

    rows = [{
        "order_id":                   order.order_id,
        "sim_id":                     sim_id,
        "item_id":                    order.item_id,
        "supplier_id":                order.supplier_id,
        "order_tick":                 order.order_tick,
        "expected_arrival_tick":      order.expected_arrival_tick,
        "order_qty":                  order.order_qty,
        "status":                     "pending",
        "disruptions_active_at_order": order.disruptions_active or [],
    }]
    spark.createDataFrame(rows, schema=_PENDING_SCHEMA.strip()) \
        .write.mode("append").saveAsTable(_PENDING_TABLE)

def update_order_status(
    spark:   "SparkSession",
    sim_id:  str,
    results: list[ArrivalResult],
) -> None:
    '''
    Update the table "ops_pending_orders" status for arrived orders.
    
    ---

    PARAMETERS:
    - `spark` (SparkSession): SparkSession instance handling Spark operations
    - `sim_id` (str): Simulation ID
    - `results` (list[ArrivalResult]): List of ArrivalResult instances for orders arriving this tick
    
    RETURNS:
    - None
    '''
    
    for r in results:
        spark.sql(f'''
            UPDATE {_PENDING_TABLE}
            SET status = '{r.status}'
            WHERE sim_id = '{sim_id}'
              AND order_id = '{r.order_id}'
        ''')

def write_arrivals(
    spark:   "SparkSession",
    sim_id:  str,
    tick:    int,
    results: list[ArrivalResult],
) -> None:
    '''
    Append arrival records to the table "hist_supply_arrivals".
    
    ---

    PARAMETERS:
    - `spark` (SparkSession): SparkSession instance handling Spark operations
    - `sim_id` (str): Simulation ID
    - `tick` (int): Simulation tick number
    - `results` (list[ArrivalResult]): List of ArrivalResult instances for orders arriving this tick
    
    RETURNS:
    - None
    '''
    
    if not results:
        return

    rows = [
        {
            "sim_id":                sim_id,
            "tick":                  tick,
            "order_id":              r.order_id,
            "item_id":               r.item_id,
            "supplier_id":           r.supplier_id,
            "ordered_qty":           r.ordered_qty,
            "arrived_qty":           r.arrived_qty,
            "lost_qty":              r.lost_qty,
            "actual_lead_time_ticks": r.actual_lead_time_ticks,
        }
        for r in results
    ]
    spark.createDataFrame(rows, schema=_ARRIVALS_SCHEMA.strip()) \
        .write.mode("append").saveAsTable(_ARRIVALS_TABLE)


def fetch_pending_orders(
    spark:  "SparkSession",
    sim_id: str,
) -> list[dict]:
    '''
    Read all pending (undelivered) orders for this simulation from the table "ops_pending_orders".
    
    ----

    PARAMETERS:
    - `spark` (SparkSession): SparkSession instance handling Spark operations
    - `sim_id` (str): Simulation ID
    
    RETURNS:
    - (list[dict]): List of dictionaries, each dictionary encapsulating a row from the table "ops_pending_orders"
    '''
    
    return [
        row.asDict()
        for row in spark.sql(f'''
            SELECT * FROM {_PENDING_TABLE}
            WHERE sim_id = '{sim_id}'
              AND status = 'pending'
        ''').collect()
    ]
