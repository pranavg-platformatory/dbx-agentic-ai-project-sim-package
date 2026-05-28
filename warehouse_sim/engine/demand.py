'''
warehouse_sim/engine/demand.py

Sub-steps 2 and 3b of the tick sequence (see reference below):
- 2  - draw demand from pattern, apply disruption multiplier
- 3b - deplete stock by fulfilled demand (floored at 0)

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

Returns a DemandResult per item which the runner uses to:
- Update ops_warehouse_state (via state.py)
- Write hist_demand_actuals
- Fire `DEMAND_DRAWN` and `STOCKOUT_OCCURRED` events

---

NOTE: No agent dependency.
'''

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..config.models import Pattern
from ..world.patterns import PatternSampler
from .disruptions import DisruptionActivation, get_demand_multiplier

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


#################################################
# Catalog / table
#################################################

CATALOG = "hackathon_of_the_century"
_TABLE  = f"{CATALOG}.tables4hist.hist_demand_actuals"

_SCHEMA = '''
    sim_id           STRING,
    tick             INT,
    item_id          STRING,
    consumer_id      STRING,
    raw_demand       DOUBLE,
    disrupted_demand DOUBLE,
    fulfilled_demand INT,
    unmet_demand     INT,
    pattern_id       STRING
'''


#################################################
# Result dataclass
#################################################

@dataclass(frozen=True)
class DemandResult:
    '''The outcome of drawing and fulfilling demand for one item in one tick.'''

    item_id:          str
    consumer_id:      str
    pattern_id:       str
    raw_demand:       float   # float sample before disruption
    disrupted_demand: float   # float after disruption multiplier
    fulfilled:        int     # min(floor(disrupted_demand), stock_on_hand)
    unmet:            int     # floor(disrupted_demand) - fulfilled
    stock_after:      int     # stock_on_hand after depletion (>= 0)


#################################################
# Core logic (pure Python - no Spark)
#################################################

def draw_demand(
    tick:           int,
    item_id:        str,
    pattern:        Pattern,
    stock_on_hand:  int,            # stock AFTER arrivals (sub-step 3a)
    activations:    list[DisruptionActivation],
    sampler:        PatternSampler,
) -> DemandResult:
    '''
    Draw demand for one item at one tick and compute stock depletion.

    Pipeline (spec section 3.5 + suggestions, __docs__/simulationSpecs.md):
    1. Sample raw float from pattern
    2. Apply demand disruption multiplier (multiply demand_spike / suppression)
    3. floor() to int, clamp to >= 0
    4. fulfilled = min(int_demand, stock_on_hand)
    5. unmet     = int_demand - fulfilled
    6. stock_after = stock_on_hand - fulfilled  (>= 0 guaranteed)

    ---
    PARAMETERS:
    - `tick` (int): Simulation tick number
    - `item_id` (str): Item ID (which corresponds to a specific item type)
    - `pattern` (Pattern): Pattern instance encapsulating the specifications for a demand pattern
    - `stock_on_hand` (int): Stock in hand for the specified item ID (which corresponds to a specific item type)
    - `activations` (list[DisruptionActivation]): List of demand disruptions active in this tick
    - `sampler` (PatternSampler): Stateful sampler that wraps a seeded numpy RNG to sample from the specified demand pattern
    
    RETURNS:
    - (DemandResult): DemandResult result encapsulating the state of key system parameters resulting from the demand in this tick
    '''

    # Sample raw demand as raw float before flooring for the record
    raw_float  = _raw_float_sample(pattern, tick, sampler)
    # NOTE: To get the true pre-floor float we replicate the base value + seasonal without the final floor - use a private call path via `_raw_float_sample`.

    # Apply demand multiplier
    multiplier = get_demand_multiplier(item_id, activations)

    # Disrupt raw demand
    disrupted_float = raw_float * multiplier
    int_demand      = max(0, math.floor(disrupted_float))

    # Compute resulting values
    fulfilled  = min(int_demand, stock_on_hand)
    unmet      = int_demand - fulfilled
    stock_after = stock_on_hand - fulfilled   # guaranteed >= 0

    # Return 
    return DemandResult(
        item_id          = item_id,
        consumer_id      = pattern.sim_id,    # resolved by runner from consumer_item_map
        pattern_id       = pattern.pattern_id,
        raw_demand       = raw_float,
        disrupted_demand = disrupted_float,
        fulfilled        = fulfilled,
        unmet            = unmet,
        stock_after      = stock_after,
    )


def _raw_float_sample(pattern: Pattern, tick: int, sampler: PatternSampler) -> float:
    '''
    - Return the raw float value (base + seasonal + noise) before flooring
    - This is used to populate the `raw_demand` and `disrupted_demand` fields in the table "hist_demand_actuals"
      (with the pre-floor float, matching the spec's column type; see spec section 6, __docs__/simulationSpecs.md)

    NOTE:
    - This draws from the RNG, so call order matters for reproducibility
    - The runner always calls `draw_demand` (not `_raw_float_sample` directly) - `_raw_float_sample` is an internal helper only.
    
    ---

    PARAMETERS:
    - `pattern` (Pattern): Pattern instance encapsulating the specifications for a demand pattern
    - `tick` (int): Simulation tick number
    - `sampler` (PatternSampler): Stateful sampler that wraps a seeded numpy RNG to sample from the specified demand pattern
    
    RETURNS:
    - (float): Pattern-derived/pattern-sampled float value (which serves as the raw demand)
    '''

    from ..config.models import PatternType

    # Base value
    if pattern.pattern_type == PatternType.CUSTOM:
        schedule = pattern.custom_schedule
        base     = float(schedule[tick % len(schedule)])
    else:
        #n Sampler's internal statistical draw
        base = float(sampler.sample(pattern, tick))

    # Seasonal overlay (deterministic, no RNG)
    if pattern.seasonal_multiplier_schedule:
        base *= pattern.seasonal_multiplier_schedule[tick % len(pattern.seasonal_multiplier_schedule)]

    return max(0.0, base)


#################################################
# Spark write (lazy import)
#################################################

def write_demand_actuals(
    spark:   "SparkSession",
    sim_id:  str,
    tick:    int,
    results: list[DemandResult],
    consumer_map: dict[str, str],   # item_id -> consumer_id
) -> None:
    '''
    Append demand actuals for this tick to the table "hist_demand_actuals".
    
    ---

    PARAMETERS:
    - `spark` (SparkSession): SparkSession instance handling Spark operations
    - `sim_id` (str): Simulation ID
    - `tick` (int): Simulation tick number
    - `results` (list[DemandResult]): List of DemandResult instances, which encapsulate demand results for this tick (each being for a particular consumer ID and item ID (corresponding to a specific item type) combination)
    - `consumer_map` (dict[str, str]): Dictionary mapping item IDs (corresponding to specific item types) to consumer IDs who are consuming these items for this tick

    Returns:
    - None
    '''

    if not results:
        return

    rows = [
        {
            "sim_id":           sim_id,
            "tick":             tick,
            "item_id":          r.item_id,
            "consumer_id":      consumer_map.get(r.item_id, "unknown"),
            "raw_demand":       r.raw_demand,
            "disrupted_demand": r.disrupted_demand,
            "fulfilled_demand": r.fulfilled,
            "unmet_demand":     r.unmet,
            "pattern_id":       r.pattern_id,
        }
        for r in results
    ]
    spark.createDataFrame(rows, schema=_SCHEMA.strip()) \
        .write.mode("append").saveAsTable(_TABLE)
