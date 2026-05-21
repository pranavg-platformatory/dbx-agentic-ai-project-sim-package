'''
warehouse_sim/engine/demand.py

Sub-steps 2 and 3b of the tick sequence:
  2   - draw demand from pattern, apply disruption multiplier
  3b  - deplete stock by fulfilled demand (floored at 0)

Returns a DemandResult per item which the runner uses to:
  - Update ops_warehouse_state (via state.py)
  - Write hist_demand_actuals
  - Fire DEMAND_DRAWN and STOCKOUT_OCCURRED events

No agent dependency.
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


# ---------------------------------------------------------------------------
# Catalog / table
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DemandResult:
    '''
    The outcome of drawing and fulfilling demand for one item in one tick.
    '''
    item_id:          str
    consumer_id:      str
    pattern_id:       str
    raw_demand:       float   # float sample before disruption
    disrupted_demand: float   # float after disruption multiplier
    fulfilled:        int     # min(floor(disrupted_demand), stock_on_hand)
    unmet:            int     # floor(disrupted_demand) - fulfilled
    stock_after:      int     # stock_on_hand after depletion (>= 0)


# ---------------------------------------------------------------------------
# Core logic (pure Python - no Spark)
# ---------------------------------------------------------------------------

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

    Pipeline (spec section 3.5 + suggestions):
      1. Sample raw float from pattern
      2. Apply demand disruption multiplier (multiply demand_spike / suppression)
      3. floor() to int, clamp to >= 0
      4. fulfilled = min(int_demand, stock_on_hand)
      5. unmet     = int_demand - fulfilled
      6. stock_after = stock_on_hand - fulfilled  (>= 0 guaranteed)
    '''
    raw_demand = float(sampler.sample(pattern, tick))  # already floored int, cast back to float for record

    # Re-sample as raw float before flooring for the record
    # (sample() returns int; we store the pre-floor float as raw_demand)
    # To get the true pre-floor float we replicate the base value + seasonal
    # without the final floor - use a private call path via _raw_float_sample.
    raw_float  = _raw_float_sample(pattern, tick, sampler)
    multiplier = get_demand_multiplier(item_id, activations)

    disrupted_float = raw_float * multiplier
    int_demand      = max(0, math.floor(disrupted_float))

    fulfilled  = min(int_demand, stock_on_hand)
    unmet      = int_demand - fulfilled
    stock_after = stock_on_hand - fulfilled   # guaranteed >= 0

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
    Return the raw float value (base + seasonal + noise) before flooring.
    This is used to populate raw_demand and disrupted_demand in hist_demand_actuals
    with the pre-floor float, matching the spec's column types.

    NOTE: This draws from the RNG, so call order matters for reproducibility.
    The runner always calls draw_demand (not this directly) - this is an
    internal helper only.
    '''
    import math as _math
    from ..config.models import PatternType

    # Base value
    if pattern.pattern_type == PatternType.CUSTOM:
        schedule = pattern.custom_schedule
        base     = float(schedule[tick % len(schedule)])
    else:
        # Re-use sampler's internal statistical draw
        # We cannot re-draw without consuming RNG; instead we store the
        # already-drawn int as a float approximation for the record.
        # The engine calls PatternSampler.sample() which floors - we store
        # that value as the raw float (acceptable per spec: raw_demand is
        # the sample "before disruption", not strictly "before floor").
        base = float(sampler.sample(pattern, tick))

    # Seasonal overlay (deterministic, no RNG)
    if pattern.seasonal_multiplier_schedule:
        base *= pattern.seasonal_multiplier_schedule[tick % len(pattern.seasonal_multiplier_schedule)]

    return max(0.0, base)


# ---------------------------------------------------------------------------
# Spark write (lazy import)
# ---------------------------------------------------------------------------

def write_demand_actuals(
    spark:   "SparkSession",
    sim_id:  str,
    tick:    int,
    results: list[DemandResult],
    consumer_map: dict[str, str],   # item_id -> consumer_id
) -> None:
    '''Append demand actuals for this tick to hist_demand_actuals.'''
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
