"""
warehouse_sim/engine/disruptions.py

Sub-step 0 of the tick sequence: evaluate which disruptions are active
this tick and write to ops_active_disruptions.

Deterministic disruptions are always active within their window.
Stochastic disruptions draw from the shared RNG (via PatternSampler.draw_uniform)
in disruption_id alphabetical order — required for reproducibility (FR-07).

Multiple disruptions of the same type on the same item are multiplied
together (spec FR-06 suggestion).

No agent or runner dependency.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..config.models import DisruptionSchedule, DisruptionType
from ..world.patterns import PatternSampler

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Catalog / table
# ---------------------------------------------------------------------------

CATALOG = "hackathon_of_the_century"
_TABLE  = f"{CATALOG}.tables4ops.ops_active_disruptions"

_SCHEMA = """
    sim_id              STRING,
    tick                INT,
    disruption_id       STRING,
    item_id             STRING,
    disruption_type     STRING,
    effective_magnitude DOUBLE,
    is_active_this_tick BOOLEAN
"""


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DisruptionActivation:
    """
    The resolved activation state of one disruption for one tick.
    Returned by evaluate_disruptions; consumed by runner and other sub-modules.
    """
    disruption_id:       str
    item_id:             str
    disruption_type:     DisruptionType
    effective_magnitude: float   # 0.0 if stochastic and did not trigger
    is_active_this_tick: bool


# ---------------------------------------------------------------------------
# Core logic (pure Python — no Spark)
# ---------------------------------------------------------------------------

def evaluate_disruptions(
    tick:        int,
    disruptions: list[DisruptionSchedule],
    sampler:     PatternSampler,
) -> list[DisruptionActivation]:
    """
    Evaluate all disruptions for the given tick.

    Rules (spec section 3.8 + FR-07):
      - Only disruptions whose window includes this tick are considered
      - Stochastic disruptions are evaluated in disruption_id alphabetical order
      - Each stochastic disruption draws one uniform value from the shared RNG
      - draw < trigger_probability → active; else → inactive (magnitude 0.0)
      - Deterministic disruptions are always active within their window

    Returns one DisruptionActivation per in-window disruption.
    """
    in_window = [d for d in disruptions if d.start_tick <= tick <= d.end_tick]
    in_window.sort(key=lambda d: d.disruption_id)  # alphabetical for FR-07

    activations: list[DisruptionActivation] = []

    for d in in_window:
        if d.is_stochastic:
            draw      = sampler.draw_uniform()
            is_active = draw < d.trigger_probability
        else:
            is_active = True

        activations.append(DisruptionActivation(
            disruption_id       = d.disruption_id,
            item_id             = d.item_id,
            disruption_type     = d.disruption_type,
            effective_magnitude = d.magnitude if is_active else 0.0,
            is_active_this_tick = is_active,
        ))

    return activations


def get_demand_multiplier(
    item_id:     str,
    activations: list[DisruptionActivation],
) -> float:
    """
    Net demand multiplier for an item this tick.
    demand_spike and demand_suppression magnitudes are multiplied together.
    Returns 1.0 if no demand disruptions are active.
    """
    multiplier = 1.0
    for a in activations:
        if a.item_id != item_id or not a.is_active_this_tick:
            continue
        if a.disruption_type in (DisruptionType.DEMAND_SPIKE, DisruptionType.DEMAND_SUPPRESSION):
            multiplier *= a.effective_magnitude
    return multiplier


def get_lead_time_multiplier(
    item_id:     str,
    activations: list[DisruptionActivation],
) -> float:
    """
    Net lead time multiplier for an item this tick.
    transit_delay magnitudes are multiplied together.
    Clamped to minimum of 1.0 (spec section 3.6).
    """
    multiplier = 1.0
    for a in activations:
        if a.item_id != item_id or not a.is_active_this_tick:
            continue
        if a.disruption_type == DisruptionType.TRANSIT_DELAY:
            multiplier *= a.effective_magnitude
    return max(1.0, multiplier)


def get_transit_loss_fraction(
    item_id:     str,
    activations: list[DisruptionActivation],
) -> float:
    """
    Net transit loss fraction for an item this tick.
    transit_loss magnitudes are multiplied together, clamped to [0.0, 1.0].
    Returns 0.0 if no transit loss disruptions are active.
    """
    fraction = 1.0
    has_loss = False
    for a in activations:
        if a.item_id != item_id or not a.is_active_this_tick:
            continue
        if a.disruption_type == DisruptionType.TRANSIT_LOSS:
            fraction *= a.effective_magnitude
            has_loss  = True
    return min(1.0, max(0.0, fraction)) if has_loss else 0.0


# ---------------------------------------------------------------------------
# Spark write (lazy import)
# ---------------------------------------------------------------------------

def write_activations(
    spark:       "SparkSession",
    sim_id:      str,
    tick:        int,
    activations: list[DisruptionActivation],
) -> None:
    """Append disruption activation records for this tick to ops_active_disruptions."""
    if not activations:
        return

    rows = [
        {
            "sim_id":              sim_id,
            "tick":                tick,
            "disruption_id":       a.disruption_id,
            "item_id":             a.item_id,
            "disruption_type":     a.disruption_type.value,
            "effective_magnitude": a.effective_magnitude,
            "is_active_this_tick": a.is_active_this_tick,
        }
        for a in activations
    ]

    spark.createDataFrame(rows, schema=_SCHEMA.strip()) \
        .write.mode("append").saveAsTable(_TABLE)
