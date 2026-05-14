"""
warehouse_sim/eventlog/event_log.py

Append-only event writer for the simulation event log.

Provides a typed `EventLogger` class with one method per event type.
The engine calls these methods directly — it never constructs raw dicts.
No simulation logic lives here; this is pure serialisation and I/O.

No Databricks dependency at import time — PySpark is imported lazily
inside `_write` so this module is testable without a Spark session.

Usage:
    from warehouse_sim.eventlog.event_log import EventLogger
    logger = EventLogger(spark, sim_id="sim_001")
    logger.sim_started(tick=0, config_snapshot={"num_ticks": 30, ...})
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Catalog / schema constants
# ---------------------------------------------------------------------------

CATALOG    = "hackathon_of_the_century"
EVENTLOG   = f"{CATALOG}.tables4eventlog"
TABLE      = f"{EVENTLOG}.event_log"

# Explicit schema — avoids PySpark type inference issues (same lesson as setup.py)
_SCHEMA = """
    event_id   STRING,
    sim_id     STRING,
    tick       INT,
    event_type STRING,
    item_id    STRING,
    entity_id  STRING,
    payload    STRING,
    logged_at  TIMESTAMP
"""

# Valid event types — matches spec section 7
EVENT_TYPES = frozenset({
    "SIM_STARTED",
    "SIM_ENDED",
    "TICK_STARTED",
    "TICK_ENDED",
    "DEMAND_DRAWN",
    "SUPPLY_ARRIVED",
    "REORDER_PLACED",
    "REORDER_HELD",
    "DISRUPTION_ACTIVATED",
    "DISRUPTION_DEACTIVATED",
    "STOCKOUT_OCCURRED",
    "BUDGET_WARNING",
    "BUDGET_EXHAUSTED",
    "COST_ACCRUED",
    "TRANSIT_LOSS_APPLIED",
    "LEAD_TIME_EXTENDED",
})


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _new_id() -> str:
    return str(uuid.uuid4())


def _serialise(payload: dict[str, Any]) -> str:
    """Serialise payload to JSON string. Handles datetime and other non-standard types."""
    def _default(obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serialisable")
    return json.dumps(payload, default=_default)


def _build_row(
    sim_id:     str,
    tick:       int,
    event_type: str,
    payload:    dict[str, Any],
    item_id:    str | None   = None,
    entity_id:  str | None   = None,
) -> dict:
    """Build a plain dict representing one event_log row."""
    if event_type not in EVENT_TYPES:
        raise ValueError(f"Unknown event_type: {event_type!r}")
    return {
        "event_id":   _new_id(),
        "sim_id":     sim_id,
        "tick":       tick,
        "event_type": event_type,
        "item_id":    item_id,
        "entity_id":  entity_id,
        "payload":    _serialise(payload),
        "logged_at":  _now(),
    }


# ---------------------------------------------------------------------------
# EventLogger
# ---------------------------------------------------------------------------

class EventLogger:
    """
    Typed event writer. One instance per simulation run.
    Each public method corresponds to one event type in the spec.

    The engine imports and calls this — the logger never calls the engine.
    """

    def __init__(self, spark: "SparkSession", sim_id: str) -> None:
        self._spark  = spark
        self._sim_id = sim_id

    # ------------------------------------------------------------------
    # Internal write
    # ------------------------------------------------------------------

    def _write(self, row: dict) -> None:
        """Append one event row to event_log using an explicit schema."""
        self._spark.createDataFrame([row], schema=_SCHEMA.strip()) \
            .write.mode("append").saveAsTable(TABLE)

    def _emit(
        self,
        tick:       int,
        event_type: str,
        payload:    dict[str, Any],
        item_id:    str | None = None,
        entity_id:  str | None = None,
    ) -> None:
        row = _build_row(
            sim_id     = self._sim_id,
            tick       = tick,
            event_type = event_type,
            payload    = payload,
            item_id    = item_id,
            entity_id  = entity_id,
        )
        self._write(row)

    # ------------------------------------------------------------------
    # Simulation lifecycle
    # ------------------------------------------------------------------

    def sim_started(self, tick: int, config_snapshot: dict[str, Any]) -> None:
        self._emit(tick, "SIM_STARTED", {"config_snapshot": config_snapshot})

    def sim_ended(
        self,
        tick:                int,
        total_cost:          float,
        total_stockout_ticks: int,
        total_reorders:      int,
    ) -> None:
        self._emit(tick, "SIM_ENDED", {
            "total_cost":           total_cost,
            "total_stockout_ticks": total_stockout_ticks,
            "total_reorders":       total_reorders,
        })

    # ------------------------------------------------------------------
    # Tick lifecycle
    # ------------------------------------------------------------------

    def tick_started(self, tick: int) -> None:
        self._emit(tick, "TICK_STARTED", {"tick": tick})

    def tick_ended(self, tick: int) -> None:
        self._emit(tick, "TICK_ENDED", {"tick": tick})

    # ------------------------------------------------------------------
    # Demand
    # ------------------------------------------------------------------

    def demand_drawn(
        self,
        tick:             int,
        item_id:          str,
        raw_demand:       float,
        disrupted_demand: float,
        fulfilled:        int,
        unmet:            int,
    ) -> None:
        self._emit(tick, "DEMAND_DRAWN", {
            "raw_demand":       raw_demand,
            "disrupted_demand": disrupted_demand,
            "fulfilled":        fulfilled,
            "unmet":            unmet,
        }, item_id=item_id)

    def stockout_occurred(
        self,
        tick:          int,
        item_id:       str,
        unmet_demand:  int,
        stockout_cost: float,
    ) -> None:
        self._emit(tick, "STOCKOUT_OCCURRED", {
            "unmet_demand":  unmet_demand,
            "stockout_cost": stockout_cost,
        }, item_id=item_id)

    # ------------------------------------------------------------------
    # Supply
    # ------------------------------------------------------------------

    def supply_arrived(
        self,
        tick:         int,
        item_id:      str,
        order_id:     str,
        ordered_qty:  int,
        arrived_qty:  int,
        lost_qty:     int,
    ) -> None:
        self._emit(tick, "SUPPLY_ARRIVED", {
            "order_id":    order_id,
            "ordered_qty": ordered_qty,
            "arrived_qty": arrived_qty,
            "lost_qty":    lost_qty,
        }, item_id=item_id, entity_id=order_id)

    def transit_loss_applied(
        self,
        tick:          int,
        item_id:       str,
        order_id:      str,
        lost_qty:      int,
        arrived_qty:   int,
        disruption_id: str,
    ) -> None:
        self._emit(tick, "TRANSIT_LOSS_APPLIED", {
            "order_id":      order_id,
            "lost_qty":      lost_qty,
            "arrived_qty":   arrived_qty,
            "disruption_id": disruption_id,
        }, item_id=item_id, entity_id=order_id)

    # ------------------------------------------------------------------
    # Reorder decisions
    # ------------------------------------------------------------------

    def reorder_placed(
        self,
        tick:                  int,
        item_id:               str,
        order_id:              str,
        order_qty:             int,
        expected_arrival_tick: int,
        order_cost:            float,
    ) -> None:
        self._emit(tick, "REORDER_PLACED", {
            "order_id":              order_id,
            "order_qty":             order_qty,
            "expected_arrival_tick": expected_arrival_tick,
            "order_cost":            order_cost,
        }, item_id=item_id, entity_id=order_id)

    def reorder_held(
        self,
        tick:              int,
        item_id:           str,
        stock_on_hand:     int,
        stock_in_transit:  int,
        reasoning:         str | None = None,
    ) -> None:
        self._emit(tick, "REORDER_HELD", {
            "stock_on_hand":    stock_on_hand,
            "stock_in_transit": stock_in_transit,
            "reasoning":        reasoning,
        }, item_id=item_id)

    def lead_time_extended(
        self,
        tick:               int,
        item_id:            str,
        order_id:           str,
        original_lead_time: int,
        extended_lead_time: int,
        disruption_id:      str,
    ) -> None:
        self._emit(tick, "LEAD_TIME_EXTENDED", {
            "order_id":           order_id,
            "original_lead_time": original_lead_time,
            "extended_lead_time": extended_lead_time,
            "disruption_id":      disruption_id,
        }, item_id=item_id, entity_id=order_id)

    # ------------------------------------------------------------------
    # Disruptions
    # ------------------------------------------------------------------

    def disruption_activated(
        self,
        tick:               int,
        item_id:            str,
        disruption_id:      str,
        disruption_type:    str,
        effective_magnitude: float,
    ) -> None:
        self._emit(tick, "DISRUPTION_ACTIVATED", {
            "disruption_id":      disruption_id,
            "disruption_type":    disruption_type,
            "effective_magnitude": effective_magnitude,
        }, item_id=item_id, entity_id=disruption_id)

    def disruption_deactivated(
        self,
        tick:          int,
        item_id:       str,
        disruption_id: str,
    ) -> None:
        self._emit(tick, "DISRUPTION_DEACTIVATED", {
            "disruption_id": disruption_id,
        }, item_id=item_id, entity_id=disruption_id)

    # ------------------------------------------------------------------
    # Costs
    # ------------------------------------------------------------------

    def cost_accrued(
        self,
        tick:               int,
        item_id:            str,
        holding_cost:       float,
        stockout_cost:      float,
        order_cost:         float,
        transit_loss_cost:  float,
        tick_total:         float,
    ) -> None:
        self._emit(tick, "COST_ACCRUED", {
            "holding_cost":      holding_cost,
            "stockout_cost":     stockout_cost,
            "order_cost":        order_cost,
            "transit_loss_cost": transit_loss_cost,
            "tick_total":        tick_total,
        }, item_id=item_id)

    # ------------------------------------------------------------------
    # Budget
    # ------------------------------------------------------------------

    def budget_warning(
        self,
        tick:              int,
        remaining_budget:  float,
        budget_limit:      float,
        threshold:         float,
    ) -> None:
        self._emit(tick, "BUDGET_WARNING", {
            "remaining_budget": remaining_budget,
            "budget_limit":     budget_limit,
            "threshold":        threshold,
        })

    def budget_exhausted(
        self,
        tick:             int,
        remaining_budget: float,
    ) -> None:
        self._emit(tick, "BUDGET_EXHAUSTED", {
            "tick":             tick,
            "remaining_budget": remaining_budget,
        })


# ---------------------------------------------------------------------------
# Standalone row builder (no Spark — used by tests and engine internals)
# ---------------------------------------------------------------------------

def build_event_row(
    sim_id:     str,
    tick:       int,
    event_type: str,
    payload:    dict[str, Any],
    item_id:    str | None = None,
    entity_id:  str | None = None,
) -> dict:
    """
    Build and return a single event row dict without writing to Spark.
    Used in unit tests and anywhere the row needs to be inspected
    before writing.
    """
    return _build_row(
        sim_id=sim_id,
        tick=tick,
        event_type=event_type,
        payload=payload,
        item_id=item_id,
        entity_id=entity_id,
    )
