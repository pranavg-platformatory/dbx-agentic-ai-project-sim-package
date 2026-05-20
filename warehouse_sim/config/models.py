"""
warehouse_sim/config/models.py

Pydantic v2 data models for every entity in the simulation schema.
These are the canonical typed representations used throughout the package.
No Databricks dependency - pure Python.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class RunMode(str, Enum):
    FINITE   = "finite"
    INFINITE = "infinite"
    CYCLIC   = "cyclic"


class TickUnit(str, Enum):
    HOUR = "hour"
    DAY  = "day"
    WEEK = "week"


class PatternRole(str, Enum):
    DEMAND = "demand"
    SUPPLY = "supply"


class PatternType(str, Enum):
    STATISTICAL = "statistical"
    CUSTOM      = "custom"


class Distribution(str, Enum):
    POISSON               = "poisson"
    NORMAL                = "normal"
    UNIFORM               = "uniform"
    NEGATIVE_BINOMIAL     = "negative_binomial"
    ZERO_INFLATED_POISSON = "zero_inflated_poisson"


class DisruptionType(str, Enum):
    DEMAND_SPIKE       = "demand_spike"
    DEMAND_SUPPRESSION = "demand_suppression"
    TRANSIT_DELAY      = "transit_delay"
    TRANSIT_LOSS       = "transit_loss"


class OrderStatus(str, Enum):
    PENDING        = "pending"
    ARRIVED        = "arrived"
    PARTIALLY_LOST = "partially_lost"
    FULLY_LOST     = "fully_lost"


class AgentDecision(str, Enum):
    REORDER = "reorder"
    HOLD    = "hold"


# ---------------------------------------------------------------------------
# Environment models
# ---------------------------------------------------------------------------

class SimConfig(BaseModel):
    """Maps to env_sim_config."""
    sim_id:                     str
    random_seed:                int
    num_ticks:                  Optional[int]   = None   # None = infinite
    run_mode:                   RunMode
    tick_unit:                  TickUnit
    budget_limit:               Optional[float] = None   # None = unlimited
    budget_warning_threshold:   float           = 0.10
    agent_history_window_ticks: Optional[int]   = None   # None = full history
    start_timestamp:            datetime
    created_at:                 datetime

    @model_validator(mode="after")
    def validate_finite_requires_num_ticks(self) -> SimConfig:
        if self.run_mode == RunMode.FINITE and self.num_ticks is None:
            raise ValueError("num_ticks is required when run_mode is 'finite'")
        if self.num_ticks is not None and self.num_ticks < 1:
            raise ValueError("num_ticks must be >= 1 when set")
        if self.agent_history_window_ticks is not None and self.agent_history_window_ticks < 1:
            raise ValueError("agent_history_window_ticks must be >= 1 when set")
        if not (0.0 < self.budget_warning_threshold <= 1.0):
            raise ValueError("budget_warning_threshold must be in (0.0, 1.0]")
        return self


class ItemType(BaseModel):
    """Maps to env_item_types."""
    item_id:                         str
    item_name:                       str
    unit_value:                      float  = Field(ge=0)
    initial_stock:                   int    = Field(ge=0)
    reorder_point:                   int    = Field(ge=0)
    min_order_qty:                   int    = Field(ge=1)
    max_order_qty:                   int
    holding_cost_per_unit_per_tick:  float  = Field(ge=0)
    stockout_cost_per_unit_per_tick: float  = Field(ge=0)
    order_fixed_cost:                float  = Field(ge=0)
    order_variable_cost_per_unit:    float  = Field(ge=0)
    transit_loss_cost_per_unit:      float  = Field(ge=0)

    @model_validator(mode="after")
    def validate_order_qty_range(self) -> ItemType:
        if self.max_order_qty < self.min_order_qty:
            raise ValueError("max_order_qty must be >= min_order_qty")
        return self


class Supplier(BaseModel):
    """Maps to env_suppliers."""
    supplier_id:           str
    supplier_name:         str
    base_lead_time_ticks:  int   = Field(ge=1)
    lead_time_variability: float = Field(ge=0)


class Consumer(BaseModel):
    """Maps to env_consumers."""
    consumer_id:   str
    consumer_name: str


class SupplierItemMapping(BaseModel):
    """Maps to env_supplier_item_map."""
    sim_id:      str
    supplier_id: str
    item_id:     str


class ConsumerItemMapping(BaseModel):
    """Maps to env_consumer_item_map."""
    sim_id:      str
    consumer_id: str
    item_id:     str


class Pattern(BaseModel):
    """Maps to env_patterns."""
    pattern_id:                   str
    sim_id:                       str
    item_id:                      str
    role:                         PatternRole
    pattern_type:                 PatternType
    distribution:                 Optional[Distribution]   = None
    dist_params:                  Optional[dict[str, Any]] = None
    custom_schedule:              Optional[list[float]]    = None
    seasonal_multiplier_schedule: Optional[list[float]]   = None
    noise_std:                    float = Field(default=0.0, ge=0)
    supplier_id:                  Optional[str] = None   # required when role=supply

    @model_validator(mode="after")
    def validate_pattern_fields(self) -> Pattern:
        if self.pattern_type == PatternType.STATISTICAL:
            if self.distribution is None:
                raise ValueError("distribution is required when pattern_type is 'statistical'")
            if self.dist_params is None:
                raise ValueError("dist_params is required when pattern_type is 'statistical'")
        if self.pattern_type == PatternType.CUSTOM:
            if not self.custom_schedule:
                raise ValueError("custom_schedule is required when pattern_type is 'custom'")
        if self.role == PatternRole.SUPPLY and self.supplier_id is None:
            raise ValueError("supplier_id is required when role is 'supply'")
        return self


class DisruptionSchedule(BaseModel):
    """Maps to env_disruption_schedule."""
    disruption_id:       str
    sim_id:              str
    item_id:             str
    disruption_type:     DisruptionType
    start_tick:          int   = Field(ge=0)
    end_tick:            int
    magnitude:           float
    is_stochastic:       bool
    trigger_probability: Optional[float] = None   # required when is_stochastic=True

    @model_validator(mode="after")
    def validate_disruption(self) -> DisruptionSchedule:
        if self.end_tick < self.start_tick:
            raise ValueError("end_tick must be >= start_tick")
        if self.is_stochastic:
            if self.trigger_probability is None:
                raise ValueError("trigger_probability is required when is_stochastic is True")
            if not (0.0 <= self.trigger_probability <= 1.0):
                raise ValueError("trigger_probability must be in [0.0, 1.0]")
        if self.disruption_type == DisruptionType.TRANSIT_LOSS:
            if not (0.0 <= self.magnitude <= 1.0):
                raise ValueError("magnitude must be in [0.0, 1.0] for transit_loss")
        else:
            if self.magnitude <= 0:
                raise ValueError("magnitude must be > 0 for demand/transit_delay disruptions")
        return self


# ---------------------------------------------------------------------------
# World: resolved per-sim view (convenience container, not a DB table)
# ---------------------------------------------------------------------------

class SimWorld(BaseModel):
    """
    Fully resolved world configuration for a single simulation run.
    Assembled by the loader from all env tables.
    This is what the engine receives at startup.
    """
    config:            SimConfig
    items:             dict[str, ItemType]        # keyed by item_id
    suppliers:         dict[str, Supplier]        # keyed by supplier_id
    consumers:         dict[str, Consumer]        # keyed by consumer_id
    supplier_item_map: dict[str, str]             # item_id -> supplier_id
    consumer_item_map: dict[str, str]             # item_id -> consumer_id
    demand_patterns:   dict[str, Pattern]         # item_id -> demand Pattern
    supply_patterns:   dict[str, Pattern]         # item_id -> supply Pattern (may be empty)
    disruptions:       list[DisruptionSchedule]

    def supplier_for(self, item_id: str) -> Supplier:
        supplier_id = self.supplier_item_map[item_id]
        return self.suppliers[supplier_id]

    def consumer_for(self, item_id: str) -> Consumer:
        consumer_id = self.consumer_item_map[item_id]
        return self.consumers[consumer_id]

    def disruptions_for_tick(self, tick: int) -> list[DisruptionSchedule]:
        """Return all disruptions whose window includes the given tick."""
        return [d for d in self.disruptions if d.start_tick <= tick <= d.end_tick]