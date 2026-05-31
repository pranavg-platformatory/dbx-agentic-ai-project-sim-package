'''
warehouse_sim/agent/base.py

Agent contract for the warehouse reorder simulation.

Defines three things and nothing else:
- `AgentContext`    : the read-only snapshot the engine delivers to the agent each tick
- `ReorderDecision` : the agent's response for one item (reorder or hold)
- `BaseAgent`       : the ABC every agent implementation must subclass

NOTE:
- This module has no simulation logic, no Databricks dependency, no pattern sampling
- The engine (see warehouse_sim/engine) imports this module
- Concrete agents are injected into the engine at runtime and never imported by it directly
- The catalog and schema for all the tables mentioned are specified in warehouse_sim/config/loader.py

---

Usage:

```
from warehouse_sim.agent.base import BaseAgent, AgentContext, ReorderDecision

class MyAgent(BaseAgent):
    def decide(self, context: AgentContext) -> list[ReorderDecision]:
        ...
```
'''

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


#################################################
# AgentContext building blocks
#################################################

@dataclass(frozen=True)
class ItemState:
    '''Stock snapshot for one item, as seen by the agent at decision time (end of sub-step 3b - after arrivals and demand depletion).'''

    item_id:                     str
    stock_on_hand:               int   # current units in warehouse (>= 0)
    stock_in_transit:            int   # units on order, not yet arrived
    expected_arrivals_next_tick: int   # units due to arrive next tick
    reorder_point:               int   # advisory signal from env_item_types
    min_order_qty:               int
    max_order_qty:               int


@dataclass(frozen=True)
class PendingOrder:
    '''One pending (undelivered) order, surfaced to the agent.'''

    order_id:              str
    item_id:               str
    supplier_id:           str
    order_tick:            int
    expected_arrival_tick: int
    order_qty:             int


@dataclass(frozen=True)
class DemandRecord:
    '''One row from the table "hist_demand_actuals", surfaced as demand history.'''

    tick:             int
    item_id:          str
    raw_demand:       float
    disrupted_demand: float
    fulfilled:        int
    unmet:            int


@dataclass(frozen=True)
class ActiveDisruption:
    '''One active disruption row for this tick.'''

    disruption_id:       str
    item_id:             str
    disruption_type:     str
    effective_magnitude: float
    is_active_this_tick: bool


@dataclass(frozen=True)
class CostSnapshot:
    '''Read-only cost accumulator snapshot for one item, as seen by the agent at decision time.'''

    item_id:                      str
    cumulative_holding_cost:      float
    cumulative_stockout_cost:     float
    cumulative_order_cost:        float
    cumulative_transit_loss_cost: float
    cumulative_total_cost:        float
    remaining_budget:             Optional[float]  # None if unlimited


#################################################
# AgentContext
#################################################

@dataclass(frozen=True)
class AgentContext:
    '''
    The complete read-only snapshot delivered to the agent at sub-step 4 of each tick, after arrivals (3a) and demand depletion (3b).
    
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

    # Fields
    
    `sim_id`             : identifies the simulation run
    `tick`               : current tick number
    `item_states`        : stock snapshot per item (keyed by it`em_id)
    `pending_orders`     : all open (undelivered) orders across all items
    `demand_history`     : last N ticks of hist_demand_actuals per item (N = `agent_history_window_ticks`; all history if None)
    `active_disruptions` : disruptions active this tick (`is_active_this_tick`=True)
    `cost_snapshots`     : cumulative cost totals per item (keyed by `item_id`)
    `remaining_budget`   : global remaining budget (None if unlimited)
    '''

    sim_id:             str
    tick:               int
    item_states:        dict[str, ItemState]
    pending_orders:     list[PendingOrder]
    demand_history:     dict[str, list[DemandRecord]]
    active_disruptions: list[ActiveDisruption]
    cost_snapshots:     dict[str, CostSnapshot]
    remaining_budget:   Optional[float]

    def items(self) -> list[str]:
        '''
        Sorted list of item_ids the agent is responsible for.
        
        ---

        RETURNS:
        - (list[str]): Sortted list of item IDs
        '''

        return sorted(self.item_states.keys())

    def pending_for(self, item_id: str) -> list[PendingOrder]:
        '''
        Pending orders for one item.

        ---

        PARAMETERS:
        - `item_id` (str): Item ID (which corresponds to a specific item type)

        RETURNS:
        - (list[PendingOrder]): List of pending orders for the item ID (each pending order encapsulated as a `PendingOrder` instance) 
        '''
        return [o for o in self.pending_orders if o.item_id == item_id]

    def history_for(self, item_id: str) -> list[DemandRecord]:
        '''
        Demand history for one item, oldest first.
        
        ---

        PARAMETERS:
        - `item_id` (str): Item ID (which corresponds to a specific item type)

        RETURNS:
        - (list[DemandRecord]): List of demand records for the item ID (each demand record encapsulated as a `DemandRecord` instance) 
        '''

        return self.demand_history.get(item_id, [])

    def disruptions_for(self, item_id: str) -> list[ActiveDisruption]:
        '''
        Active disruptions for one item this tick.
        
        ---

        PARAMETERS:
        - `item_id` (str): Item ID (which corresponds to a specific item type)

        RETURNS:
        - (list[ActiveDisruption]): List of active disruptions for the item ID (each active disruption encapsulated as a `ActiveDisruption` instance) 
        '''

        return [d for d in self.active_disruptions if d.item_id == item_id]


#################################################
# ReorderDecision
#################################################

@dataclass(frozen=True)
class ReorderDecision:
    '''
    The agent's decision for one item in one tick.

    ---

    # Fields

    `item_id`   : the item this decision covers
    `order_qty` : units to order; 0 means hold
    `reasoning` : optional free-text explanation (populated by LLM agents; optional for rule-based agents)

    Constraints (enforced by the engine, not here):
      - `order_qty` == 0  ->  decision logged as HOLD
      - `order_qty` >  0  ->  decision logged as REORDER
      - `order_qty` must satisfy `min_order_qty <= order_qty <= max_order_qty` when > 0
    '''

    item_id:   str
    order_qty: int
    reasoning: Optional[str] = None

    @property
    def is_reorder(self) -> bool:
        return self.order_qty > 0

    @property
    def is_hold(self) -> bool:
        return self.order_qty == 0

# Contract-checking error that does not rely on exact class matches
def follows_reorder_agent_class_contract(object:object) -> bool:
    '''
    Checks if the given object adheres to ReorderDecision's contract.

    This ensures that identical contracts under different class definitions can still be appropriately validated.
    
    NOTE: This is relevant since the reasoning agent that has been separately developed may use an identical contract under its own class definition.
    
    ---

    PARAMETERS:
    - `object` (object): Object to validate

    RETURNS:
    - (bool): True => Object follows ReorderDecision's contract, False => Object does not follow ReorderDecision's contract
    '''
    
    try:
        item_id = object.item_id
        order_qty = object.order_qty
        reasoning = object.reasoning
        is_reorder = object.is_reorder
        is_hold = object.is_hold
    except AttributeError:
        return False

    try:
        if not isinstance(item_id, str): raise TypeError
        if not isinstance(order_qty, int): raise TypeError
        if not isinstance(reasoning, str) and not (reasoning is None): raise TypeError
        if not isinstance(is_reorder(), bool): raise TypeError
        if not isinstance(is_hold(), bool): raise TypeError
        # NOTE: If is_reorder or is_hold are not callable, a TypeError is still raised.
    except TypeError:
        return False

#################################################
# BaseAgent
#################################################

class BaseAgent(ABC):
    '''
    Abstract base class for all reorder agents.

    - This is a subclass this and implement `decide`
    - The engine calls `decide` once per tick, passing a fully populated `AgentContext`
    - The agent returns 1 `ReorderDecision` per item it manages.

    The agent must not:
    - Write to any table directly
    - Mutate the `AgentContext`
    - Retain mutable state that would break reproducibility
    '''

    @abstractmethod
    def decide(self, context: AgentContext) -> list[ReorderDecision]:
        '''
        Evaluate the current simulation state and return reorder decisions.

        ---

        PARAMETERS:
        - `context` (AgentContext): Complete read-only snapshot of the simulation at this tick

        RETURNS:
        - (list[ReorderDecision]): 1 decision per item in `context.items()` \n
          NOTE: The engine will raise an exception if an item is missing from the returned list
        '''
        ...

    def agent_version(self) -> str:
        '''
        - Version/identifier string for this agent
        - Stored in the field `agent_version` in the table "hist_reorder_decisions"
        - IMPORTANT: Override in subclasses for meaningful labels
        '''

        return self.__class__.__name__
