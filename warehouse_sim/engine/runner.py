'''
warehouse_sim/engine/runner.py

The simulation tick loop:
- Orchestrates all sub-modules in the correct sequence per tick
- This file contains no simulation logic of its own
- Instead, it only calls the sub-modules in order and passes data between them

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
- The agent (warehouse_sim/agent) and event logger (warehouse_sim/event_logger) are injected at construction time
- The runner never imports a concrete agent implementation

Usage:

```
from warehouse_sim.engine.runner import SimRunner
from warehouse_sim.agent.rule_based import RuleBasedAgent

world   = load_world(spark, sim_id)
sampler = PatternSampler(seed=world.config.random_seed)
logger  = EventLogger(spark, sim_id=world.config.sim_id)
agent   = RuleBasedAgent()

runner  = SimRunner(spark, world, agent, logger, sampler)
runner.run()
```
'''

from __future__ import annotations

from typing import Optional, TYPE_CHECKING

from ..config.models import RunMode, SimWorld
from ..world.patterns import PatternSampler
from ..event_log.event_log import EventLogger
from ..agent.base import (
    ActiveDisruption,
    AgentContext,
    BaseAgent,
    CostSnapshot,
    DemandRecord,
    ItemState,
    PendingOrder,
    ReorderDecision,
)
from .disruptions import (
    DisruptionActivation,
    evaluate_disruptions,
    write_activations,
)
from .supply import (
    PlacedOrder,
    fetch_pending_orders,
    place_order,
    process_arrivals,
    update_order_status,
    write_arrivals,
    write_placed_order,
)
from .demand import draw_demand, write_demand_actuals
from .state import (
    StockState,
    apply_arrivals,
    apply_demand,
    apply_new_order,
    initialise_states,
    write_warehouse_state,
)
from .costs import (
    CostState,
    accumulate,
    check_budget,
    compute_holding_cost,
    compute_order_cost,
    compute_stockout_cost,
    compute_transit_loss_cost,
    deduct_budget,
    write_cost_accumulator,
    write_cost_by_tick,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Hist tables written by runner directly
# ---------------------------------------------------------------------------

CATALOG = "hackathon_of_the_century"

_DECISIONS_TABLE  = f"{CATALOG}.tables4hist.hist_reorder_decisions"

_DECISIONS_SCHEMA = '''
    sim_id                       STRING,
    tick                         INT,
    item_id                      STRING,
    supplier_id                  STRING,
    stock_on_hand_at_decision    INT,
    stock_in_transit_at_decision INT,
    decision                     STRING,
    order_qty                    INT,
    order_id                     STRING,
    agent_reasoning              STRING,
    agent_version                STRING
'''


# ---------------------------------------------------------------------------
# SimRunner
# ---------------------------------------------------------------------------

class SimRunner:
    '''
    Orchestrates the simulation tick loop.

    ---

    PARAMETERS:
    - `spark` (SparkSession): Active SparkSession
    - `world` (SimWorld): Fully loaded SimWorld (from `load_world`)
    - `agent` (BaseAgent): Any BaseAgent subclass - injected, never imported directly
    - `logger` (EventLogger): EventLogger instance for this sim run
    - `sampler` (PatternSampler): PatternSampler seeded with world.config.random_see
    '''

    def __init__(
        self,
        spark:   "SparkSession",
        world:   SimWorld,
        agent:   BaseAgent,
        logger:  EventLogger,
        sampler: PatternSampler,
    ) -> None:
        self._spark   = spark
        self._world   = world
        self._agent   = agent
        self._logger  = logger
        self._sampler = sampler
        self._sim_id  = world.config.sim_id
        self._config  = world.config

        # In-memory state - initialised in run()
        self._stock_states: dict[str, StockState] = {}
        self._cost_states:  dict[str, CostState]  = {}
        self._remaining_budget: Optional[float]   = world.config.budget_limit

        # Budget warning guard - fire BUDGET_WARNING only once per crossing
        self._budget_warning_fired: bool = False

        # Totals for SIM_ENDED
        self._total_reorders:       int   = 0
        self._total_stockout_ticks: int   = 0
        self._total_cost:           float = 0.0

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        '''
        Execute the simulation.
        
        NOTE: The simulation runs for `config.num_ticks` ticks when run_mode is 'finite', or forever when 'infinite' or 'cyclic'.
        '''
        
        self._initialise()

        tick = 0
        while self._should_continue(tick):
            self._run_tick(tick)
            tick += 1

        self._teardown(tick - 1)

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def _initialise(self) -> None:
        '''Set up in-memory state and fire the event `SIM_STARTED`.'''

        self._stock_states = initialise_states(self._world)
        self._cost_states  = {
            item_id: CostState(item_id=item_id)
            for item_id in self._world.items
        }

        self._logger.sim_started(
            tick             = 0,
            config_snapshot  = {
                "sim_id":    self._sim_id,
                "run_mode":  self._config.run_mode.value,
                "num_ticks": self._config.num_ticks,
                "tick_unit": self._config.tick_unit.value,
                "seed":      self._config.random_seed,
            },
        )

        # Write tick-0 warehouse state (initial_stock, no arrivals or demand yet)
        write_warehouse_state(self._spark, self._sim_id, tick=0,
                              states=self._stock_states)

    # ------------------------------------------------------------------
    # Loop termination
    # ------------------------------------------------------------------

    def _should_continue(self, tick: int) -> bool:
        if self._config.run_mode == RunMode.FINITE:
            return tick < self._config.num_ticks
        return True   # INFINITE and CYCLIC run until interrupted

    # ------------------------------------------------------------------
    # Per-tick orchestration
    # ------------------------------------------------------------------

    def _run_tick(self, tick: int) -> None:
        self._logger.tick_started(tick)

        # ----------------------------------------------------------------
        # [0] Evaluate disruptions
        # ----------------------------------------------------------------
        activations = evaluate_disruptions(
            tick        = tick,
            disruptions = self._world.disruptions,
            sampler     = self._sampler,
        )
        write_activations(self._spark, self._sim_id, tick, activations)

        # Fire DISRUPTION_ACTIVATED / DEACTIVATED events
        self._emit_disruption_events(tick, activations)

        # ----------------------------------------------------------------
        # [1] Process supply arrivals
        # ----------------------------------------------------------------
        pending_raw   = fetch_pending_orders(self._spark, self._sim_id)
        arrival_results = process_arrivals(tick, pending_raw, activations)

        if arrival_results:
            update_order_status(self._spark, self._sim_id, arrival_results)
            write_arrivals(self._spark, self._sim_id, tick, arrival_results)

        # ----------------------------------------------------------------
        # [3a] Apply arrivals to stock
        # ----------------------------------------------------------------
        arrival_by_item: dict[str, int] = {}
        lost_by_item:    dict[str, int] = {}

        for r in arrival_results:
            arrival_by_item[r.item_id] = arrival_by_item.get(r.item_id, 0) + r.arrived_qty
            lost_by_item[r.item_id]    = lost_by_item.get(r.item_id, 0)    + r.lost_qty

        for item_id, arrived_qty in arrival_by_item.items():
            apply_arrivals(self._stock_states[item_id], arrived_qty)

        # Emit supply and transit loss events
        for r in arrival_results:
            self._logger.supply_arrived(
                tick=tick, item_id=r.item_id, order_id=r.order_id,
                ordered_qty=r.ordered_qty, arrived_qty=r.arrived_qty,
                lost_qty=r.lost_qty,
            )
            if r.lost_qty > 0:
                # Find which disruption caused the loss
                loss_disruption = next(
                    (a.disruption_id for a in activations
                     if a.item_id == r.item_id
                     and a.disruption_type.value == "transit_loss"
                     and a.is_active_this_tick),
                    "unknown",
                )
                self._logger.transit_loss_applied(
                    tick=tick, item_id=r.item_id, order_id=r.order_id,
                    lost_qty=r.lost_qty, arrived_qty=r.arrived_qty,
                    disruption_id=loss_disruption,
                )

        # ----------------------------------------------------------------
        # [2] Draw demand + [3b] Apply demand to stock
        # ----------------------------------------------------------------
        demand_results = []
        has_stockout   = False

        for item_id in sorted(self._world.items):   # alphabetical for FR-07
            pattern = self._world.demand_patterns[item_id]
            stock   = self._stock_states[item_id].stock_on_hand

            result  = draw_demand(
                tick          = tick,
                item_id       = item_id,
                pattern       = pattern,
                stock_on_hand = stock,
                activations   = activations,
                sampler       = self._sampler,
            )
            demand_results.append(result)
            apply_demand(self._stock_states[item_id], result.fulfilled)

            self._logger.demand_drawn(
                tick=tick, item_id=item_id,
                raw_demand=result.raw_demand,
                disrupted_demand=result.disrupted_demand,
                fulfilled=result.fulfilled,
                unmet=result.unmet,
            )

            if result.unmet > 0:
                has_stockout = True
                self._total_stockout_ticks += 1

        write_demand_actuals(
            self._spark, self._sim_id, tick, demand_results,
            consumer_map=self._world.consumer_item_map,
        )

        # ----------------------------------------------------------------
        # Update expected_arrivals_next_tick on stock states
        # ----------------------------------------------------------------
        self._update_expected_arrivals(tick)

        # ----------------------------------------------------------------
        # Write warehouse state (post-arrivals, post-demand)
        # ----------------------------------------------------------------
        write_warehouse_state(self._spark, self._sim_id, tick,
                              states=self._stock_states)

        # ----------------------------------------------------------------
        # [4] Agent decides
        # ----------------------------------------------------------------
        context   = self._build_agent_context(tick, activations)
        decisions = self._agent.decide(context)
        self._validate_decisions(decisions, context)

        placed_orders: list[PlacedOrder] = []
        tick_order_costs: dict[str, float] = {i: 0.0 for i in self._world.items}

        for decision in decisions:
            item_id  = decision.item_id
            item     = self._world.items[item_id]
            supplier = self._world.supplier_for(item_id)

            if decision.is_reorder:
                order_cost = compute_order_cost(decision.order_qty, item)

                # Budget check
                if not check_budget(self._remaining_budget, order_cost):
                    # Cannot afford - log as hold
                    self._logger.reorder_held(
                        tick=tick, item_id=item_id,
                        stock_on_hand=self._stock_states[item_id].stock_on_hand,
                        stock_in_transit=self._stock_states[item_id].stock_in_transit,
                        reasoning="Budget insufficient for reorder.",
                    )
                    self._write_decision_row(
                        tick=tick, item_id=item_id,
                        supplier_id=supplier.supplier_id,
                        state=self._stock_states[item_id],
                        decision="hold", order_qty=0,
                        order_id=None,
                        reasoning="Budget insufficient for reorder.",
                    )
                    continue

                order = place_order(
                    tick                  = tick,
                    item_id               = item_id,
                    supplier_id           = supplier.supplier_id,
                    order_qty             = decision.order_qty,
                    base_lead_time_ticks  = supplier.base_lead_time_ticks,
                    lead_time_variability = supplier.lead_time_variability,
                    activations           = activations,
                    sampler               = self._sampler,
                )
                write_placed_order(self._spark, self._sim_id, order)
                apply_new_order(self._stock_states[item_id], order.order_qty)
                placed_orders.append(order)

                self._remaining_budget = deduct_budget(self._remaining_budget, order_cost)
                tick_order_costs[item_id] += order_cost
                self._total_reorders += 1

                # Check if lead time was extended
                base_lt     = supplier.base_lead_time_ticks
                actual_lt   = order.expected_arrival_tick - tick
                if actual_lt > base_lt:
                    loss_dis = next(
                        (a.disruption_id for a in activations
                         if a.item_id == item_id
                         and a.disruption_type.value == "transit_delay"
                         and a.is_active_this_tick),
                        "unknown",
                    )
                    self._logger.lead_time_extended(
                        tick=tick, item_id=item_id, order_id=order.order_id,
                        original_lead_time=base_lt,
                        extended_lead_time=actual_lt,
                        disruption_id=loss_dis,
                    )

                self._logger.reorder_placed(
                    tick=tick, item_id=item_id, order_id=order.order_id,
                    order_qty=order.order_qty,
                    expected_arrival_tick=order.expected_arrival_tick,
                    order_cost=order_cost,
                )
                self._write_decision_row(
                    tick=tick, item_id=item_id,
                    supplier_id=supplier.supplier_id,
                    state=self._stock_states[item_id],
                    decision="reorder", order_qty=order.order_qty,
                    order_id=order.order_id,
                    reasoning=decision.reasoning,
                )

            else:
                self._logger.reorder_held(
                    tick=tick, item_id=item_id,
                    stock_on_hand=self._stock_states[item_id].stock_on_hand,
                    stock_in_transit=self._stock_states[item_id].stock_in_transit,
                    reasoning=decision.reasoning,
                )
                self._write_decision_row(
                    tick=tick, item_id=item_id,
                    supplier_id=supplier.supplier_id,
                    state=self._stock_states[item_id],
                    decision="hold", order_qty=0,
                    order_id=None,
                    reasoning=decision.reasoning,
                )

        # ----------------------------------------------------------------
        # [5] Accumulate costs
        # ----------------------------------------------------------------
        tick_costs: dict[str, dict] = {}

        for item_id, cs in self._cost_states.items():
            item    = self._world.items[item_id]
            stock   = self._stock_states[item_id].stock_on_hand

            # Find unmet demand for this item this tick
            unmet   = next((r.unmet for r in demand_results if r.item_id == item_id), 0)

            # Transit loss cost - charged at arrival
            lost    = lost_by_item.get(item_id, 0)

            h = compute_holding_cost(stock, item)
            s = compute_stockout_cost(unmet, item)
            o = tick_order_costs.get(item_id, 0.0)
            t = compute_transit_loss_cost(lost, item)

            accumulate(cs, h, s, o, t)

            tick_costs[item_id] = {
                "holding": h, "stockout": s, "order": o, "transit_loss": t
            }

            tick_total = h + s + o + t
            self._total_cost += tick_total

            self._logger.cost_accrued(
                tick=tick, item_id=item_id,
                holding_cost=h, stockout_cost=s,
                order_cost=o, transit_loss_cost=t,
                tick_total=tick_total,
            )

            if unmet > 0:
                self._logger.stockout_occurred(
                    tick=tick, item_id=item_id,
                    unmet_demand=unmet,
                    stockout_cost=s,
                )

        write_cost_accumulator(self._spark, self._sim_id, tick,
                               self._cost_states, self._remaining_budget)
        write_cost_by_tick(self._spark, self._sim_id, tick, tick_costs)

        # ----------------------------------------------------------------
        # Budget warning / exhausted events
        # ----------------------------------------------------------------
        self._check_budget_events(tick)

        self._logger.tick_ended(tick)

    # ------------------------------------------------------------------
    # Teardown
    # ------------------------------------------------------------------

    def _teardown(self, final_tick: int) -> None:
        self._logger.sim_ended(
            tick                 = final_tick,
            total_cost           = self._total_cost,
            total_stockout_ticks = self._total_stockout_ticks,
            total_reorders       = self._total_reorders,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _update_expected_arrivals(self, tick: int) -> None:
        '''
        Compute expected_arrivals_next_tick for each item by querying the in-flight pending orders that are due at tick+1.
        
        NOTE: We query the table "ops_pending_orders" rather than keeping a separate structure.
        
        ---

        PARAMETERS:
        - `tick` (int): Simulation tick number

        RETURNS:
        - None
        '''
        
        pending = fetch_pending_orders(self._spark, self._sim_id)
        next_tick = tick + 1
        for item_id, state in self._stock_states.items():
            state.expected_arrivals_next_tick = sum(
                o["order_qty"] for o in pending
                if o["item_id"] == item_id
                and o["expected_arrival_tick"] == next_tick
            )

    def _build_agent_context(
        self,
        tick:        int,
        activations: list[DisruptionActivation],
    ) -> AgentContext:
        '''
        Assemble the AgentContext the agent will receive.
        
        ---

        PARAMETERS:
        - `tick` (int): Simulation tick number
        - `activations` (list[DisruptionActivation]): Disruption activations for this tick (from sub-step 0)

        RETURNS:
        - (AgentContext): The complete read-only snapshot delivered to the agent at sub-step 4 of each tick, after arrivals (3a) and demand depletion (3b)

        NOTE: See module docstring for the reference for the simulation loop steps and sub-steps.
        '''

        # -- Item states
        item_states = {
            item_id: ItemState(
                item_id                     = item_id,
                stock_on_hand               = s.stock_on_hand,
                stock_in_transit            = s.stock_in_transit,
                expected_arrivals_next_tick = s.expected_arrivals_next_tick,
                reorder_point               = self._world.items[item_id].reorder_point,
                min_order_qty               = self._world.items[item_id].min_order_qty,
                max_order_qty               = self._world.items[item_id].max_order_qty,
            )
            for item_id, s in self._stock_states.items()
        }

        # -- Pending orders
        pending_raw = fetch_pending_orders(self._spark, self._sim_id)
        pending_orders = [
            PendingOrder(
                order_id              = o["order_id"],
                item_id               = o["item_id"],
                supplier_id           = o["supplier_id"],
                order_tick            = o["order_tick"],
                expected_arrival_tick = o["expected_arrival_tick"],
                order_qty             = o["order_qty"],
            )
            for o in pending_raw
        ]

        # -- Demand history (last N ticks per item)
        window  = self._config.agent_history_window_ticks
        history = self._fetch_demand_history(tick, window)

        # -- Active disruptions (only those that triggered)
        active_disruptions = [
            ActiveDisruption(
                disruption_id       = a.disruption_id,
                item_id             = a.item_id,
                disruption_type     = a.disruption_type.value,
                effective_magnitude = a.effective_magnitude,
                is_active_this_tick = a.is_active_this_tick,
            )
            for a in activations
            if a.is_active_this_tick
        ]

        # -- Cost snapshots
        cost_snapshots = {
            item_id: CostSnapshot(
                item_id                      = item_id,
                cumulative_holding_cost      = cs.cumulative_holding_cost,
                cumulative_stockout_cost     = cs.cumulative_stockout_cost,
                cumulative_order_cost        = cs.cumulative_order_cost,
                cumulative_transit_loss_cost = cs.cumulative_transit_loss_cost,
                cumulative_total_cost        = cs.cumulative_total,
                remaining_budget             = self._remaining_budget,
            )
            for item_id, cs in self._cost_states.items()
        }

        return AgentContext(
            sim_id             = self._sim_id,
            tick               = tick,
            item_states        = item_states,
            pending_orders     = pending_orders,
            demand_history     = history,
            active_disruptions = active_disruptions,
            cost_snapshots     = cost_snapshots,
            remaining_budget   = self._remaining_budget,
        )

    def _fetch_demand_history(
        self,
        tick:   int,
        window: Optional[int],
    ) -> dict[str, list[DemandRecord]]:
        '''
        Read demand history from the table "hist_demand_actuals"; return last `window` ticks per item (all if window is None).
        
        ---

        PARAMETERS:
        - `tick` (int): Simulation tick number
        - `window` (int, optional): Number of consecutive ticks (before `tick`) to consider
        
        RETURNS:
        - (dict[str, list[DemandRecord]]): Dictionary linking item IDs (which correspond to specific item types) to corresponding DemandRecord instances
        '''

        if window is not None:
            min_tick = tick - window
            where    = f"AND tick > {min_tick}"
        else:
            where = ""

        rows = self._spark.sql(f'''
            SELECT tick, item_id, raw_demand, disrupted_demand,
                   fulfilled_demand, unmet_demand
            FROM {CATALOG}.tables4hist.hist_demand_actuals
            WHERE sim_id = '{self._sim_id}' {where}
            ORDER BY tick ASC
        ''').collect()

        history: dict[str, list[DemandRecord]] = {}
        for row in rows:
            rec = DemandRecord(
                tick             = row["tick"],
                item_id          = row["item_id"],
                raw_demand       = row["raw_demand"],
                disrupted_demand = row["disrupted_demand"],
                fulfilled        = row["fulfilled_demand"],
                unmet            = row["unmet_demand"],
            )
            history.setdefault(row["item_id"], []).append(rec)
        return history

    def _emit_disruption_events(
        self,
        tick:        int,
        activations: list[DisruptionActivation],
    ) -> None:
        '''
        Fire the event `DISRUPTION_ACTIVATED` for newly triggered disruptions.
        
        ---

        PARAMETERS:
        - `tick` (int): Simulation tick number
        - `activations` (list[DisruptionActivation]): Disruption activations for this tick (from sub-step 0)

        RETURNS:
        - None
        '''

        for a in activations:
            if a.is_active_this_tick:
                self._logger.disruption_activated(
                    tick                = tick,
                    item_id             = a.item_id,
                    disruption_id       = a.disruption_id,
                    disruption_type     = a.disruption_type.value,
                    effective_magnitude = a.effective_magnitude,
                )
            # Fire DISRUPTION_DEACTIVATED on the last tick of the window
            sched = next(
                (d for d in self._world.disruptions if d.disruption_id == a.disruption_id),
                None,
            )
            if sched and tick == sched.end_tick:
                self._logger.disruption_deactivated(
                    tick          = tick,
                    item_id       = a.item_id,
                    disruption_id = a.disruption_id,
                )

    def _write_decision_row(
        self,
        tick:        int,
        item_id:     str,
        supplier_id: str,
        state:       StockState,
        decision:    str,
        order_qty:   int,
        order_id:    Optional[str],
        reasoning:   Optional[str],
    ) -> None:
        rows = [{
            "sim_id":                      self._sim_id,
            "tick":                        tick,
            "item_id":                     item_id,
            "supplier_id":                 supplier_id,
            "stock_on_hand_at_decision":   state.stock_on_hand,
            "stock_in_transit_at_decision": state.stock_in_transit,
            "decision":                    decision,
            "order_qty":                   order_qty,
            "order_id":                    order_id,
            "agent_reasoning":             reasoning,
            "agent_version":               self._agent.agent_version(),
        }]
        self._spark.createDataFrame(rows, schema=_DECISIONS_SCHEMA.strip()) \
            .write.mode("append").saveAsTable(_DECISIONS_TABLE)

    def _check_budget_events(self, tick: int) -> None:
        '''Fire BUDGET_WARNING and BUDGET_EXHAUSTED events as needed.'''
        if self._remaining_budget is None:
            return

        budget_limit      = self._config.budget_limit
        warning_threshold = self._config.budget_warning_threshold

        if self._remaining_budget == 0.0:
            self._logger.budget_exhausted(tick=tick, remaining_budget=0.0)
            return

        if (
            not self._budget_warning_fired
            and self._remaining_budget < budget_limit * warning_threshold
        ):
            self._logger.budget_warning(
                tick             = tick,
                remaining_budget = self._remaining_budget,
                budget_limit     = budget_limit,
                threshold        = warning_threshold,
            )
            self._budget_warning_fired = True

    def _validate_decisions(
        self,
        decisions: list[ReorderDecision],
        context:   AgentContext,
    ) -> None:
        '''
        Ensure the following:
        - The agent returned exactly one decision per item
        - Order quantities are within bounds

        ---

        PARAMETERS:
        - `decisions` (list[ReorderDecision]): List of agent decisions for this tick (each ReorderDecision instance encapsulates the agent's decision for one item type in one tick)
        - `context` (AgentContext): The complete read-only snapshot delivered to the agent at sub-step 4 of each tick, after arrivals (3a) and demand depletion (3b)

        NOTE: See module docstring for the reference for the simulation loop steps and sub-steps.

        RETURNS:
        - None
        '''
        
        decided_items = {d.item_id for d in decisions}
        expected      = set(context.items())

        missing = expected - decided_items
        if missing:
            raise ValueError(
                f"Agent returned no decision for item(s): {sorted(missing)}"
            )

        for d in decisions:
            if not d.is_reorder:
                continue
            item = self._world.items[d.item_id]
            if not (item.min_order_qty <= d.order_qty <= item.max_order_qty):
                raise ValueError(
                    f"Agent decision for {d.item_id}: order_qty={d.order_qty} "
                    f"violates [{item.min_order_qty}, {item.max_order_qty}]"
                )