'''
warehouse_sim/agent/llm_agent_wrapper.py

LLM Agent Wrapper Object (LLMAgentWrapper) - Stage 5: monitoring loop only.

This file implements the sync, tick-bound half of LLMAgentWrapper.decide():
- Assemble QueueMessage from the current AgentContext
- Push to the in-process queue (collections.deque, capped at queue_size)
- Write evaluation metrics to hist_eval_metrics
- Return _last_committed (hold-all until the executor produces a result)

The executor half (background thread, shared result slot, StubLLMAgent,
pre-flight validation, fallback routing) is added in Stage 6. A clearly
marked # STAGE 6 block in decide() shows exactly where it will be inserted.
The class is fully runnable at this stage - wirable into the runner and
returning valid decisions every tick - so the monitoring loop can be tested
in isolation before the async complexity is introduced.

---

DEPENDENCY FLAGS:

[DEP-1] EventLogger.agent_error()
    Called in runner.py's resilience wrap (Stage 3) when the agent raises an
    unhandled exception. This method does not yet exist on EventLogger - it
    must be added before the Stage 3 change can be tested end-to-end. The
    addition follows the same pattern as every other event method on the
    logger (same payload structure as FALLBACK_STRUCTURAL / FALLBACK_LOGICAL
    defined in Stage 6).

[DEP-2] SimWorld.suppliers (min_lead_time resolution)
    __init__ resolves context_obsolescence_threshold_k=None to the minimum
    base_lead_time_ticks across all suppliers in the world. This reads
    world.suppliers, which is a dict[str, Supplier] based on the runner's
    usage pattern. If the SimWorld structure differs, the resolution logic
    in __init__ must be updated accordingly.

[DEP-3] hist_eval_metrics table (Stage 4)
    _write_eval_metrics writes to hackathon_of_the_century.tables4hist.hist_eval_metrics.
    This table must exist before the LLMAgentWrapper is run. It is created in Stage 4
    (setup4dataStore.py). Running the LLMAgentWrapper before Stage 4 is complete will
    raise a table-not-found error on the first tick.

[DEP-4] SparkSession
    _write_eval_metrics needs a SparkSession to write to Delta. The runner
    holds self._spark but does not pass it to agent.decide(). Two options:
      (a) Inject SparkSession into LLMAgentWrapper.__init__ (chosen here)
      (b) Write metrics via a side-channel outside decide()
    Option (a) is consistent with the runner's own pattern (spark injected at
    construction) and keeps decide() side-effect-free with respect to its
    caller. The SparkSession is stored as self._spark.
'''

from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

from ..config.models import SimWorld
from ..event_log.event_log import EventLogger
from .base import AgentContext, BaseAgent, ReorderDecision
from ..config import LLMAgentWrapperConfig
from .llm_agent_wrapper_types import ExecutorResult, QueueMessage
from .rule_based_agent import RuleBasedAgent

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Table reference
# ---------------------------------------------------------------------------

_CATALOG             = "hackathon_of_the_century"
_EVAL_METRICS_TABLE  = f"{_CATALOG}.tables4hist.hist_eval_metrics"
_EVAL_METRICS_SCHEMA = "sim_id STRING, tick INT, item_id STRING, metric_name STRING, metric_value DOUBLE, logged_at TIMESTAMP"


# ---------------------------------------------------------------------------
# LLMAgentWrapper
# ---------------------------------------------------------------------------

class LLMAgentWrapper(BaseAgent):
    '''
    LLM Agent Wrapper Object (LLMAgentWrapper).

    Implements the BaseAgent contract.
    
    From the runner's perspective this is just another agent - it is never aware of the:
    - Monitoring loop
    - Queue
    - Executor thread
    - LLM call inside

    NOTE (with respect to development approach as documented in __docs__/reasoningIntegrationDevelopmentApproach-3.md):
    - Stage implements the monitoring loop only
    - The executor half is added in Stage 6 in the clearly marked `# STAGE 6` block inside `decide()`

    ---

    # Fields
    
    - `spark` (SparkSession): Active SparkSession (required to write to the table "hist_eval_metrics"; see [DEP-4] in module docstring
    - `world` (SimWorld): Fully loaded SimWorld. Used to:
        - Resolve `context_obsolescence_threshold_k=None` to `min_lead_time`; see [DEP-2]
        - Initialise `_last_committed` with hold decisions for all items, so `decide()` returns a valid list from tick 0 without a None check
    - `config` (LLMAgentWrapperConfig): LLMAgentWrapperConfig instance
        - If context_obsolescence_threshold_k is None, it is resolved here and stored as self._resolved_k
        - The resolved value is what must be logged to MLflow - not None
    - `logger` (EventLogger): EventLogger instance for this sim run
        - Used in Stage 6 (see __docs/reasoningIntegrationDevelopmentApproach-3.md) to log `FALLBACK_STRUCTURAL`, `FALLBACK_LOGICAL` events
        - Stored here so it is available without being passed through `decide()`
    '''

    def __init__(
        self,
        spark:  "SparkSession",
        world:  SimWorld,
        config: LLMAgentWrapperConfig,
        logger: EventLogger,
    ) -> None:
        self._spark  = spark
        self._world  = world
        self._config = config
        self._logger = logger

        # ------------------------------------------------------------------
        # Resolve `context_obsolescence_threshold_k`
        #
        # - If not explicitly set, K defaults to the minimum base_lead_time_ticks across all suppliers in the world
        # - A context containing PendingOrders whose expected_arrival_tick has already passed is unreliable for the LLM reasoner
        # - Using the shortest lead time as K ensures we never act on a context that is at minimum one full lead-time cycle stale
        #
        # NOTE:
        # - The resolved value is stored as self._resolved_k and must be logged to MLflow at run start - never the raw None.
        # - See [DEP-2] for `SimWorld.suppliers` structure assumption
        # ------------------------------------------------------------------
        if config.context_obsolescence_threshold_k is not None:
            self._resolved_k: int = config.context_obsolescence_threshold_k
        else:
            min_lead_time = min(
                s.base_lead_time_ticks
                for s in world.suppliers.values()
            )
            self._resolved_k = min_lead_time

        # ------------------------------------------------------------------
        self._queue: deque[QueueMessage] = deque(maxlen=config.queue_size)
        '''
        _queue
        
        - collections.deque with maxlen=queue_size. maxlen handles the size cap automatically
        - When the deque is full and a new message is appended, the oldest message is silently evicted
        - No manual size management is needed
        
        NOTE: The drain logic in Stage 6 (see __docs/reasoningIntegrationDevelopmentApproach-3.md) is implemented in full regardless of queue_size, so that larger values work without code changes.
        '''

        # ------------------------------------------------------------------
        self._last_committed: list[ReorderDecision] = [
            ReorderDecision(
                item_id   = item_id,
                order_qty = 0,
                reasoning = "LLMAgentWrapper initialising - holding until first executor result.",
            )
            for item_id in world.items
        ]
        '''
        _last_committed
        
        - Initialised at construction time as hold decisions for every item in the world, rather than None
        - This means decide() always returns a valid list[ReorderDecision] from tick 0 - no None check needed in the hot path, and the runner never receives an empty list
        '''

        # ------------------------------------------------------------------
        # Shared state for executor (Stage 6 (see __docs__/reasoningIntegrationDevelopmentApproach-3.md))
        # 
        # - Declared here so the full class shape is visible at Stage 5 (see _docs__/reasoningIntegrationDevelopmentApproach-3.md)
        # - Both are written exclusively by the executor thread and read exclusively by the sync side of `decide()` - see Stage 6 for thread-safety treatment
        self._result_slot:   Optional[ExecutorResult] = None   # written by executor thread
        self._executor_busy: bool                     = False  # True from dispatch until slot write

        # ------------------------------------------------------------------
        self._fallback_agent = RuleBasedAgent()
        '''
        Fallback agent
        
        - Used by the executor when the LLM response is structurally or logically invalid
        - Instantiated here (not in the executor thread) so it is shared and not re-created per invocation
        '''

    # ------------------------------------------------------------------
    # BaseAgent contract
    # ------------------------------------------------------------------

    @staticmethod
    def agent_version() -> str:
        return "llm_agent_wrapper_v1"

    def decide(self, context: AgentContext) -> list[ReorderDecision]:
        '''
        Called by the runner on every tick. Never blocks.

        Stage 5 behaviour:
          [1] Run the monitoring loop (assemble QueueMessage, push to queue,
              write eval metrics).
          [2] # STAGE 6 block: executor dispatch and result slot consumption.
          [3] Return _last_committed.

        The runner receives a valid list[ReorderDecision] on every tick.
        Until the executor produces a result (Stage 6), this is always the
        hold-all initialised in __init__.
        '''

        # ------------------------------------------------------------------
        # [1] Monitoring loop
        # ------------------------------------------------------------------
        message = self._build_queue_message(context)
        self._queue.append(message)
        self._write_eval_metrics(context)

        # ------------------------------------------------------------------
        # [2] Executor loop
        # 
        # [STAGE 6] (See __docs__/reasoningIntegrationDevelopmentApproach-3.md) Executor dispatch and result slot consumption
        #
        # This block will contain:
        #   (a) Consume _result_slot if populated: update _last_committed, clear slot, mark executor idle
        #   (b) Check trigger condition (tick % N == 0) and executor idle: snapshot queue, dispatch executor thread, mark executor busy
        #
        # NOTE: At Stage 5 (see __docs__/reasoningIntegrationDevelopmentApproach-3.md) this block is intentionally empty - the monitoring loop above is proven in isolation before async complexity is introduced.
        # ------------------------------------------------------------------

        # TODO

        # ------------------------------------------------------------------
        # [3] Return last committed decisions
        # ------------------------------------------------------------------
        return self._last_committed

    # ------------------------------------------------------------------
    # Monitoring loop helpers
    # ------------------------------------------------------------------

    def _build_queue_message(self, context: AgentContext) -> QueueMessage:
        '''
        Assemble a QueueMessage from the current AgentContext.

        NOTE:
        - `trigger_condition_met` is evaluated here (in the sync monitoring loop) rather than in the executor
        - This means the queue accurately reflects the monitoring loop's view at assembly time, regardless of when the executor drains the queue
        
        ---

        PARAMETERS:
        - `context` (AgentContext): AgentContext instance encapsulating the context necessary for reasoning
        
        RETURNS:
        - (QueueMessage): Wraps agent context into a queue message for future reference of the executor
        '''

        trigger_condition_met = (
            context.tick % self._config.executor_trigger_every_n_ticks == 0
        )

        return QueueMessage(
            trigger_tick           = context.tick,
            trigger_condition_met  = trigger_condition_met,
            assembly_timestamp     = datetime.now(tz=timezone.utc),
            obsolescence_threshold = self._resolved_k,
            context                = context,
            # sim_id is copied from context to the envelope so Her Majesty Reshma the Boss can attach it to LangFuse traces on the executor side without unpacking the full context. See llm_agent_wrapper_types.py.
            sim_id                 = context.sim_id,
        )

    def _write_eval_metrics(self, context: AgentContext) -> None:
        '''
        Write evaluation metrics to hist_eval_metrics for this tick.

        NOTE:
        - This is the evaluation tool call placeholder described in the LLMAgentWrapper design
        - The method is embedded in the monitoring loop with clea comments so that evaluation calls are trivially extractable into a separate loop later \n
          ... this will matter when Her Majesty Reshma the Boss instruments LangFuse around them
        - Metric computations are stubbed as TODOs
        - The write infrastructure (schema, table reference, Spark append) is implemented so that:
            - The table "hist_eval_metrics" receives rows on every tick
            - The table can be read by pull consumers immediately, even before the metric values are finalised
        
        ---

        PARAMETERS:
        - `context` (AgentContext): AgentContext instance encapsulating the context necessary for reasoning

        RETURNS:
        - None
        '''

        # ############ EVALUATION TOOL CALL BOUNDARY - START ############

        # (Extract everything between these markers into a separate loo when LangFuse instrumentation is added)

        # See [DEP-3]: hist_eval_metrics must exist (Stage 4 (see __docs__/reasoningIntegrationDevelopmentApproach-3.md)) before this runs.

        logged_at = datetime.now(tz=timezone.utc)
        rows = []

        # -- Item-level metrics (one row per metric per item) --------------

        for item_id, item_state in context.item_states.items():

            # TODO: replace stub values with computed metrics
            # Stub values of 0.0 are placeholders that keep the write
            # pipeline exercisable before metric logic is finalised.

            # stockout_rate: fraction of demand unmet over the history window.
            # Source: context.demand_history[item_id] - sum(unmet) / sum(disrupted_demand)
            rows.append(_metric_row(
                sim_id     = context.sim_id,
                tick       = context.tick,
                item_id    = item_id,
                name       = "stockout_rate",
                value      = 0.0,   # TODO
                logged_at  = logged_at,
            ))

            # holding_cost_delta: change in cumulative holding cost since last tick.
            # Source: context.cost_snapshots[item_id].cumulative_holding_cost
            rows.append(_metric_row(
                sim_id     = context.sim_id,
                tick       = context.tick,
                item_id    = item_id,
                name       = "holding_cost_delta",
                value      = 0.0,   # TODO
                logged_at  = logged_at,
            ))

            # stock_cover: stock_on_hand / mean_demand_last_window_ticks.
            # Proxy for how many ticks of demand the current stock can cover.
            # Source: item_state.stock_on_hand, context.demand_history[item_id]
            rows.append(_metric_row(
                sim_id     = context.sim_id,
                tick       = context.tick,
                item_id    = item_id,
                name       = "stock_cover",
                value      = 0.0,   # TODO
                logged_at  = logged_at,
            ))

        # -- Run-level metrics (item_id = None) ----------------------------

        # budget_utilisation: fraction of budget_limit spent so far.
        # Source: context.remaining_budget, world.config.budget_limit
        rows.append(_metric_row(
            sim_id    = context.sim_id,
            tick      = context.tick,
            item_id   = None,
            name      = "budget_utilisation",
            value     = 0.0,   # TODO
            logged_at = logged_at,
        ))

        # ############ EVALUATION TOOL CALL BOUNDARY - END ############

        # Write all rows for this tick in a single Spark operation.
        # One append per tick (not per row) to minimise write overhead.
        if rows:
            (
                self._spark
                .createDataFrame(rows, schema=_EVAL_METRICS_SCHEMA)
                .write
                .mode("append")
                .saveAsTable(_EVAL_METRICS_TABLE)
            )


# ---------------------------------------------------------------------------
# Module-level helper
# ---------------------------------------------------------------------------

def _metric_row(
    sim_id:    str,
    tick:      int,
    item_id:   Optional[str],
    name:      str,
    value:     float,
    logged_at: datetime,
) -> dict:
    '''Build a single hist_eval_metrics row dict.'''
    return {
        "sim_id":       sim_id,
        "tick":         tick,
        "item_id":      item_id,
        "metric_name":  name,
        "metric_value": value,
        "logged_at":    logged_at,
    }