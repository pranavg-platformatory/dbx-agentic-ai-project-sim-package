'''
warehouse_sim/agent/llm_agent_wrapper.py

LLM Agent Wrapper Object (LLMAgentWrapper) - Stage 6: executor thread and shared result slot.

NOTE: For documentation on stages, see __docs__/reasoningIntegrationDevelopmentApproach-3.md.

Extends Stage 5 (monitoring loop) with the async executor half:
- Shared result slot (`_result_slot`, `_executor_busy`, `_last_committed`)
- Trigger condition check and executor thread dispatch (queue snapshot on dispatch)
- Executor thread: drain logic, LLMReorderAgent call (or StubLLMAgent for testing),
  pre-flight validation, fallback routing, ExecutorResult write to slot
- Slot consumption on the sync side at the top of each `decide()` call
- StubLLMAgent with three modes (`valid`, `structural_fail`, `logical_fail`)

Stage 5 summary (monitoring loop - unchanged):
- Assemble QueueMessage from the current AgentContext each tick
- Push to the in-process queue (collections.deque, capped at queue_size)
- Write evaluation metrics to hist_eval_metrics
- Return _last_committed (hold-all until the executor produces a result)

---

LLMReorderAgent integration (stub_mode=None path):
- LLMReorderAgent (Her Majesty Reshma the Boss's package) is instantiated once in __init__
- In _run_executor, its decide(context) is called in place of the former NotImplementedError
- decide() returns list[ReorderDecision] directly; _validate_structural accepts this as-is
- LLMAgentWrapper remains the agent the runner knows about; LLMReorderAgent is an internal detail

---

DEPENDENCY FLAGS:

[DEP-1] `EventLogger.agent_error()`:
- Called in runner.py's resilience wrap (Stage 3) when the agent raises an unhandled exception
- This method does not yet exist on EventLogger - it must be added before the Stage 3 change can be tested end-to-end
- The addition follows the same pattern as every other event method on the logger (same payload structure as `FALLBACK_STRUCTURAL` / `FALLBACK_LOGICAL` defined in Stage 6)

[DEP-2] SimWorld.suppliers (`min_lead_time` resolution):
- `__init__` resolves `context_obsolescence_threshold_k`=None to the minimum `base_lead_time_ticks` across all suppliers in the world
- This reads world.suppliers, which is a dict[str, Supplier] based on the runner's usage pattern
- If the SimWorld structure differs, the resolution logic in `__init__` must be updated accordingly

[DEP-3] "hist_eval_metrics" table (Stage 4):
- _write_eval_metrics writes to hackathon_of_the_century.tables4hist.hist_eval_metrics
- This table must exist before the LLMAgentWrapper is run
- It is created in Stage 4 (_dataStoreDefinition/setup4dataStore.py)
- Running the LLMAgentWrapper before Stage 4 is complete will raise a table-not-found error on the first tick

[DEP-4] SparkSession:
- _write_eval_metrics needs a SparkSession to write to Delta
- The runner holds `self._spark` but does not pass it to `agent.decide()`
- 2 options:
    (a) Inject SparkSession into `LLMAgentWrapper.__init__` (chosen here)
    (b) Write metrics via a side-channel outside `decide()`
- Option (a) is consistent with the runner's own pattern (spark injected at construction) and keeps decide() side-effect-free with respect to its caller
- The SparkSession is stored as `self._spark`

[DEP-5] LLMReorderAgent package on the Python path:
- LLMReorderAgent is imported at the top of this module from `llm_reorder_agent.llm_agent`
- The LLM agent codebase (test_reorder_llm_agent) must be on sys.path before LLMAgentWrapper is instantiated
- In a Databricks notebook this is typically: sys.path.insert(0, '/Workspace/Shared/reorder-llm-agent')
- LLMReorderAgent is only instantiated when stub_mode=None; an ImportError here will surface at __init__ time, not at _run_executor time, making it fail-fast and easy to diagnose
'''

from __future__ import annotations

import threading
from collections import deque
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from ..config.models import SimWorld
from ..event_log.event_log import EventLogger
from .base import AgentContext, BaseAgent, ReorderDecision
from ..config import LLMAgentWrapperConfig
from .llm_agent_wrapper_types import ExecutorResult, QueueMessage
from .rule_based_agent import RuleBasedAgent

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# NOTE: LLMReorderAgent is imported lazily inside __init__ (only when stub_mode=None) so that the module can be imported and stub-mode tests run without the LLM agent package being on the path (see [DEP-5] in the module docstring).
_LLMReorderAgentClass = None  # resolved at first instantiation with stub_mode=None


#################################################
# Table reference
#################################################

_CATALOG            = "hackathon_of_the_century"
_EVAL_METRICS_TABLE = f"{_CATALOG}.tables4hist.hist_eval_metrics"

# FIX: NOT NULL constraint violation for item_id in tables4hist.hist_eval_metrics
#
# Root cause:
# - Spark's string DDL parser marks every column NOT NULL by default
# - The hist_eval_metrics Delta table declares item_id as nullable (no NOT NULL in setup4dataStore.py), but there are 2 issues here:
#   - createDataFrame was enforcing non-nullability at the Spark level before the data even reached the table
#   - item_id is part of the primary key of hist_eval_metrics, which means it cannot be set as nullable
# - The run-level metric row (budget_utilisation) has item_id=None by design - NULL is the signal that a metric is run-level rather than item-level
# - This produced a NOT NULL constraint violation on every tick
# 
# Resolution:
# - createDataFrame side:
#   - StructType with nullable=True on item_id matches the actual table DDL and allows NULL to pass through createDataFrame correctly
#   - All other columns are nullable=False, consistent with their NOT NULL declarations in the DDL
#   - NOTE: This will only be effective if item_id is removed as a child column in the primary key in the future
# - Sentinel value (actual fix, considering that item_id is part of the primary key) (implemented in _write_eval_metrics):
#   - If item_id is None, ensure it becomes a string
#   - Still, the above string should indicate that the metric is run-level, not item-level
_EVAL_METRICS_SCHEMA = StructType([
    StructField("sim_id",       StringType(),    nullable=False),
    StructField("tick",         IntegerType(),   nullable=False),
    StructField("item_id",      StringType(),    nullable=True),
    StructField("metric_name",  StringType(),    nullable=False),
    StructField("metric_value", DoubleType(),    nullable=False),
    StructField("logged_at",    TimestampType(), nullable=False),
])

#################################################
# LLMAgentWrapper
#################################################

class LLMAgentWrapper(BaseAgent):
    '''
    LLM Agent Wrapper Object (LLMAgentWrapper).

    Implements the BaseAgent contract.
    
    From the runner's perspective this is just another agent - it is never aware of the:
    - Monitoring loop
    - Queue
    - Executor thread
    - LLM call inside

    KEY POINTS:
    - The real LLM call is made by LLMReorderAgent (Her Majesty Reshma the Boss's codebase)
    - LLMReorderAgent nstantiated once in `__init__` and called inside `_run_executor`
    - LLMAgentWrapper remains the sole agent the runner interacts with; LLMReorderAgent is an internal detail

    NOTE (with respect to development approach as documented in __docs__/reasoningIntegrationDevelopmentApproach-3.md):
    - Stage 5 implements the monitoring loop only
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

        #------------------------
        # Resolve `context_obsolescence_threshold_k`
        #
        # - If not explicitly set, K defaults to the minimum base_lead_time_ticks across all suppliers in the world
        # - A context containing PendingOrders whose expected_arrival_tick has already passed is unreliable for the LLM reasoner
        # - Using the shortest lead time as K ensures we never act on a context that is at minimum one full lead-time cycle stale
        #
        # NOTE:
        # - The resolved value is stored as self._resolved_k and must be logged to MLflow at run start - never the raw None.
        # - See [DEP-2] for `SimWorld.suppliers` structure assumption
        #------------------------
        if config.context_obsolescence_threshold_k is not None:
            self._resolved_k: int = config.context_obsolescence_threshold_k
        else:
            min_lead_time = min(
                s.base_lead_time_ticks
                for s in world.suppliers.values()
            )
            self._resolved_k = min_lead_time

        #------------------------
        self._queue: deque[QueueMessage] = deque(maxlen=config.queue_size)
        '''
        _queue
        
        - collections.deque with maxlen=queue_size. maxlen handles the size cap automatically
        - When the deque is full and a new message is appended, the oldest message is silently evicted
        - No manual size management is needed
        
        NOTE: The drain logic in Stage 6 (see __docs/reasoningIntegrationDevelopmentApproach-3.md) is implemented in full regardless of queue_size, so that larger values work without code changes.
        '''

        #------------------------
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

        #------------------------
        # Shared state for executor (Stage 6 (see __docs__/reasoningIntegrationDevelopmentApproach-3.md))
        # 
        # - Declared here so the full class shape is visible at Stage 5 (see _docs__/reasoningIntegrationDevelopmentApproach-3.md)
        # - Both are written exclusively by the executor thread and read exclusively by the sync side of `decide()` - see Stage 6 for thread-safety treatment
        self._result_slot:   Optional[ExecutorResult] = None   # written by executor thread
        self._executor_busy: bool                     = False  # True from dispatch until slot write

        #------------------------
        # Thread-safety lock for shared result slot
        #
        # CONCEPTUAL SIDE NOTE:
        # - GIL = Global Interpretor Lock, a mutex ensuring only one thread executes Python bytecode at a time.
        # - PyPy with Software Transactional Memory (STM) is an experimental version of the PyPy interpreter designed to run multi-threaded Python programs on multiple CPU cores simultaneously by removing the Global Interpreter Lock (GIL)
        #
        # Why use thread-safety lock:
        # - _result_slot and _executor_busy are written by the executor thread and read by the sync side of decide()
        # - In CPython, simple attribute assignment is GIL-protected for basic types, making bare reads/writes technically safe in practice
        # - However, relying on GIL behaviour is an implementation detail, not a language guarantee
        #   ... Code that runs correctly only because of the GIL is fragile and non-portable!
        #
        # Hence:
        # - A threading.Lock makes the intent explicit and ensures correctness outside CPython (e.g. Jython, PyPy with STM)
        # - The lock is held only for the read-then-clear or write operations on the slot - never across the LLM call - so contention is negligible
        #------------------------
        self._slot_lock: threading.Lock = threading.Lock()

        #------------------------
        self._fallback_agent = RuleBasedAgent()
        '''
        Fallback agent
        
        - Used by the executor when the LLM response is structurally or logically invalid
        - Instantiated here (not in the executor thread) so it is shared and not re-created per invocation
        '''

        #------------------------
        # LLMReorderAgent - the real LLM call (stub_mode=None only)
        #
        # - Instantiated here rather than in _run_executor so:
        #   (a) Import and initialisation failures (missing package, bad config.yml, LLM endpoint unreachable at startup) surface immediately at construction time, not silently inside the background thread on the first trigger tick
        #   (b) The object is shared across all executor invocations without being re-created each time (LLMReorderAgent.__init__ builds the LangGraph graph and binds tools, which is non-trivial)
        # - When stub_mode is not None, this is None and never called
        # - See [DEP-5] in the module docstring for path requirements
        #------------------------
        if config.stub_mode is None:
            import sys
            sys.path.insert(0, '/Workspace/Shared/reorder-llm-agent')

            global _LLMReorderAgentClass
            if _LLMReorderAgentClass is None:
                try:
                    from llm_agent import LLMReorderAgent as _LLMReorderAgentClass
                except ImportError as e:
                    raise ImportError(
                        "LLMReorderAgent could not be imported. "
                        "Ensure the LLM agent package is on sys.path before constructing "
                        "LLMAgentWrapper with stub_mode=None. "
                        "Typically: sys.path.insert(0, '/Workspace/Shared/reorder-llm-agent'). "
                        f"Original error: {e}"
                    ) from e

            #------------------------
            # Write-tool suppression (to address point 3 - duplicate hist_reorder_decisions writes (see "Integration Points with Her Majesty Reshma the Boss's Package" in devlog.md))
            #
            # LLMReorderAgent's tool list (uc_tools.ALL_TOOLS) includes log_agent_decision and escalate_item. When running inside the simulation:
            # - log_agent_decision writes to hist_reorder_decisions...
            #   - The runner's _write_decision_row() also writes to hist_reorder_decisions for every decision
            #   - Both write rows with the same (sim_id, tick, item_id) PK
            #   - Because the table is append-only (delta.appendOnly=true), Delta does not enforce the PK constraint on write - duplicate rows accumulate silently
            # - escalate_item writes to ops_escalation_queue...
            #   - The runner has no awareness of this table and does not read from it, so its writes are not duplicated
            #   - However, suppressing it here is consistent: inside the simulation the runner owns all table writes that affect the sim state, and the escalation queue is a production-facing concern that does not need to be populated during testing
            #
            # Resolution:
            # - Temporarily replace ALL_TOOLS with a filtered list before LLMReorderAgent.__init__ calls _build_agent_graph() (which reads ALL_TOOLS at construction time). The original list is restored immediately after
            # - This is safe because __init__ is called from the main thread before any executor threads are running, so there is no concurrency risk on the module-level list
            #
            # NOTE:
            # - The LLM's system prompt still instructs it to call log_agent_decision
            # - With the tool absent from the bound list, the LLM will either skip it or produce a tool-not-found message that is handled by tools_node without halting the loop
            # - The reasoning and decision still flow through correctly
            #------------------------
            _WRITE_TOOL_NAMES_TO_EXCLUDE = {"log_agent_decision"}

            if config.suppress_write_tools:
                import uc_tools as _uc_tools
                _original_tools   = _uc_tools.ALL_TOOLS
                _uc_tools.ALL_TOOLS = [
                    t for t in _original_tools
                    if t.name not in _WRITE_TOOL_NAMES_TO_EXCLUDE
                ]

            try:
                self._llm_agent = _LLMReorderAgentClass(
                    config_override=config.llm_agent_config_override,
                )
            finally:
                if config.suppress_write_tools:
                    _uc_tools.ALL_TOOLS = _original_tools  # always restore, even on error

        else:
            self._llm_agent = None  # stub path; _run_executor uses _StubLLMAgent instead

    #====================================
    # BaseAgent contract
    #====================================

    @staticmethod
    def agent_version() -> str:
        return "llm_agent_wrapper_v1"

    def decide(self, context: AgentContext) -> list[ReorderDecision]:
        '''
        Called by the runner on every tick. Never blocks.

        Stage 5 behaviour:
          [1] Run the monitoring loop (assemble QueueMessage, push to queue, write eval metrics)
          [2] `# STAGE 6` block: executor dispatch and result slot consumption
          [3] Return _last_committed

        NOTE 1: For documentation on stages, see __docs__/reasoningIntegrationDevelopmentApproach-3.md.

        NOTE 2:
        - The runner receives a valid list[ReorderDecision] on every tick
        - Until the executor produces a result (Stage 6), this is always the hold-all initialised in __init__

        NOTE 3: sync side vs. async side:
        - `.decide()` has 2 components:
            - sync side (monitoring + executor condition checking), the code that runs synchronously with the simulation ticks
            - async side (executor), the code that runs as a separate thread
        - The sync side regularly checks for the completion of the async side and provides results in-sync with simulation ticks once it receives the async side's result
        
        ---

        PARAMETERS:
        - `context` (AgentContext): AgentContext instance encapsulating the context necessary for reasoning
        
        RETURNS:
        - (list[ReorderDecision]): 1 decision per item in `context.items()` \n
          NOTE: The engine will raise an exception if an item is missing from the returned list
        '''

        #------------------------
        # [1] Monitoring loop
        #------------------------
        message = self._build_queue_message(context)
        self._queue.append(message)
        self._write_eval_metrics(context)

        #------------------------
        # [2] Executor loop
        #------------------------

        # (a) Consume result slot
        #
        # - Slot consumption happens BEFORE the trigger check
        # - If a result arrived while the executor was running, it must be picked up and committed before deciding whether to dispatch the executor again
        # - Otherwise the executor could be dispatched a second time based on a stale _last_committed value
        #
        # NOTE: The lock is acquired only for the read-then-clear operation, not across the LLM call; this keeps contention negligible
        with self._slot_lock:
            pending = self._result_slot
            if pending is not None:
                self._result_slot   = None
                self._executor_busy = False

        if pending is not None:
            # Commit outside the lock - _last_committed is only ever written by the sync side of decide(), so no race here.
            self._last_committed = pending.decisions

        # (b) Trigger check and executor dispatch
        # 
        # NOTE:
        # - If the executor is still running when the trigger fires, the tick is skipped silently
        # - The monitoring loop has continued queuing messages, so the executor will have a fresh context to drain to on the next dispatch
        #
        # [PREVIOUS] Conditions for dispatch:
        # - Trigger condition met (tick % N == 0)
        # - Executor is not already busy
        #
        # [CURRENT] Conditions for dispatch:
        # - tick > 0 guard: tick % N == 0 is True for ANY N at tick 0
        # - So without this guard the executor would always dispatch on the very first tick regardless of the configured interval
        # 
        # NOTE ON [CURRENT] VS [PREVIOUS]:
        # - Tick 0 has no meaningful state to reason over (no demand drawn, no arrivals processed)
        # - The first eligible dispatch is at tick N
        trigger_met = (
            context.tick > 0
            and context.tick % self._config.executor_trigger_every_n_ticks == 0
        )

        if trigger_met and not self._executor_busy:
            # KEY NOTES:
            # - Snapshot the queue at dispatch time
            # - The executor works on this stable copy - independent of what the monitoring loop appends to self._queue on subsequent ticks while the executor runs
            #
            # OTHER NOTES:
            # - list() on a deque is O(n) and thread-safe in CPython (the GIL protects the deque read)
            # - The lock is NOT used here because the deque is only ever appended to by the sync side - the executor never writes to it, so there is no write-write race
            
            queue_snapshot = list(self._queue)

            with self._slot_lock:
                self._executor_busy = True

            thread = threading.Thread(
                target = self._run_executor,
                args   = (queue_snapshot, context.tick),
                daemon = True,
                # NOTE: daemon=True: executor will not prevent process exit if the simulation ends while it is still running. Acceptable for simulation; revisit for PROD.
            )
            thread.start()

        #------------------------
        # [3] Return last committed decisions
        #------------------------
        return self._last_committed

    #====================================
    # Monitoring loop helpers
    #====================================

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

    #====================================
    # Executor thread
    #====================================

    def _run_executor(
        self,
        queue_snapshot: list[QueueMessage],
        current_tick:   int,
    ) -> None:
        '''
        - Executor thread entry point
        - Runs asynchronously - never called by the sync side of decide() directly, only via threading.Thread

        Steps:
        1. Drain queue_snapshot to the latest non-outdated QueueMessage.
        2. Call real LLM (or StubLLMAgent for testing) with the context.
        3. Structural pre-flight validation.
        4. Logical pre-flight validation (if structural passed).
        5. Apply RuleBasedAgent fallback if either check fails.
        6. Write ExecutorResult to `_result_slot`; mark executor idle.

        On all-outdated queue: log EXECUTOR_ALL_STALE event and return without writing to `_result_slot. _last_committed` is unchanged - holding is correct when every available context is stale.
        
        ---

        PARAMETERS:
        - `queue_snapshot` (list): Snapshot (local point-in-time copy) of the queue
        - `current_tick` (int): Current tick number

        RETURNS:
        - None
        '''

        # Step 1: Drain to latest non-outdated QueueMessage
        #
        # Iterate newest-to-oldest (reversed). A message assembled at tick T is stale if: current_tick - T > obsolescence_threshold (K).
        #
        # NOTE:
        # - We want the LATEST non-outdated message
        # - Newest-first means the first non-outdated message encountered is the correct one
        # - The drain logic is in full regardless of queue_size so larger sizes work without code changes
        # - With queue_size=1 this is trivially always the only message
        chosen = None
        for msg in reversed(queue_snapshot):
            if (current_tick - msg.trigger_tick) <= msg.obsolescence_threshold:
                chosen = msg
                break

        if chosen is None:
            # CASE: Every message is stale:
            # - Log a warning event so post-run analysis can identify ticks where the executor had nothing actionable
            # - Writing nothing to _result_slot is correct - acting on a stale context is worse than holding
            self._logger.executor_all_stale(
                event_type = "EXECUTOR_ALL_STALE",
                tick       = current_tick,
                queue_size = len(queue_snapshot),
                oldest_tick = queue_snapshot[0].trigger_tick  if queue_snapshot else None,
                newest_tick = queue_snapshot[-1].trigger_tick if queue_snapshot else None,
            )
            with self._slot_lock:
                self._executor_busy = False
            return

        context = chosen.context

        # Step 2: LLM call (or StubLLMAgent for testing)
        #
        # Two paths, selected by stub_mode in LLMAgentWrapperConfig:
        #
        # stub_mode is not None → _StubLLMAgent.respond() stands in for the LLM call.
        #   Returns list[ReorderDecision] directly (valid/logical_fail modes) or a raw
        #   string (structural_fail mode). Pre-flight validation below runs in both cases.
        #
        # stub_mode is None → self._llm_agent.decide(context) calls LLMReorderAgent.
        #   LLMReorderAgent runs the full LangGraph loop (LLM call + tool calls) and
        #   returns list[ReorderDecision]. Its internal _parse_llm_decisions already
        #   clamps out-of-range quantities and fills missing items with HOLD, so the
        #   list it returns is always structurally valid. _validate_structural will
        #   accept it as-is; _validate_logical is still run as a safety net.
        #
        # NOTE: _run_executor is the ONLY place the LLM call happens. LLMAgentWrapper
        # remains the agent the runner knows about. LLMReorderAgent is an internal detail.
        try:
            if self._config.stub_mode is not None:
                stub = _StubLLMAgent(self._config.stub_mode)
                raw_response = stub.respond(context)
            else:
                # Real LLM call via LLMReorderAgent (Her Majesty Reshma the Boss's package).
                # decide() returns list[ReorderDecision] directly - the same type _validate_structural
                # already accepts from the stub's valid/logical_fail modes.
                raw_response = self._llm_agent.decide(context)

        except Exception as e:
            # Any exception from the LLM call (network failure, LangGraph error, etc.)
            # is caught here so the executor thread can still write a fallback result to
            # _result_slot and clear _executor_busy. Without this, an unhandled exception
            # would leave _executor_busy=True permanently, silently stopping all future
            # executor dispatches for the rest of the simulation run.
            #
            # This also addresses the known gap documented in the devlog:
            # "No finally block in _run_executor - if NotImplementedError is raised
            # (stub_mode=None), _executor_busy is never cleared."
            self._logger.fallback_structural(
                tick         = current_tick,
                raw_response = "",
                error        = f"LLM call raised exception: {type(e).__name__}: {e}",
            )
            with self._slot_lock:
                self._result_slot = ExecutorResult(
                    decisions        = self._fallback_agent.decide(context),
                    produced_at_tick = chosen.trigger_tick,
                    fallback_used    = True,
                    fallback_type    = "FALLBACK_STRUCTURAL",
                )
                self._executor_busy = False
            return

        # Step 3: Structural validation
        #
        # Can the response be parsed into list[ReorderDecision]?
        #
        # - The LLMAgentWrapper's pre-flight validation is the PRIMARY defence
        # - It must intercept invalid responses before they reach the runner
        # - The runner's _validate_decisions raises ValueError with no recovery; it is a last-resort safety net only
        fallback_type = None
        decisions, structural_error = self._validate_structural(raw_response)

        if structural_error is not None:
            self._logger.fallback_structural(
                tick         = current_tick,
                raw_response = str(raw_response),
                error        = structural_error
            )
            fallback_type = "FALLBACK_STRUCTURAL"
            decisions     = self._fallback_agent.decide(context)

        else:
            # Step 4: Logical validation
            #
            # - Structurally valid but semantically wrong responses are a distinct failure mode from parse failures
            # - They get a distinct event type (FALLBACK_LOGICAL vs FALLBACK_STRUCTURAL) so post-run analysis can distinguish them.
            #
            # Checks per decision:
            # - item_id is known in context.item_states
            # - order_qty >= 0
            # - hold (order_qty == 0) is always valid
            # - min_order_qty <= order_qty <= max_order_qty for reorders
            logical_violations = self._validate_logical(decisions, context)

            if logical_violations:
                self._logger.fallback_logical(
                    tick         = current_tick,
                    violations   = logical_violations
                )
                fallback_type = "FALLBACK_LOGICAL"
                decisions     = self._fallback_agent.decide(context)

        # Step 6: Write result and mark idle
        #
        # - Both writes are inside the lock so the sync side always sees a consistent pair:
        #       either (slot=None, busy=True) while running
        #       or (slot=result, busy=False) when done
        # - There is no window where busy=False but slot is still None after a completed execution
        result = ExecutorResult(
            decisions        = decisions,
            produced_at_tick = chosen.trigger_tick,
            fallback_used    = fallback_type is not None,
            fallback_type    = fallback_type,
        )
        with self._slot_lock:
            self._result_slot   = result
            self._executor_busy = False

    #====================================
    # Pre-flight validation helpers
    #====================================

    def _validate_structural(
        self,
        raw_response: object,
    ) -> "tuple[Optional[list[ReorderDecision]], Optional[str]]":
        '''
        Check whether `raw_response` is a list[ReorderDecision]:
        - Returns (decisions, None) on success
        - Returns (None, error_message) on failure

        NOTE: Stub phase:
        - In the stub phase, `valid` and `logical_fail` modes return a list[ReorderDecision] directly (no parsing needed)
        - `structural_fail` mode returns a string
        - In the real LLM phase, this method will also handle JSON parsing of the LLM text response
        
        ---

        PARAMETERS:
        - `raw_response` (object): Raw response


        RETURNS:
        - (list[ReorderDecision], optional): List of reorder decisions (only given on success)
        - (str, optional): Error message (only given on failure)
        '''

        if isinstance(raw_response, list) and all(
            isinstance(d, ReorderDecision) for d in raw_response
        ):
            return raw_response, None
        return None, (
            f"Response is not a list[ReorderDecision]: "
            f"got {type(raw_response).__name__}"
        )

    def _validate_logical(
        self,
        decisions: "list[ReorderDecision]",
        context:   AgentContext,
    ) -> list:
        '''
        Check semantic correctness of a structurally valid decision list.

        NOTE:
        - Returns a list of violation dicts (empty = all valid)
        - Each dict contains enough context for the FALLBACK_LOGICAL event payload

        ---

        PARAMETERS:
        - `decisions` (list[ReorderDecision]): List of reorder decisions given by agent
        - `context`
        '''
        
        violations = []
        for decision in decisions:
            item_state = context.item_states.get(decision.item_id)

            if item_state is None:
                violations.append({
                    "violation_type":  "unknown_item_id",
                    "item_id":         decision.item_id,
                    "offending_value": decision.item_id,
                })
                continue

            if decision.order_qty < 0:
                violations.append({
                    "violation_type":  "negative_order_qty",
                    "item_id":         decision.item_id,
                    "offending_value": decision.order_qty,
                })
                continue

            # Hold (order_qty == 0) is always logically valid.
            if decision.order_qty == 0:
                continue

            if not (item_state.min_order_qty <= decision.order_qty <= item_state.max_order_qty):
                violations.append({
                    "violation_type":  "order_qty_out_of_range",
                    "item_id":         decision.item_id,
                    "offending_value": decision.order_qty,
                    "min_order_qty":   item_state.min_order_qty,
                    "max_order_qty":   item_state.max_order_qty,
                })
        return violations

    #====================================
    # Monitoring loop helpers (Stage 5 - unchanged)
    #====================================

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
            # NOTE: Stub values of 0.0 are placeholders that keep the write pipeline exercisable before metric logic is finalised.

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

        # FIX (part of FIX: NOT NULL constraint violation for item_id in tables4hist.hist_eval_metrics: 
        # Adding a sentinel value for run-level metrics (to avoid a NOT NULL constraint violation)
        item_id = "__run_level__"

        # budget_utilisation: fraction of budget_limit spent so far.
        # Source: context.remaining_budget, world.config.budget_limit
        rows.append(_metric_row(
            sim_id    = context.sim_id,
            tick      = context.tick,
            item_id   = item_id,
            name      = "budget_utilisation",
            value     = 0.0,   # TODO
            logged_at = logged_at,
        ))

        # ############ EVALUATION TOOL CALL BOUNDARY - END ############

        # Write all rows for this tick in a single Spark operation.
        # NOTE: One append per tick (not per row) to minimise write overhead.
        if rows:
            (
                self._spark
                .createDataFrame(rows, schema=_EVAL_METRICS_SCHEMA)
                .write
                .mode("append")
                .saveAsTable(_EVAL_METRICS_TABLE)
            )

#################################################
# _StubLLMAgent (simulation phase only)
#################################################

class _StubLLMAgent:
    '''
    Stands in for the HTTP call to the real LLM during the stub testing phase.

    NOTE:
    - Not a BaseAgent subclass - it does not implement the agent contract
    - It replaces only the LLM call itself inside _run_executor, not the full agent pipeline
    - Pre-flight validation and fallback routing apply to its output exactly as they would to a real LLM response

    3 modes, set via LLMAgentWrapperConfig.stub_mode:

    - "valid"
        - Returns one correct ReorderDecision per item
        - `order_qty` = `min_order_qty` for all items (always passes validation
        - Exercises the happy path: LLM response -> runner
      - "structural_fail"
        - Returns a raw string that cannot be parsed as list[ReorderDecision].
        - Exercises: `FALLBACK_STRUCTURAL` event -> RuleBasedAgent -> runner
      - "logical_fail"
        - Returns a structurally valid list[ReorderDecision] but with order_qty = max_order_qty + 1 for every item, guaranteed to fail the logical range check
        - Exercises: `FALLBACK_LOGICAL` event -> RuleBasedAgent -> runner
    - None: Indicates a real LLM call should be made. _StubLLMAgent should not be instantiated in this case - `_run_executor` routes to the real LLM call instead (not yet implemented
    '''

    def __init__(self, stub_mode: Optional[str]) -> None:
        self._mode = stub_mode

    def respond(self, context: AgentContext) -> object:
        '''
        Return a stub response for the given context.

        NOTE: Return type is deliberately 'object' - structural validation in `_validate_structural` checks whether it is actually list[ReorderDecision].
        
        ---

        PARAMETERS:
        - `context` (AgentContext): AgentContext instance encapsulating the context necessary for reasoning
        
        RETURNS:
        - (object): Response object
        '''

        if self._mode == "valid":
            # min_order_qty is always within [min_order_qty, max_order_qty],
            # so this always passes logical validation.
            return [
                ReorderDecision(
                    item_id   = item_id,
                    order_qty = state.min_order_qty,
                    reasoning = "StubLLMAgent (valid): ordering min_order_qty.",
                )
                for item_id, state in context.item_states.items()
            ]

        elif self._mode == "structural_fail":
            # A raw string - _validate_structural will reject this.
            return "STUB_STRUCTURAL_FAIL: this is not a list[ReorderDecision]."

        elif self._mode == "logical_fail":
            # order_qty = max_order_qty + 1 is guaranteed out-of-range.
            return [
                ReorderDecision(
                    item_id   = item_id,
                    order_qty = state.max_order_qty + 1,
                    reasoning = "StubLLMAgent (logical_fail): order_qty intentionally out of range.",
                )
                for item_id, state in context.item_states.items()
            ]

        else:
            # stub_mode=None should never reach here - _run_executor guards it.
            raise ValueError("_StubLLMAgent instantiated with stub_mode=None. Route to the real LLM call instead.")

#################################################
# Module-level helper
#################################################

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