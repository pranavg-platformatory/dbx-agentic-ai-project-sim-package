# Databricks notebook source
# MAGIC %md
# MAGIC # LLMAgentWrapper - Functional Tests
# MAGIC
# MAGIC **Scope**: Pranav's LLMAgentWrapper implementation (stub phase).
# MAGIC
# MAGIC **Test structure**:
# MAGIC - Tests 1-6: Pure Python - no Spark required. Run anywhere.
# MAGIC - Tests 7-8: Spark required - full runner integration.
# MAGIC
# MAGIC Each test cell prints a clear PASS / FAIL with a short reason. A summary cell at the end collects all results.
# MAGIC
# MAGIC **Files under test**:
# MAGIC - `warehouse_sim/agent/rule_based_agent.py`
# MAGIC - `warehouse_sim/agent/llm_agent_wrapper.py`
# MAGIC - `warehouse_sim/agent/llm_agent_wrapper_types.py`
# MAGIC - `warehouse_sim/config/llm_agent_wrapper_config.py`
# MAGIC - `warehouse_sim/engine/runner.py` (resilience wrap)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Context**:
# MAGIC - [`__docs__/reasoningIntegrationDevelopmentApproach-3.md`](./__docs__/reasoningIntegrationDevelopmentApproach-3.md) - implementation stages
# MAGIC - [`__docs__/reasoningIntegrationDevelopmentApproach-1.md`](./__docs__/reasoningIntegrationDevelopmentApproach-1.md) - implementation approach

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %pip install pydantic numpy matplotlib pandas
# MAGIC %restart_python

# COMMAND ----------

import sys

PACKAGE_ROOT = "/Workspace/Repos/mistermilvusmigrans@gmail.com/dbx-agentic-ai-project-sim-package"
if PACKAGE_ROOT not in sys.path:
    sys.path.insert(0, PACKAGE_ROOT)

print("Python path updated.")

# COMMAND ----------

# Shared test result collector.
# Each test appends (test_id, label, passed, reason).
# The summary cell at the end prints all results.
_results: list[tuple[str, str, bool, str]] = []

def _record(test_id: str, label: str, passed: bool, reason: str = "") -> None:
    status = "✓ PASS" if passed else "✗ FAIL"
    print(f"  {status}  {label}" + (f"\n         {reason}" if reason else ""))
    _results.append((test_id, label, passed, reason))

def _header(test_id: str, title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {test_id}  {title}")
    print(f"{'─' * 60}")

# COMMAND ----------

# Shared imports used across multiple tests
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import MagicMock
import warnings

from warehouse_sim.agent.base import (
    ActiveDisruption,
    AgentContext,
    CostSnapshot,
    DemandRecord,
    ItemState,
    PendingOrder,
    ReorderDecision,
)
from warehouse_sim.agent.rule_based_agent import RuleBasedAgent
from warehouse_sim.agent.llm_agent_wrapper import LLMAgentWrapper
from warehouse_sim.agent.llm_agent_wrapper_types import QueueMessage, ExecutorResult
from warehouse_sim.config.llm_agent_wrapper_config import LLMAgentWrapperConfig

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shared fixtures
# MAGIC
# MAGIC A minimal two-item world and `AgentContext` used across multiple tests.
# MAGIC Hard-coded values are chosen to make assertions readable at a glance.

# COMMAND ----------

# Two items:
#   item_a: stock_on_hand=5, reorder_point=10  => stock BELOW threshold => reorder
#   item_b: stock_on_hand=20, reorder_point=10 => stock AT OR ABOVE threshold => hold
_ITEM_STATES = {
    "item_a": ItemState(
        item_id                     = "item_a",
        stock_on_hand               = 5,
        stock_in_transit            = 0,
        expected_arrivals_next_tick = 0,
        reorder_point               = 10,
        min_order_qty               = 3,
        max_order_qty               = 20,
    ),
    "item_b": ItemState(
        item_id                     = "item_b",
        stock_on_hand               = 20,
        stock_in_transit            = 2,
        expected_arrivals_next_tick = 2,
        reorder_point               = 10,
        min_order_qty               = 5,
        max_order_qty               = 50,
    ),
}

def _make_context(tick: int = 1, sim_id: str = "test_sim_001") -> AgentContext:
    '''Build a minimal AgentContext from the shared item states.'''
    return AgentContext(
        sim_id             = sim_id,
        tick               = tick,
        item_states        = _ITEM_STATES,
        pending_orders     = [],
        demand_history     = {},
        active_disruptions = [],
        cost_snapshots     = {
            item_id: CostSnapshot(
                item_id                      = item_id,
                cumulative_holding_cost      = 0.0,
                cumulative_stockout_cost     = 0.0,
                cumulative_order_cost        = 0.0,
                cumulative_transit_loss_cost = 0.0,
                cumulative_total_cost        = 0.0,
                remaining_budget             = 10_000.0,
            )
            for item_id in _ITEM_STATES
        },
        remaining_budget   = 10_000.0,
    )


def _make_mock_world(min_lead_time: int = 3) -> MagicMock:
    '''
    Minimal SimWorld mock. Supplies two suppliers with base_lead_time_ticks
    of min_lead_time and min_lead_time + 2 so the minimum is predictable.
    '''
    supplier_a       = MagicMock()
    supplier_a.base_lead_time_ticks = min_lead_time

    supplier_b       = MagicMock()
    supplier_b.base_lead_time_ticks = min_lead_time + 2

    world            = MagicMock()
    world.suppliers  = {"sup_a": supplier_a, "sup_b": supplier_b}
    world.items      = {item_id: MagicMock() for item_id in _ITEM_STATES}

    return world


def _make_llm_agent_wrapper(stub_mode: Optional[str], n_ticks: int = 9999) -> LLMAgentWrapper:
    '''
    Construct a LLMAgentWrapper with a mock world and Spark session.
    executor_trigger_every_n_ticks defaults to 9999 so the executor never
    fires unless the test explicitly sets a lower value.
    '''
    config = LLMAgentWrapperConfig(
        executor_trigger_every_n_ticks  = n_ticks,
        context_obsolescence_threshold_k = 3,
        queue_size                       = 3,
        stub_mode                        = stub_mode,
    )
    world  = _make_mock_world(min_lead_time=3)
    spark  = MagicMock()   # _write_eval_metrics is not under test in pure-Python tests
    logger = MagicMock()   # EventLogger calls are captured by the mock
    return LLMAgentWrapper(spark=spark, world=world, config=config, logger=logger)


def _make_queue_message(tick: int, k: int = 3) -> QueueMessage:
    '''Build a QueueMessage for the given tick.'''
    return QueueMessage(
        trigger_tick           = tick,
        trigger_condition_met  = True,
        assembly_timestamp     = datetime.now(tz=timezone.utc),
        obsolescence_threshold = k,
        context                = _make_context(tick=tick),
        sim_id                 = "test_sim_001",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1 - `RuleBasedAgent` correctness

# COMMAND ----------

_header("T1", "RuleBasedAgent correctness")

agent   = RuleBasedAgent()
context = _make_context()
decisions = agent.decide(context)

# 1a: One decision per item
decision_map = {d.item_id: d for d in decisions}
_record("T1a", "One decision per item",
    set(decision_map.keys()) == set(_ITEM_STATES.keys()),
    f"got item_ids={set(decision_map.keys())}")

# 1b: item_a is below reorder_point => should reorder with min_order_qty
d_a = decision_map.get("item_a")
_record("T1b", "item_a (stock < reorder_point) => order_qty == min_order_qty",
    d_a is not None and d_a.order_qty == _ITEM_STATES["item_a"].min_order_qty,
    f"order_qty={d_a.order_qty if d_a else 'MISSING'}, expected={_ITEM_STATES['item_a'].min_order_qty}")

# 1c: item_b is at or above reorder_point => should hold
d_b = decision_map.get("item_b")
_record("T1c", "item_b (stock >= reorder_point) => order_qty == 0 (hold)",
    d_b is not None and d_b.order_qty == 0,
    f"order_qty={d_b.order_qty if d_b else 'MISSING'}")

# 1d: Determinism - two calls with same context produce identical decisions
decisions_2 = agent.decide(context)
_record("T1d", "Deterministic - two calls produce identical decisions",
    [(d.item_id, d.order_qty) for d in decisions] ==
    [(d.item_id, d.order_qty) for d in decisions_2])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2 - `LLMAgentWrapperConfig` validation

# COMMAND ----------

_header("T2", "LLMAgentWrapperConfig validation")

from pydantic import ValidationError

# 2a: Valid config constructs without error
try:
    cfg = LLMAgentWrapperConfig(executor_trigger_every_n_ticks=5)
    _record("T2a", "Valid minimal config constructs without error", True)
except Exception as e:
    _record("T2a", "Valid minimal config constructs without error", False, str(e))

# 2b: Missing executor_trigger_every_n_ticks => ValidationError
try:
    LLMAgentWrapperConfig()
    _record("T2b", "Missing executor_trigger_every_n_ticks => ValidationError", False,
            "No error raised")
except ValidationError:
    _record("T2b", "Missing executor_trigger_every_n_ticks => ValidationError", True)

# 2c: context_obsolescence_threshold_k=None => UserWarning emitted
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    LLMAgentWrapperConfig(executor_trigger_every_n_ticks=5, context_obsolescence_threshold_k=None)
    user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
    _record("T2c", "context_obsolescence_threshold_k=None => UserWarning emitted",
        len(user_warnings) > 0,
        f"warnings caught: {len(user_warnings)}: {[user_warning.message for user_warning in user_warnings]}")

# 2d: context_obsolescence_threshold_k=0 => ValidationError (ge=1)
try:
    LLMAgentWrapperConfig(executor_trigger_every_n_ticks=5, context_obsolescence_threshold_k=0)
    _record("T2d", "context_obsolescence_threshold_k=0 => ValidationError (ge=1)", False,
            "No error raised")
except ValidationError:
    _record("T2d", "context_obsolescence_threshold_k=0 => ValidationError (ge=1)", True)

# 2e: queue_size=0 => ValidationError (ge=1)
try:
    LLMAgentWrapperConfig(executor_trigger_every_n_ticks=5, queue_size=0)
    _record("T2e", "queue_size=0 => ValidationError (ge=1)", False, "No error raised")
except ValidationError:
    _record("T2e", "queue_size=0 => ValidationError (ge=1)", True)

# 2f: Invalid stub_mode => ValidationError
try:
    LLMAgentWrapperConfig(executor_trigger_every_n_ticks=5, stub_mode="not_a_valid_mode")
    _record("T2f", "Invalid stub_mode => ValidationError", False, "No error raised")
except ValidationError:
    _record("T2f", "Invalid stub_mode => ValidationError", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3 - `LLMAgentWrapper.__init__` - `_resolved_k` and initial state

# COMMAND ----------

_header("T3", "LLMAgentWrapper.__init__ - _resolved_k and initial state")

# 3a: context_obsolescence_threshold_k=None resolves to min_lead_time (3)
llm_agent_wrapper = _make_llm_agent_wrapper(stub_mode="valid")
_record("T3a", "_resolved_k == min_lead_time when k=None (expect 3)",
    llm_agent_wrapper._resolved_k == 3,
    f"_resolved_k={llm_agent_wrapper._resolved_k}")

# 3b: Explicit k is respected
cfg_explicit = LLMAgentWrapperConfig(
    executor_trigger_every_n_ticks   = 9999,
    context_obsolescence_threshold_k = 7,
    stub_mode                        = "valid",
)
llm_agent_wrapper_explicit = LLMAgentWrapper(
    spark  = MagicMock(),
    world  = _make_mock_world(min_lead_time=3),
    config = cfg_explicit,
    logger = MagicMock(),
)
_record("T3b", "Explicit context_obsolescence_threshold_k=7 is used as _resolved_k",
    llm_agent_wrapper_explicit._resolved_k == 7,
    f"_resolved_k={llm_agent_wrapper_explicit._resolved_k}")

# 3c: _last_committed is hold-all for all items at construction
all_hold = all(d.order_qty == 0 for d in llm_agent_wrapper._last_committed)
correct_items = {d.item_id for d in llm_agent_wrapper._last_committed} == set(_ITEM_STATES.keys())
_record("T3c", "_last_committed is hold-all for all items at construction",
    all_hold and correct_items,
    f"all_hold={all_hold}, correct_items={correct_items}")

# 3d: _queue is a deque with correct maxlen
from collections import deque
_record("T3d", "_queue is a deque with maxlen == queue_size",
    isinstance(llm_agent_wrapper._queue, deque) and llm_agent_wrapper._queue.maxlen == 3,
    f"type={type(llm_agent_wrapper._queue).__name__}, maxlen={llm_agent_wrapper._queue.maxlen}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4 - Pre-flight validation

# COMMAND ----------

_header("T4", "Pre-flight validation - structural and logical")

llm_agent_wrapper    = _make_llm_agent_wrapper(stub_mode="valid")
context = _make_context()

# 4a: Structural - valid list[ReorderDecision] passes
valid_decisions = [ReorderDecision(item_id="item_a", order_qty=3, reasoning="test")]
result, err = llm_agent_wrapper._validate_structural(valid_decisions)
_record("T4a", "Structural: valid list[ReorderDecision] => passes",
    err is None and result == valid_decisions,
    f"err={err}")

# 4b: Structural - raw string fails
result, err = llm_agent_wrapper._validate_structural("this is not a list of decisions")
_record("T4b", "Structural: raw string => fails with error message",
    result is None and err is not None,
    f"err={err}")

# 4c: Logical - all valid decisions => empty violations
valid_all = [
    ReorderDecision(item_id="item_a", order_qty=3,  reasoning="ok"),  # min=3, max=20
    ReorderDecision(item_id="item_b", order_qty=0,  reasoning="hold"),
]
violations = llm_agent_wrapper._validate_logical(valid_all, context)
_record("T4c", "Logical: all valid decisions => empty violations list",
    violations == [],
    f"violations={violations}")

# 4d: Logical - unknown item_id => violation recorded
unknown_item = [ReorderDecision(item_id="item_z", order_qty=5, reasoning="test")]
violations = llm_agent_wrapper._validate_logical(unknown_item, context)
_record("T4d", "Logical: unknown item_id => violation with violation_type='unknown_item_id'",
    len(violations) == 1 and violations[0]["violation_type"] == "unknown_item_id",
    f"violations={violations}")

# 4e: Logical - order_qty above max_order_qty => violation recorded
# item_a max_order_qty=20; sending 21
over_max = [
    ReorderDecision(item_id="item_a", order_qty=21, reasoning="too much"),
    ReorderDecision(item_id="item_b", order_qty=0,  reasoning="hold"),
]
violations = llm_agent_wrapper._validate_logical(over_max, context)
_record("T4e", "Logical: order_qty > max_order_qty => violation with violation_type='order_qty_out_of_range'",
    len(violations) == 1 and violations[0]["violation_type"] == "order_qty_out_of_range",
    f"violations={violations}")

# 4f: Logical - hold (order_qty=0) always passes regardless of min/max
hold_decisions = [ReorderDecision(item_id=iid, order_qty=0, reasoning="hold")
                  for iid in _ITEM_STATES]
violations = llm_agent_wrapper._validate_logical(hold_decisions, context)
_record("T4f", "Logical: hold (order_qty=0) always passes regardless of min/max",
    violations == [],
    f"violations={violations}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5 - `_run_executor` (called synchronously)
# MAGIC
# MAGIC The executor thread is called directly (not via `threading.Thread`) so
# MAGIC results are available immediately without any timing dependency.
# MAGIC Each sub-test inspects `_result_slot` and the mock logger after the call.

# COMMAND ----------

_header("T5", "_run_executor - all code paths (sync call)")

# ── T5a: valid stub mode - happy path ────────────────────────────────────────
llm_agent_wrapper_valid = _make_llm_agent_wrapper(stub_mode="valid")
snapshot   = [_make_queue_message(tick=1, k=3)]

llm_agent_wrapper_valid._run_executor(snapshot, current_tick=1)

slot = llm_agent_wrapper_valid._result_slot
_record("T5a", "valid: _result_slot is populated",
    slot is not None, f"slot={slot}")

_record("T5a.i", "valid: fallback_used=False",
    slot is not None and not slot.fallback_used,
    f"fallback_used={slot.fallback_used if slot else 'N/A'}")

_record("T5a.ii", "valid: one decision per item, all order_qty == min_order_qty",
    slot is not None and
    all(d.order_qty == _ITEM_STATES[d.item_id].min_order_qty for d in slot.decisions),
    f"decisions={[(d.item_id, d.order_qty) for d in slot.decisions] if slot else 'N/A'}")

# The implementation uses typed logger methods (fallback_structural, fallback_logical). Assert those typed methods were NOT called.
_record("T5a.iii", "valid: no FALLBACK events logged",
    llm_agent_wrapper_valid._logger.fallback_structural.call_count == 0
    and llm_agent_wrapper_valid._logger.fallback_logical.call_count == 0)

# ── T5b: structural_fail - FALLBACK_STRUCTURAL path ──────────────────────────
llm_agent_wrapper_sf  = _make_llm_agent_wrapper(stub_mode="structural_fail")
snapshot = [_make_queue_message(tick=1, k=3)]

llm_agent_wrapper_sf._run_executor(snapshot, current_tick=1)

slot = llm_agent_wrapper_sf._result_slot
_record("T5b", "structural_fail: _result_slot is populated",
    slot is not None)

_record("T5b.i", "structural_fail: fallback_type == 'FALLBACK_STRUCTURAL'",
    slot is not None and slot.fallback_type == "FALLBACK_STRUCTURAL",
    f"fallback_type={slot.fallback_type if slot else 'N/A'}")

# The implementation calls self._logger.fallback_structural(...) directly. Assert the typed method was called exactly once.
_record("T5b.ii", "structural_fail: FALLBACK_STRUCTURAL event logged",
    llm_agent_wrapper_sf._logger.fallback_structural.call_count == 1)

_record("T5b.iii", "structural_fail: RuleBasedAgent decisions returned (item_a reorders)",
    slot is not None and
    any(d.item_id == "item_a" and d.order_qty == _ITEM_STATES["item_a"].min_order_qty
        for d in slot.decisions),
    f"decisions={[(d.item_id, d.order_qty) for d in slot.decisions] if slot else 'N/A'}")

# ── T5c: logical_fail - FALLBACK_LOGICAL path ─────────────────────────────────
llm_agent_wrapper_lf  = _make_llm_agent_wrapper(stub_mode="logical_fail")
snapshot = [_make_queue_message(tick=1, k=3)]

llm_agent_wrapper_lf._run_executor(snapshot, current_tick=1)

slot = llm_agent_wrapper_lf._result_slot
_record("T5c", "logical_fail: _result_slot is populated",
    slot is not None)

_record("T5c.i", "logical_fail: fallback_type == 'FALLBACK_LOGICAL'",
    slot is not None and slot.fallback_type == "FALLBACK_LOGICAL",
    f"fallback_type={slot.fallback_type if slot else 'N/A'}")

# The implementation calls self._logger.fallback_logical(...) directly. Assert the typed method was called exactly once.
_record("T5c.ii", "logical_fail: FALLBACK_LOGICAL event logged",
    llm_agent_wrapper_lf._logger.fallback_logical.call_count == 1)

# ── T5d: all-stale queue ──────────────────────────────────────────────────────
llm_agent_wrapper_stale = _make_llm_agent_wrapper(stub_mode="valid")
# k=3, current_tick=10 => age = 10 - 1 = 9 > 3 => stale
stale_snapshot = [_make_queue_message(tick=1, k=3)]

llm_agent_wrapper_stale._run_executor(stale_snapshot, current_tick=10)

_record("T5d", "all-stale: _result_slot remains None",
    llm_agent_wrapper_stale._result_slot is None,
    f"_result_slot={llm_agent_wrapper_stale._result_slot}")

# The implementation calls self._logger.executor_all_stale(...) directly. Assert the typed method was called exactly once.
_record("T5d.i", "all-stale: EXECUTOR_ALL_STALE event logged",
    llm_agent_wrapper_stale._logger.executor_all_stale.call_count == 1)

# ── T5e: empty queue snapshot ─────────────────────────────────────────────────
llm_agent_wrapper_empty = _make_llm_agent_wrapper(stub_mode="valid")
llm_agent_wrapper_empty._run_executor([], current_tick=5)

_record("T5e", "empty queue: _result_slot remains None",
    llm_agent_wrapper_empty._result_slot is None)

# Same as T5d.i - empty queue also triggers executor_all_stale.
_record("T5e.i", "empty queue: EXECUTOR_ALL_STALE event logged",
    llm_agent_wrapper_empty._logger.executor_all_stale.call_count == 1)

# ── T5f: obsolescence boundary ───────────────────────────────────────────────
# A message at exactly the boundary (age == k) should NOT be stale.
# k=3, trigger_tick=7, current_tick=10 => age = 10 - 7 = 3 == k => valid
llm_agent_wrapper_boundary = _make_llm_agent_wrapper(stub_mode="valid")
boundary_snapshot = [_make_queue_message(tick=7, k=3)]

llm_agent_wrapper_boundary._run_executor(boundary_snapshot, current_tick=10)

_record("T5f", "obsolescence boundary: age == k is NOT stale (slot populated)",
    llm_agent_wrapper_boundary._result_slot is not None,
    f"_result_slot={llm_agent_wrapper_boundary._result_slot}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6 - Full `decide()` loop - monitoring loop only
# MAGIC
# MAGIC Executor trigger set to 9999 so it never fires. Verifies the monitoring
# MAGIC loop queues messages every tick and returns hold-all throughout.
# MAGIC No Spark required - `_write_eval_metrics` is mocked via the mock SparkSession.

# COMMAND ----------

_header("T6", "decide() - monitoring loop only (executor never fires)")

llm_agent_wrapper = _make_llm_agent_wrapper(stub_mode="valid", n_ticks=9999)

# KEY POINT: Start from tick 1, not tick 0. Why?
# - tick % N == 0 is True for ANY N at tick 0
# - So tick 0 would trigger the executor regardless of n_ticks=9999
# - The implementation guards against this with `tick > 0` in the trigger condition
# - Starting at 1 here makes the test's intent explicit and avoids relying solely on that guard
for t in range(1, 6):
    ctx    = _make_context(tick=t)
    result = llm_agent_wrapper.decide(ctx)

    # executor_trigger_every_n_ticks=9999 and tick > 0 guard mean the executor never fires - hold-all expected every tick
    _record(f"T6.tick{t}", f"tick {t}: decide() returns hold-all",
        all(d.order_qty == 0 for d in result),
        f"order_qtys={[d.order_qty for d in result]}")

# After ticks 1-5 with queue_size=3, the deque holds the last 3: ticks 3, 4, 5
_record("T6.queue", "Queue holds the last queue_size messages after 5 ticks",
    len(llm_agent_wrapper._queue) == 3,
    f"queue length={len(llm_agent_wrapper._queue)}")

_record("T6.queue_ticks", "Queue contains messages for ticks 3, 4, 5 (newest 3)",
    [m.trigger_tick for m in llm_agent_wrapper._queue] == [3, 4, 5],
    f"trigger_ticks={[m.trigger_tick for m in llm_agent_wrapper._queue]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark tests - world setup
# MAGIC
# MAGIC Tests 7 and 8 require env tables to be populated before the runner can be called.
# MAGIC This cell builds a minimal `SimWorld` programmatically and writes it to the env tables
# MAGIC using `write_world` - the same pattern used in the simulation's own test notebooks.
# MAGIC
# MAGIC **World spec (shared across T7 and T8)**:
# MAGIC - 2 items (`item_a`, `item_b`), 1 supplier (`sup_001`), 1 consumer (`con_001`)
# MAGIC - Flat demand pattern (Poisson, mu=10) for both items
# MAGIC - Deterministic lead time of 3 ticks, no variability
# MAGIC - 10 ticks, finite run, seed=42, budget=100,000
# MAGIC - No disruptions
# MAGIC
# MAGIC Three sim_ids are written:
# MAGIC - `test_llm_agent_wrapper_resilience_001` - used by T7
# MAGIC - `test_llm_agent_wrapper_repro_001` - used by T8 (run A)
# MAGIC - `test_llm_agent_wrapper_repro_002` - used by T8 (run B, identical config to repro_001)
# MAGIC
# MAGIC The cell is idempotent: `write_world` uses `IF NOT EXISTS` semantics on the env tables,
# MAGIC so re-running the notebook does not duplicate rows.
# MAGIC
# MAGIC **Key design choices and rationale**:
# MAGIC - `lead_time_variability=0.0` - deterministic lead time is required for T8. Any variability
# MAGIC   would mean the two repro runs diverge on lead time RNG draws even with the same seed,
# MAGIC   because the executor thread dispatches asynchronously and draw order is not guaranteed
# MAGIC   to be identical between runs.
# MAGIC - `random_seed=42` shared across all three sim_ids - repro_001 and repro_002 must have
# MAGIC   identical configs (only `sim_id` differs) for the T8 reproducibility assertion to be
# MAGIC   meaningful. A different seed per run would trivially produce different decisions.
# MAGIC - `num_ticks=10` - short enough for the Spark tests to run quickly; long enough for the
# MAGIC   executor to fire at least once at `executor_trigger_every_n_ticks=5`.
# MAGIC - `disruptions=[]` - no disruptions keeps the world fully deterministic and the assertions
# MAGIC   clean. Disruption stochasticity would add RNG draws that could mask reproducibility failures.

# COMMAND ----------

from warehouse_sim.config.models import (
    SimConfig,
    ItemType,
    Supplier,
    Consumer,
    Pattern,
    RunMode,
    TickUnit,
    PatternType,
    Distribution,
    SimWorld,
)
from warehouse_sim.world.setup import write_world
from datetime import datetime, timezone

# Databricks notebook source
# MAGIC %md
# MAGIC # LLMAgentWrapper - Functional Tests
# MAGIC
# MAGIC **Scope**: Pranav's LLMAgentWrapper implementation (stub phase).
# MAGIC
# MAGIC **Test structure**:
# MAGIC - Tests 1-6: Pure Python - no Spark required. Run anywhere.
# MAGIC - Tests 7-8: Spark required - full runner integration.
# MAGIC
# MAGIC Each test cell prints a clear PASS / FAIL with a short reason. A summary cell at the end collects all results.
# MAGIC
# MAGIC **Files under test**:
# MAGIC - `warehouse_sim/agent/rule_based_agent.py`
# MAGIC - `warehouse_sim/agent/llm_agent_wrapper.py`
# MAGIC - `warehouse_sim/agent/llm_agent_wrapper_types.py`
# MAGIC - `warehouse_sim/config/llm_agent_wrapper_config.py`
# MAGIC - `warehouse_sim/engine/runner.py` (resilience wrap)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Context**:
# MAGIC - [`__docs__/reasoningIntegrationDevelopmentApproach-3.md`](./__docs__/reasoningIntegrationDevelopmentApproach-3.md) - implementation stages
# MAGIC - [`__docs__/reasoningIntegrationDevelopmentApproach-1.md`](./__docs__/reasoningIntegrationDevelopmentApproach-1.md) - implementation approach

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %pip install pydantic numpy matplotlib pandas
# MAGIC %restart_python

# COMMAND ----------

import sys

PACKAGE_ROOT = "/Workspace/Repos/mistermilvusmigrans@gmail.com/dbx-agentic-ai-project-sim-package"
if PACKAGE_ROOT not in sys.path:
    sys.path.insert(0, PACKAGE_ROOT)

print("Python path updated.")

# COMMAND ----------

# Shared test result collector.
# Each test appends (test_id, label, passed, reason).
# The summary cell at the end prints all results.
_results: list[tuple[str, str, bool, str]] = []

def _record(test_id: str, label: str, passed: bool, reason: str = "") -> None:
    status = "✓ PASS" if passed else "✗ FAIL"
    print(f"  {status}  {label}" + (f"\n         {reason}" if reason else ""))
    _results.append((test_id, label, passed, reason))

def _header(test_id: str, title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {test_id}  {title}")
    print(f"{'─' * 60}")

# COMMAND ----------

# Shared imports used across multiple tests
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import MagicMock
import warnings

from warehouse_sim.agent.base import (
    ActiveDisruption,
    AgentContext,
    CostSnapshot,
    DemandRecord,
    ItemState,
    PendingOrder,
    ReorderDecision,
)
from warehouse_sim.agent.rule_based_agent import RuleBasedAgent
from warehouse_sim.agent.llm_agent_wrapper import LLMAgentWrapper
from warehouse_sim.agent.llm_agent_wrapper_types import QueueMessage, ExecutorResult
from warehouse_sim.config.llm_agent_wrapper_config import LLMAgentWrapperConfig

# COMMAND ----------

# MAGIC %md
# MAGIC ### Shared fixtures
# MAGIC
# MAGIC A minimal two-item world and `AgentContext` used across multiple tests.
# MAGIC Hard-coded values are chosen to make assertions readable at a glance.

# COMMAND ----------

# Two items:
#   item_a: stock_on_hand=5, reorder_point=10  => stock BELOW threshold => reorder
#   item_b: stock_on_hand=20, reorder_point=10 => stock AT OR ABOVE threshold => hold
_ITEM_STATES = {
    "item_a": ItemState(
        item_id                     = "item_a",
        stock_on_hand               = 5,
        stock_in_transit            = 0,
        expected_arrivals_next_tick = 0,
        reorder_point               = 10,
        min_order_qty               = 3,
        max_order_qty               = 20,
    ),
    "item_b": ItemState(
        item_id                     = "item_b",
        stock_on_hand               = 20,
        stock_in_transit            = 2,
        expected_arrivals_next_tick = 2,
        reorder_point               = 10,
        min_order_qty               = 5,
        max_order_qty               = 50,
    ),
}

def _make_context(tick: int = 1, sim_id: str = "test_sim_001") -> AgentContext:
    '''Build a minimal AgentContext from the shared item states.'''
    return AgentContext(
        sim_id             = sim_id,
        tick               = tick,
        item_states        = _ITEM_STATES,
        pending_orders     = [],
        demand_history     = {},
        active_disruptions = [],
        cost_snapshots     = {
            item_id: CostSnapshot(
                item_id                      = item_id,
                cumulative_holding_cost      = 0.0,
                cumulative_stockout_cost     = 0.0,
                cumulative_order_cost        = 0.0,
                cumulative_transit_loss_cost = 0.0,
                cumulative_total_cost        = 0.0,
                remaining_budget             = 10_000.0,
            )
            for item_id in _ITEM_STATES
        },
        remaining_budget   = 10_000.0,
    )


def _make_mock_world(min_lead_time: int = 3) -> MagicMock:
    '''
    Minimal SimWorld mock. Supplies two suppliers with base_lead_time_ticks
    of min_lead_time and min_lead_time + 2 so the minimum is predictable.
    '''
    supplier_a       = MagicMock()
    supplier_a.base_lead_time_ticks = min_lead_time

    supplier_b       = MagicMock()
    supplier_b.base_lead_time_ticks = min_lead_time + 2

    world            = MagicMock()
    world.suppliers  = {"sup_a": supplier_a, "sup_b": supplier_b}
    world.items      = {item_id: MagicMock() for item_id in _ITEM_STATES}

    return world


def _make_llm_agent_wrapper(stub_mode: Optional[str], n_ticks: int = 9999) -> LLMAgentWrapper:
    '''
    Construct a LLMAgentWrapper with a mock world and Spark session.
    executor_trigger_every_n_ticks defaults to 9999 so the executor never
    fires unless the test explicitly sets a lower value.
    '''
    config = LLMAgentWrapperConfig(
        executor_trigger_every_n_ticks  = n_ticks,
        context_obsolescence_threshold_k = 3,
        queue_size                       = 3,
        stub_mode                        = stub_mode,
    )
    world  = _make_mock_world(min_lead_time=3)
    spark  = MagicMock()   # _write_eval_metrics is not under test in pure-Python tests
    logger = MagicMock()   # EventLogger calls are captured by the mock
    return LLMAgentWrapper(spark=spark, world=world, config=config, logger=logger)


def _make_queue_message(tick: int, k: int = 3) -> QueueMessage:
    '''Build a QueueMessage for the given tick.'''
    return QueueMessage(
        trigger_tick           = tick,
        trigger_condition_met  = True,
        assembly_timestamp     = datetime.now(tz=timezone.utc),
        obsolescence_threshold = k,
        context                = _make_context(tick=tick),
        sim_id                 = "test_sim_001",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1 - `RuleBasedAgent` correctness

# COMMAND ----------

_header("T1", "RuleBasedAgent correctness")

agent   = RuleBasedAgent()
context = _make_context()
decisions = agent.decide(context)

# 1a: One decision per item
decision_map = {d.item_id: d for d in decisions}
_record("T1a", "One decision per item",
    set(decision_map.keys()) == set(_ITEM_STATES.keys()),
    f"got item_ids={set(decision_map.keys())}")

# 1b: item_a is below reorder_point => should reorder with min_order_qty
d_a = decision_map.get("item_a")
_record("T1b", "item_a (stock < reorder_point) => order_qty == min_order_qty",
    d_a is not None and d_a.order_qty == _ITEM_STATES["item_a"].min_order_qty,
    f"order_qty={d_a.order_qty if d_a else 'MISSING'}, expected={_ITEM_STATES['item_a'].min_order_qty}")

# 1c: item_b is at or above reorder_point => should hold
d_b = decision_map.get("item_b")
_record("T1c", "item_b (stock >= reorder_point) => order_qty == 0 (hold)",
    d_b is not None and d_b.order_qty == 0,
    f"order_qty={d_b.order_qty if d_b else 'MISSING'}")

# 1d: Determinism - two calls with same context produce identical decisions
decisions_2 = agent.decide(context)
_record("T1d", "Deterministic - two calls produce identical decisions",
    [(d.item_id, d.order_qty) for d in decisions] ==
    [(d.item_id, d.order_qty) for d in decisions_2])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2 - `LLMAgentWrapperConfig` validation

# COMMAND ----------

_header("T2", "LLMAgentWrapperConfig validation")

from pydantic import ValidationError

# 2a: Valid config constructs without error
try:
    cfg = LLMAgentWrapperConfig(executor_trigger_every_n_ticks=5)
    _record("T2a", "Valid minimal config constructs without error", True)
except Exception as e:
    _record("T2a", "Valid minimal config constructs without error", False, str(e))

# 2b: Missing executor_trigger_every_n_ticks => ValidationError
try:
    LLMAgentWrapperConfig()
    _record("T2b", "Missing executor_trigger_every_n_ticks => ValidationError", False,
            "No error raised")
except ValidationError:
    _record("T2b", "Missing executor_trigger_every_n_ticks => ValidationError", True)

# 2c: context_obsolescence_threshold_k=None => UserWarning emitted
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    LLMAgentWrapperConfig(executor_trigger_every_n_ticks=5, context_obsolescence_threshold_k=None)
    user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
    _record("T2c", "context_obsolescence_threshold_k=None => UserWarning emitted",
        len(user_warnings) > 0,
        f"warnings caught: {len(user_warnings)}: {[user_warning.message for user_warning in user_warnings]}")

# 2d: context_obsolescence_threshold_k=0 => ValidationError (ge=1)
try:
    LLMAgentWrapperConfig(executor_trigger_every_n_ticks=5, context_obsolescence_threshold_k=0)
    _record("T2d", "context_obsolescence_threshold_k=0 => ValidationError (ge=1)", False,
            "No error raised")
except ValidationError:
    _record("T2d", "context_obsolescence_threshold_k=0 => ValidationError (ge=1)", True)

# 2e: queue_size=0 => ValidationError (ge=1)
try:
    LLMAgentWrapperConfig(executor_trigger_every_n_ticks=5, queue_size=0)
    _record("T2e", "queue_size=0 => ValidationError (ge=1)", False, "No error raised")
except ValidationError:
    _record("T2e", "queue_size=0 => ValidationError (ge=1)", True)

# 2f: Invalid stub_mode => ValidationError
try:
    LLMAgentWrapperConfig(executor_trigger_every_n_ticks=5, stub_mode="not_a_valid_mode")
    _record("T2f", "Invalid stub_mode => ValidationError", False, "No error raised")
except ValidationError:
    _record("T2f", "Invalid stub_mode => ValidationError", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3 - `LLMAgentWrapper.__init__` - `_resolved_k` and initial state

# COMMAND ----------

_header("T3", "LLMAgentWrapper.__init__ - _resolved_k and initial state")

# 3a: context_obsolescence_threshold_k=None resolves to min_lead_time (3)
llm_agent_wrapper = _make_llm_agent_wrapper(stub_mode="valid")
_record("T3a", "_resolved_k == min_lead_time when k=None (expect 3)",
    llm_agent_wrapper._resolved_k == 3,
    f"_resolved_k={llm_agent_wrapper._resolved_k}")

# 3b: Explicit k is respected
cfg_explicit = LLMAgentWrapperConfig(
    executor_trigger_every_n_ticks   = 9999,
    context_obsolescence_threshold_k = 7,
    stub_mode                        = "valid",
)
llm_agent_wrapper_explicit = LLMAgentWrapper(
    spark  = MagicMock(),
    world  = _make_mock_world(min_lead_time=3),
    config = cfg_explicit,
    logger = MagicMock(),
)
_record("T3b", "Explicit context_obsolescence_threshold_k=7 is used as _resolved_k",
    llm_agent_wrapper_explicit._resolved_k == 7,
    f"_resolved_k={llm_agent_wrapper_explicit._resolved_k}")

# 3c: _last_committed is hold-all for all items at construction
all_hold = all(d.order_qty == 0 for d in llm_agent_wrapper._last_committed)
correct_items = {d.item_id for d in llm_agent_wrapper._last_committed} == set(_ITEM_STATES.keys())
_record("T3c", "_last_committed is hold-all for all items at construction",
    all_hold and correct_items,
    f"all_hold={all_hold}, correct_items={correct_items}")

# 3d: _queue is a deque with correct maxlen
from collections import deque
_record("T3d", "_queue is a deque with maxlen == queue_size",
    isinstance(llm_agent_wrapper._queue, deque) and llm_agent_wrapper._queue.maxlen == 3,
    f"type={type(llm_agent_wrapper._queue).__name__}, maxlen={llm_agent_wrapper._queue.maxlen}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4 - Pre-flight validation

# COMMAND ----------

_header("T4", "Pre-flight validation - structural and logical")

llm_agent_wrapper    = _make_llm_agent_wrapper(stub_mode="valid")
context = _make_context()

# 4a: Structural - valid list[ReorderDecision] passes
valid_decisions = [ReorderDecision(item_id="item_a", order_qty=3, reasoning="test")]
result, err = llm_agent_wrapper._validate_structural(valid_decisions)
_record("T4a", "Structural: valid list[ReorderDecision] => passes",
    err is None and result == valid_decisions,
    f"err={err}")

# 4b: Structural - raw string fails
result, err = llm_agent_wrapper._validate_structural("this is not a list of decisions")
_record("T4b", "Structural: raw string => fails with error message",
    result is None and err is not None,
    f"err={err}")

# 4c: Logical - all valid decisions => empty violations
valid_all = [
    ReorderDecision(item_id="item_a", order_qty=3,  reasoning="ok"),  # min=3, max=20
    ReorderDecision(item_id="item_b", order_qty=0,  reasoning="hold"),
]
violations = llm_agent_wrapper._validate_logical(valid_all, context)
_record("T4c", "Logical: all valid decisions => empty violations list",
    violations == [],
    f"violations={violations}")

# 4d: Logical - unknown item_id => violation recorded
unknown_item = [ReorderDecision(item_id="item_z", order_qty=5, reasoning="test")]
violations = llm_agent_wrapper._validate_logical(unknown_item, context)
_record("T4d", "Logical: unknown item_id => violation with violation_type='unknown_item_id'",
    len(violations) == 1 and violations[0]["violation_type"] == "unknown_item_id",
    f"violations={violations}")

# 4e: Logical - order_qty above max_order_qty => violation recorded
# item_a max_order_qty=20; sending 21
over_max = [
    ReorderDecision(item_id="item_a", order_qty=21, reasoning="too much"),
    ReorderDecision(item_id="item_b", order_qty=0,  reasoning="hold"),
]
violations = llm_agent_wrapper._validate_logical(over_max, context)
_record("T4e", "Logical: order_qty > max_order_qty => violation with violation_type='order_qty_out_of_range'",
    len(violations) == 1 and violations[0]["violation_type"] == "order_qty_out_of_range",
    f"violations={violations}")

# 4f: Logical - hold (order_qty=0) always passes regardless of min/max
hold_decisions = [ReorderDecision(item_id=iid, order_qty=0, reasoning="hold")
                  for iid in _ITEM_STATES]
violations = llm_agent_wrapper._validate_logical(hold_decisions, context)
_record("T4f", "Logical: hold (order_qty=0) always passes regardless of min/max",
    violations == [],
    f"violations={violations}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5 - `_run_executor` (called synchronously)
# MAGIC
# MAGIC The executor thread is called directly (not via `threading.Thread`) so
# MAGIC results are available immediately without any timing dependency.
# MAGIC Each sub-test inspects `_result_slot` and the mock logger after the call.

# COMMAND ----------

_header("T5", "_run_executor - all code paths (sync call)")

# ── T5a: valid stub mode - happy path ────────────────────────────────────────
llm_agent_wrapper_valid = _make_llm_agent_wrapper(stub_mode="valid")
snapshot   = [_make_queue_message(tick=1, k=3)]

llm_agent_wrapper_valid._run_executor(snapshot, current_tick=1)

slot = llm_agent_wrapper_valid._result_slot
_record("T5a", "valid: _result_slot is populated",
    slot is not None, f"slot={slot}")

_record("T5a.i", "valid: fallback_used=False",
    slot is not None and not slot.fallback_used,
    f"fallback_used={slot.fallback_used if slot else 'N/A'}")

_record("T5a.ii", "valid: one decision per item, all order_qty == min_order_qty",
    slot is not None and
    all(d.order_qty == _ITEM_STATES[d.item_id].min_order_qty for d in slot.decisions),
    f"decisions={[(d.item_id, d.order_qty) for d in slot.decisions] if slot else 'N/A'}")

# The implementation uses typed logger methods (fallback_structural, fallback_logical). Assert those typed methods were NOT called.
_record("T5a.iii", "valid: no FALLBACK events logged",
    llm_agent_wrapper_valid._logger.fallback_structural.call_count == 0
    and llm_agent_wrapper_valid._logger.fallback_logical.call_count == 0)

# ── T5b: structural_fail - FALLBACK_STRUCTURAL path ──────────────────────────
llm_agent_wrapper_sf  = _make_llm_agent_wrapper(stub_mode="structural_fail")
snapshot = [_make_queue_message(tick=1, k=3)]

llm_agent_wrapper_sf._run_executor(snapshot, current_tick=1)

slot = llm_agent_wrapper_sf._result_slot
_record("T5b", "structural_fail: _result_slot is populated",
    slot is not None)

_record("T5b.i", "structural_fail: fallback_type == 'FALLBACK_STRUCTURAL'",
    slot is not None and slot.fallback_type == "FALLBACK_STRUCTURAL",
    f"fallback_type={slot.fallback_type if slot else 'N/A'}")

# The implementation calls self._logger.fallback_structural(...) directly. Assert the typed method was called exactly once.
_record("T5b.ii", "structural_fail: FALLBACK_STRUCTURAL event logged",
    llm_agent_wrapper_sf._logger.fallback_structural.call_count == 1)

_record("T5b.iii", "structural_fail: RuleBasedAgent decisions returned (item_a reorders)",
    slot is not None and
    any(d.item_id == "item_a" and d.order_qty == _ITEM_STATES["item_a"].min_order_qty
        for d in slot.decisions),
    f"decisions={[(d.item_id, d.order_qty) for d in slot.decisions] if slot else 'N/A'}")

# ── T5c: logical_fail - FALLBACK_LOGICAL path ─────────────────────────────────
llm_agent_wrapper_lf  = _make_llm_agent_wrapper(stub_mode="logical_fail")
snapshot = [_make_queue_message(tick=1, k=3)]

llm_agent_wrapper_lf._run_executor(snapshot, current_tick=1)

slot = llm_agent_wrapper_lf._result_slot
_record("T5c", "logical_fail: _result_slot is populated",
    slot is not None)

_record("T5c.i", "logical_fail: fallback_type == 'FALLBACK_LOGICAL'",
    slot is not None and slot.fallback_type == "FALLBACK_LOGICAL",
    f"fallback_type={slot.fallback_type if slot else 'N/A'}")

# The implementation calls self._logger.fallback_logical(...) directly. Assert the typed method was called exactly once.
_record("T5c.ii", "logical_fail: FALLBACK_LOGICAL event logged",
    llm_agent_wrapper_lf._logger.fallback_logical.call_count == 1)

# ── T5d: all-stale queue ──────────────────────────────────────────────────────
llm_agent_wrapper_stale = _make_llm_agent_wrapper(stub_mode="valid")
# k=3, current_tick=10 => age = 10 - 1 = 9 > 3 => stale
stale_snapshot = [_make_queue_message(tick=1, k=3)]

llm_agent_wrapper_stale._run_executor(stale_snapshot, current_tick=10)

_record("T5d", "all-stale: _result_slot remains None",
    llm_agent_wrapper_stale._result_slot is None,
    f"_result_slot={llm_agent_wrapper_stale._result_slot}")

# The implementation calls self._logger.executor_all_stale(...) directly. Assert the typed method was called exactly once.
_record("T5d.i", "all-stale: EXECUTOR_ALL_STALE event logged",
    llm_agent_wrapper_stale._logger.executor_all_stale.call_count == 1)

# ── T5e: empty queue snapshot ─────────────────────────────────────────────────
llm_agent_wrapper_empty = _make_llm_agent_wrapper(stub_mode="valid")
llm_agent_wrapper_empty._run_executor([], current_tick=5)

_record("T5e", "empty queue: _result_slot remains None",
    llm_agent_wrapper_empty._result_slot is None)

# Same as T5d.i - empty queue also triggers executor_all_stale.
_record("T5e.i", "empty queue: EXECUTOR_ALL_STALE event logged",
    llm_agent_wrapper_empty._logger.executor_all_stale.call_count == 1)

# ── T5f: obsolescence boundary ───────────────────────────────────────────────
# A message at exactly the boundary (age == k) should NOT be stale.
# k=3, trigger_tick=7, current_tick=10 => age = 10 - 7 = 3 == k => valid
llm_agent_wrapper_boundary = _make_llm_agent_wrapper(stub_mode="valid")
boundary_snapshot = [_make_queue_message(tick=7, k=3)]

llm_agent_wrapper_boundary._run_executor(boundary_snapshot, current_tick=10)

_record("T5f", "obsolescence boundary: age == k is NOT stale (slot populated)",
    llm_agent_wrapper_boundary._result_slot is not None,
    f"_result_slot={llm_agent_wrapper_boundary._result_slot}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6 - Full `decide()` loop - monitoring loop only
# MAGIC
# MAGIC Executor trigger set to 9999 so it never fires. Verifies the monitoring
# MAGIC loop queues messages every tick and returns hold-all throughout.
# MAGIC No Spark required - `_write_eval_metrics` is mocked via the mock SparkSession.

# COMMAND ----------

_header("T6", "decide() - monitoring loop only (executor never fires)")

llm_agent_wrapper = _make_llm_agent_wrapper(stub_mode="valid", n_ticks=9999)

# KEY POINT: Start from tick 1, not tick 0. Why?
# - tick % N == 0 is True for ANY N at tick 0
# - So tick 0 would trigger the executor regardless of n_ticks=9999
# - The implementation guards against this with `tick > 0` in the trigger condition
# - Starting at 1 here makes the test's intent explicit and avoids relying solely on that guard
for t in range(1, 6):
    ctx    = _make_context(tick=t)
    result = llm_agent_wrapper.decide(ctx)

    # executor_trigger_every_n_ticks=9999 and tick > 0 guard mean the executor never fires - hold-all expected every tick
    _record(f"T6.tick{t}", f"tick {t}: decide() returns hold-all",
        all(d.order_qty == 0 for d in result),
        f"order_qtys={[d.order_qty for d in result]}")

# After ticks 1-5 with queue_size=3, the deque holds the last 3: ticks 3, 4, 5
_record("T6.queue", "Queue holds the last queue_size messages after 5 ticks",
    len(llm_agent_wrapper._queue) == 3,
    f"queue length={len(llm_agent_wrapper._queue)}")

_record("T6.queue_ticks", "Queue contains messages for ticks 3, 4, 5 (newest 3)",
    [m.trigger_tick for m in llm_agent_wrapper._queue] == [3, 4, 5],
    f"trigger_ticks={[m.trigger_tick for m in llm_agent_wrapper._queue]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark tests - world setup
# MAGIC
# MAGIC Tests 7 and 8 require env tables to be populated before the runner can be called.
# MAGIC This cell builds a minimal `SimWorld` programmatically and writes it to the env tables
# MAGIC using `write_world` - the same pattern used in the simulation's own test notebooks.
# MAGIC
# MAGIC **World spec (shared across T7 and T8)**:
# MAGIC - 2 items (`item_a`, `item_b`), 1 supplier (`sup_001`), 1 consumer (`con_001`)
# MAGIC - Flat demand pattern (Poisson, mu=10) for both items
# MAGIC - Deterministic lead time of 3 ticks, no variability
# MAGIC - 10 ticks, finite run, seed=42, budget=100,000
# MAGIC - No disruptions
# MAGIC
# MAGIC Three sim_ids are written:
# MAGIC - `test_llm_agent_wrapper_resilience_001` - used by T7
# MAGIC - `test_llm_agent_wrapper_repro_001` - used by T8 (run A)
# MAGIC - `test_llm_agent_wrapper_repro_002` - used by T8 (run B, identical config to repro_001)
# MAGIC
# MAGIC The cell is idempotent: `write_world` uses `IF NOT EXISTS` semantics on the env tables,
# MAGIC so re-running the notebook does not duplicate rows.
# MAGIC
# MAGIC **Key design choices and rationale**:
# MAGIC - `lead_time_variability=0.0` - deterministic lead time is required for T8. Any variability
# MAGIC   would mean the two repro runs diverge on lead time RNG draws even with the same seed,
# MAGIC   because the executor thread dispatches asynchronously and draw order is not guaranteed
# MAGIC   to be identical between runs.
# MAGIC - `random_seed=42` shared across all three sim_ids - repro_001 and repro_002 must have
# MAGIC   identical configs (only `sim_id` differs) for the T8 reproducibility assertion to be
# MAGIC   meaningful. A different seed per run would trivially produce different decisions.
# MAGIC - `num_ticks=10` - short enough for the Spark tests to run quickly; long enough for the
# MAGIC   executor to fire at least once at `executor_trigger_every_n_ticks=5`.
# MAGIC - `disruptions=[]` - no disruptions keeps the world fully deterministic and the assertions
# MAGIC   clean. Disruption stochasticity would add RNG draws that could mask reproducibility failures.

# COMMAND ----------

from warehouse_sim.config.models import (
    SimConfig,
    ItemType,
    Supplier,
    Consumer,
    Pattern,
    RunMode,
    TickUnit,
    PatternRole,
    PatternType,
    Distribution,
    SimWorld,
)
from warehouse_sim.world.setup import write_world
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Shared world definition
# ---------------------------------------------------------------------------
# One supplier covers both items; one consumer demands both items.
# Flat Poisson demand (mu=10) gives non-trivial but predictable behaviour.
# Deterministic lead time (variability=0) keeps T8 reproducibility clean.

_TEST_ITEMS = {
    "item_a": ItemType(
        item_id                         = "item_a",
        item_name                       = "Item A",
        unit_value                      = 10.0,
        initial_stock                   = 30,
        reorder_point                   = 15,
        min_order_qty                   = 3,
        max_order_qty                   = 20,
        holding_cost_per_unit_per_tick  = 0.02,
        stockout_cost_per_unit_per_tick = 5.0,
        order_fixed_cost                = 1.0,
        order_variable_cost_per_unit    = 0.0,
        transit_loss_cost_per_unit      = 0.0,
    ),
    "item_b": ItemType(
        item_id                         = "item_b",
        item_name                       = "Item B",
        unit_value                      = 5.0,
        initial_stock                   = 50,
        reorder_point                   = 20,
        min_order_qty                   = 5,
        max_order_qty                   = 50,
        holding_cost_per_unit_per_tick  = 0.01,
        stockout_cost_per_unit_per_tick = 3.0,
        order_fixed_cost                = 1.0,
        order_variable_cost_per_unit    = 0.0,
        transit_loss_cost_per_unit      = 0.0,
    ),
}

_TEST_SUPPLIERS = {
    "sup_001": Supplier(
        supplier_id           = "sup_001",
        supplier_name         = "Test Supplier",
        base_lead_time_ticks  = 3,
        # lead_time_variability=0.0: deterministic lead time is required for T8.
        # Any variability would cause lead time RNG draws to diverge between the two
        # repro runs even with the same seed, because the executor dispatches
        # asynchronously and draw order is not guaranteed to be identical across runs.
        lead_time_variability = 0.0,
    ),
}

_TEST_CONSUMERS = {
    "con_001": Consumer(
        consumer_id   = "con_001",
        consumer_name = "Test Consumer",
    ),
}

# Supplier covers both items; consumer demands both items.
_SUPPLIER_ITEM_MAP = {"item_a": "sup_001", "item_b": "sup_001"}
_CONSUMER_ITEM_MAP = {"item_a": "con_001", "item_b": "con_001"}


def _make_demand_patterns(sim_id: str) -> dict:
    '''
    One Poisson demand pattern per item, keyed by item_id.
    SimWorld.demand_patterns is dict[str, Pattern] - one entry per item.

    pattern_id is a stable deterministic string (not a uuid) so that
    re-running the setup cell does not create duplicate pattern rows
    with different IDs in the env tables.
    '''
    return {
        "item_a": Pattern(
            pattern_id   = f"{sim_id}__item_a__demand",
            sim_id       = sim_id,
            item_id      = "item_a",
            role         = PatternRole.DEMAND,
            pattern_type = PatternType.STATISTICAL,
            distribution = Distribution.POISSON,
            dist_params  = {"mu": 10},
        ),
        "item_b": Pattern(
            pattern_id   = f"{sim_id}__item_b__demand",
            sim_id       = sim_id,
            item_id      = "item_b",
            role         = PatternRole.DEMAND,
            pattern_type = PatternType.STATISTICAL,
            distribution = Distribution.POISSON,
            dist_params  = {"mu": 10},
        ),
    }


def _make_test_sim_config(sim_id: str) -> SimConfig:
    '''
    Build a SimConfig for the given sim_id.
    All test runs share the same world definition and random seed - only
    sim_id differs. This is what makes T8 a valid reproducibility test:
    two runs with identical configs must produce identical decisions.

    random_seed=42: arbitrary but fixed. A different seed per run would
    trivially produce different decisions and make T8 meaningless.

    num_ticks=10: short enough for fast Spark test runs; long enough for
    the executor to fire at least once at executor_trigger_every_n_ticks=5.
    '''
    return SimConfig(
        sim_id                     = sim_id,
        random_seed                = 42,
        num_ticks                  = 10,
        run_mode                   = RunMode.FINITE,
        tick_unit                  = TickUnit.DAY,
        budget_limit               = 100_000.0,
        budget_warning_threshold   = 0.10,
        agent_history_window_ticks = 5,
        start_timestamp            = datetime(2025, 1, 1, tzinfo=timezone.utc),
        created_at                 = datetime(2025, 1, 1, tzinfo=timezone.utc),
    )


def _write_test_world(sim_id: str) -> None:
    '''
    Assemble a SimWorld for sim_id and write it to the env tables.
    write_world is idempotent with respect to re-runs (IF NOT EXISTS semantics).
    '''
    config = _make_test_sim_config(sim_id)
    world  = SimWorld(
        config            = config,
        items             = _TEST_ITEMS,
        suppliers         = _TEST_SUPPLIERS,
        consumers         = _TEST_CONSUMERS,
        supplier_item_map = _SUPPLIER_ITEM_MAP,
        consumer_item_map = _CONSUMER_ITEM_MAP,
        demand_patterns   = _make_demand_patterns(sim_id),
        supply_patterns   = {},    # no supply patterns needed for these tests
        # disruptions=[]: no disruptions keeps the world fully deterministic
        # and assertions clean. Disruption stochasticity adds RNG draws that
        # could mask reproducibility failures in T8.
        disruptions       = [],
    )
    write_world(spark, world)
    print(f"  Written: {sim_id}")


print("Writing test worlds...")
_write_test_world("test_llm_agent_wrapper_resilience_001")   # T7
_write_test_world("test_llm_agent_wrapper_repro_001")        # T8 run A
_write_test_world("test_llm_agent_wrapper_repro_002")        # T8 run B (same config, different sim_id)
print("Done.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7 - Resilience wrap (Spark required)
# MAGIC
# MAGIC Injects an always-raising agent into the runner. Asserts the simulation
# MAGIC completes, `AGENT_ERROR` is logged every tick, and hold-all decisions
# MAGIC are written to `hist_reorder_decisions`.

# COMMAND ----------

from warehouse_sim.agent.base import BaseAgent
from warehouse_sim.engine.runner import SimRunner
from warehouse_sim.config.loader import load_world
from warehouse_sim.world.patterns import PatternSampler
from warehouse_sim.event_log.event_log import EventLogger

SIM_ID_RESILIENCE = "test_llm_agent_wrapper_resilience_001"

_header("T7", "Resilience wrap - always-raising agent (Spark required)")

class _AlwaysRaisingAgent(BaseAgent):
    @staticmethod
    def agent_version() -> str: return "always_raising_v1"
    def decide(self, context): raise RuntimeError("Intentional test failure")

try:
    world   = load_world(spark, SIM_ID_RESILIENCE)
    sampler = PatternSampler(seed=world.config.random_seed)
    logger  = EventLogger(spark, sim_id=SIM_ID_RESILIENCE)
    agent   = _AlwaysRaisingAgent()
    runner  = SimRunner(spark, world, agent, logger, sampler)
    runner.run()
    run_completed = True
except Exception as e:
    run_completed = False
    print(f"  Runner raised: {e}")

_record("T7a", "Runner completes without propagating agent exception",
    run_completed)

if run_completed:
    # Check AGENT_ERROR events in event_log
    error_events = spark.sql(f'''
        SELECT COUNT(*) AS n
        FROM hackathon_of_the_century.tables4eventlog.event_log
        WHERE sim_id = '{SIM_ID_RESILIENCE}'
          AND event_type = 'AGENT_ERROR'
    ''').collect()[0]["n"]

    _record("T7b", "AGENT_ERROR event logged for every tick",
        error_events == world.config.num_ticks,
        f"AGENT_ERROR events={error_events}, expected={world.config.num_ticks}")

    # Check hold-all decisions in hist_reorder_decisions
    non_hold = spark.sql(f'''
        SELECT COUNT(*) AS n
        FROM hackathon_of_the_century.tables4hist.hist_reorder_decisions
        WHERE sim_id = '{SIM_ID_RESILIENCE}'
          AND order_qty > 0
    ''').collect()[0]["n"]

    _record("T7c", "All decisions are hold (order_qty=0) when agent always raises",
        non_hold == 0,
        f"non-hold decisions={non_hold}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 8 - Reproducibility (Spark required)
# MAGIC
# MAGIC Two runs with identical seed and `stub_mode="structural_fail"` (so all
# MAGIC decisions come from `RuleBasedAgent`). Asserts `hist_reorder_decisions`
# MAGIC is identical across both runs.
# MAGIC
# MAGIC **Requires**: active SparkSession. Env tables are populated by the setup cell above.

# COMMAND ----------

SIM_ID_REPRO_A = "test_llm_agent_wrapper_repro_001"
SIM_ID_REPRO_B = "test_llm_agent_wrapper_repro_002"

_header("T8", "Reproducibility - two runs, same seed, structural_fail stub (Spark required)")

llm_agent_wrapper_config = LLMAgentWrapperConfig(
    executor_trigger_every_n_ticks   = 5,
    context_obsolescence_threshold_k = 3,
    stub_mode                        = "structural_fail",
)

def _run_with_llm_agent_wrapper(sim_id: str) -> bool:
    world   = load_world(spark, sim_id)
    sampler = PatternSampler(seed=world.config.random_seed)
    logger  = EventLogger(spark, sim_id=sim_id)
    agent   = LLMAgentWrapper(
        spark  = spark,
        world  = world,
        config = llm_agent_wrapper_config,
        logger = logger,
    )
    runner  = SimRunner(spark, world, agent, logger, sampler)
    runner.run()
    return True

try:
    _run_with_llm_agent_wrapper(SIM_ID_REPRO_A)
    _run_with_llm_agent_wrapper(SIM_ID_REPRO_B)
    runs_ok = True
except Exception as e:
    runs_ok = False
    print(f"  Run failed: {e}")

_record("T8a", "Both runs complete without error", runs_ok)

if runs_ok:
    # Compare hist_reorder_decisions (item_id, tick, order_qty) across both runs.
    # sim_id is excluded from the comparison intentionally.
    decisions_a = spark.sql(f'''
        SELECT tick, item_id, order_qty
        FROM hackathon_of_the_century.tables4hist.hist_reorder_decisions
        WHERE sim_id = '{SIM_ID_REPRO_A}'
        ORDER BY tick, item_id
    ''').collect()

    decisions_b = spark.sql(f'''
        SELECT tick, item_id, order_qty
        FROM hackathon_of_the_century.tables4hist.hist_reorder_decisions
        WHERE sim_id = '{SIM_ID_REPRO_B}'
        ORDER BY tick, item_id
    ''').collect()

    rows_match = decisions_a == decisions_b
    _record("T8b", "hist_reorder_decisions identical across both runs",
        rows_match,
        f"rows_a={len(decisions_a)}, rows_b={len(decisions_b)}, match={rows_match}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"\n{'═' * 60}")
print(f"  LLMAgentWrapper TEST SUMMARY")
print(f"{'═' * 60}\n")

passed = [r for r in _results if r[2]]
failed = [r for r in _results if not r[2]]

for r in _results:
    status = "✓" if r[2] else "✗"
    print(f"  {status}  [{r[0]}] {r[1]}")
    if not r[2] and r[3]:
        print(f"       {r[3]}")

print(f"\n{'─' * 60}")
print(f"  {len(passed)} passed / {len(failed)} failed / {len(_results)} total")
print(f"{'═' * 60}\n")

if failed:
    raise AssertionError(
        f"{len(failed)} test(s) failed: "
        + ", ".join(f"[{r[0]}] {r[1]}" for r in failed)
    )