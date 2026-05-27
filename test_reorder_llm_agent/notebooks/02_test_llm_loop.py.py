# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Task 3 - LangGraph Tool-Calling Loop Verification
# MAGIC Tests the live LLM agent end-to-end with a mock AgentContext.
# MAGIC No simulator required. Each cell is independent.

# COMMAND ----------
# Cell 1 - install dependencies

%pip install databricks-langchain langgraph langchain-core



# COMMAND ----------

# Cell 2 - path setup and imports

import sys
import importlib

# Force Python to ignore cached bytecode and reload from source
sys.dont_write_bytecode = True

# Remove any stale cached module if it exists
if 'llm_agent' in sys.modules:
    del sys.modules['llm_agent']
if 'uc_tools' in sys.modules:
    del sys.modules['uc_tools']

sys.path.insert(0, '/Workspace/Shared/reorder-llm-agent')

from llm_agent import LLMReorderAgent, serialise_context
from base import (
    AgentContext, ItemState, DemandRecord,
    ActiveDisruption, CostSnapshot,
)

print('✓ imports resolved')

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# COMMAND ----------
# Cell 2 - path setup and imports

import sys
sys.path.insert(0, '/Workspace/Shared/reorder-llm-agent')

from llm_agent import LLMReorderAgent, serialise_context
from base import (
    AgentContext, ItemState, DemandRecord,
    ActiveDisruption, CostSnapshot,
)

print('✓ imports resolved')

# COMMAND ----------

# COMMAND ----------
# Cell 3 - build mock context (same as Task 1 but richer)

mock_context = AgentContext(
    sim_id = 'sim_stage4_001',
    tick   = 5,
    item_states = {
        'item_A': ItemState(
            item_id                     = 'item_A',
            stock_on_hand               = 18,
            stock_in_transit            = 0,
            expected_arrivals_next_tick = 0,
            reorder_point               = 30,
            min_order_qty               = 50,
            max_order_qty               = 200,
        ),
        'item_B': ItemState(
            item_id                     = 'item_B',
            stock_on_hand               = 210,
            stock_in_transit            = 100,
            expected_arrivals_next_tick = 100,
            reorder_point               = 50,
            min_order_qty               = 50,
            max_order_qty               = 300,
        ),
    },
    pending_orders = [],
    demand_history = {
        'item_A': [
            DemandRecord(
                tick=t, item_id='item_A',
                raw_demand=15.0, disrupted_demand=15.0,
                fulfilled=14, unmet=1,
            )
            for t in range(1, 6)
        ],
        'item_B': [
            DemandRecord(
                tick=t, item_id='item_B',
                raw_demand=8.0, disrupted_demand=8.0,
                fulfilled=8, unmet=0,
            )
            for t in range(1, 6)
        ],
    },
    active_disruptions = [
        ActiveDisruption(
            disruption_id       = 'D001',
            item_id             = 'item_A',
            disruption_type     = 'transit_delay',
            effective_magnitude = 1.4,
            is_active_this_tick = True,
        )
    ],
    cost_snapshots = {
        'item_A': CostSnapshot(
            item_id                      = 'item_A',
            cumulative_holding_cost      = 45.0,
            cumulative_stockout_cost     = 20.0,
            cumulative_order_cost        = 180.0,
            cumulative_transit_loss_cost = 0.0,
            cumulative_total_cost        = 245.0,
            remaining_budget             = 800.0,
        ),
        'item_B': CostSnapshot(
            item_id                      = 'item_B',
            cumulative_holding_cost      = 30.0,
            cumulative_stockout_cost     = 0.0,
            cumulative_order_cost        = 120.0,
            cumulative_transit_loss_cost = 0.0,
            cumulative_total_cost        = 150.0,
            remaining_budget             = 800.0,
        ),
    },
    remaining_budget = 800.0,
)

print('✓ mock context built')

# COMMAND ----------

# DBTITLE 1,Cell 4
# Cell 4 - Test A: agent initialises and graph compiles

agent = LLMReorderAgent()
assert agent._graph is not None, 'LangGraph graph should be compiled'
print('✓ Test A passed: agent initialised, graph compiled')

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
for ep in w.serving_endpoints.list():
    print(ep.name, '|', ep.state.ready if ep.state else 'unknown')

# COMMAND ----------

# COMMAND ----------
# Cell 5 - Test B: full decide() call with live LLM
# This is the real test - LLM will reason, call tools, return decisions

print('Running live LLM decision for tick=5...')
print('(expect tool calls to get_pending_orders, get_supplier_info, etc.)')
print()

decisions = agent.decide(mock_context)

# Validate contract
assert len(decisions) == 2, \
    f'Expected 2 decisions, got {len(decisions)}'
assert {d.item_id for d in decisions} == {'item_A', 'item_B'}, \
    f'Wrong item_ids: {[d.item_id for d in decisions]}'
assert all(d.reasoning is not None for d in decisions), \
    'All decisions must have reasoning'

for d in decisions:
    if d.is_reorder:
        state = mock_context.item_states[d.item_id]
        assert d.order_qty >= state.min_order_qty, \
            f'{d.item_id}: order_qty {d.order_qty} below min {state.min_order_qty}'
        assert d.order_qty <= state.max_order_qty, \
            f'{d.item_id}: order_qty {d.order_qty} above max {state.max_order_qty}'

print()
print('✓ Test B passed: decide() contract satisfied with live LLM')
print()
print('── Decisions ───────────────────────────────────────────')
for d in decisions:
    print(f'  {d.item_id}:')
    print(f'    decision  : {"reorder" if d.is_reorder else "hold"}')
    if d.is_reorder:
        print(f'    order_qty : {d.order_qty}')
    print(f'    reasoning : {d.reasoning[:200]}')
    print()

# COMMAND ----------

# COMMAND ----------
# Cell 6 - Test C: verify audit log was written
# item_A should appear in hist_reorder_decisions for tick=5

audit = spark.sql('''
    SELECT sim_id, tick, item_id, decision, order_qty,
           agent_reasoning, agent_version
    FROM hackathon_of_the_century.tables4hist.hist_reorder_decisions
    WHERE sim_id = 'sim_stage4_001'
      AND tick   = 5
    ORDER BY item_id
''')

print('── Audit log entries for tick=5 ────────────────────────')
audit.show(truncate=80)

row_count = audit.count()
assert row_count >= 1, \
    f'Expected at least 1 audit row for tick=5, got {row_count}'
print(f'✓ Test C passed: {row_count} audit row(s) written to hist_reorder_decisions')

# COMMAND ----------

# COMMAND ----------
# Cell 7 - Test D: check escalation queue (may be empty - that is fine)

esc = spark.sql('''
    SELECT sim_id, tick, item_id, reason, status, raised_at
    FROM hackathon_of_the_century.tables4ops.ops_escalation_queue
    WHERE sim_id = 'sim_stage4_001'
      AND tick   = 5
    ORDER BY item_id
''')

print('── Escalation queue entries for tick=5 ─────────────────')
esc.show(truncate=80)

esc_count = esc.count()
if esc_count == 0:
    print('✓ Test D: no escalations (expected - mock context has budget headroom)')
else:
    print(f'✓ Test D: {esc_count} escalation(s) raised and written correctly')