# Databricks notebook source
# Run in a Databricks notebook cell
import yaml
print(yaml.__version__)

# COMMAND ----------

# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Task 1 — LLM Agent Skeleton Verification
# MAGIC Tests the standalone agent with no simulator dependency and no LLM calls.

# COMMAND ----------
# Cell 1 — path setup and import

import sys
sys.path.insert(0, '/Workspace/Shared/reorder-llm-agent')

# Verify pyyaml is available
import yaml
print(f'pyyaml version: {yaml.__version__}')

from llm_agent import LLMReorderAgent, serialise_context
from base import (
    AgentContext, ItemState, PendingOrder,
    DemandRecord, ActiveDisruption, CostSnapshot,
)

print('✓ All imports resolved')

# COMMAND ----------

agent = LLMReorderAgent()

assert agent._system_prompt  != '',                    'system prompt is empty'
assert agent._agent_version  == 'llm_reorder_agent_v1'
assert agent._warehouse_id   != 'your_warehouse_id_here', \
    'Fill in your real warehouse_id in config.yml before running this test'
assert len(agent._uc_functions) == 8, \
    f'Expected 8 UC functions, got {len(agent._uc_functions)}'

print('✓ Test A passed: config loaded correctly')

# COMMAND ----------

# COMMAND ----------
# Cell 3 — Build mock AgentContext (no Spark, no Delta)

mock_context = AgentContext(
    sim_id = 'test_sim_001',
    tick   = 5,
    item_states = {
        'ITEM_A': ItemState(
            item_id                     = 'ITEM_A',
            stock_on_hand               = 18,
            stock_in_transit            = 0,
            expected_arrivals_next_tick = 0,
            reorder_point               = 30,
            min_order_qty               = 50,
            max_order_qty               = 200,
        ),
        'ITEM_B': ItemState(
            item_id                     = 'ITEM_B',
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
        'ITEM_A': [
            DemandRecord(
                tick=t, item_id='ITEM_A',
                raw_demand=15.0, disrupted_demand=15.0,
                fulfilled=14, unmet=1,
            )
            for t in range(1, 6)
        ],
        'ITEM_B': [
            DemandRecord(
                tick=t, item_id='ITEM_B',
                raw_demand=8.0, disrupted_demand=8.0,
                fulfilled=8, unmet=0,
            )
            for t in range(1, 6)
        ],
    },
    active_disruptions = [
        ActiveDisruption(
            disruption_id       = 'D001',
            item_id             = 'ITEM_A',
            disruption_type     = 'transit_delay',
            effective_magnitude = 1.4,
            is_active_this_tick = True,
        )
    ],
    cost_snapshots = {
        'ITEM_A': CostSnapshot(
            item_id                      = 'ITEM_A',
            cumulative_holding_cost      = 45.0,
            cumulative_stockout_cost     = 20.0,
            cumulative_order_cost        = 180.0,
            cumulative_transit_loss_cost = 0.0,
            cumulative_total_cost        = 245.0,
            remaining_budget             = 800.0,
        ),
        'ITEM_B': CostSnapshot(
            item_id                      = 'ITEM_B',
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

print('✓ Mock AgentContext built')

# COMMAND ----------

prompt = serialise_context(mock_context, history_window=10)

assert 'SIM_ID: test_sim_001'               in prompt
assert 'TICK:   5'                           in prompt
assert '[ITEM_A]'                            in prompt
assert '[ITEM_B]'                            in prompt
assert 'stock_on_hand:               18'     in prompt
assert 'transit_delay'                       in prompt
assert 'est_ticks_to_stockout'               in prompt
assert 'REMAINING_BUDGET: 800'               in prompt
assert 'rolling_avg_fulfilled'               in prompt
assert 'pending_orders:              none'   in prompt

print('✓ Test B passed: serialiser output correct')
print('\n── Full prompt output ──────────────────────────────────')
print(prompt)

# COMMAND ----------

decisions = agent.decide(mock_context)

assert len(decisions) == 2, \
    f'Expected 2 decisions, got {len(decisions)}'
assert {d.item_id for d in decisions} == {'ITEM_A', 'ITEM_B'}, \
    f'Wrong item_ids in decisions: {[d.item_id for d in decisions]}'
assert all(d.is_hold for d in decisions), \
    'Stub should hold all items'
assert all(d.order_qty == 0 for d in decisions), \
    'Stub order_qty should be 0 for all items'
assert all(d.reasoning is not None for d in decisions), \
    'Reasoning should not be None'

print('✓ Test C passed: decide() contract satisfied')
print('\n── Decisions ───────────────────────────────────────────')
for d in decisions:
    print(f'  {d.item_id}: order_qty={d.order_qty} reasoning="{d.reasoning}"')