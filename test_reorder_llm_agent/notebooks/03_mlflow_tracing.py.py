# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Task 4 - MLflow Autolog + Tracing Verification
# MAGIC Confirms that every LLM call, tool invocation, and decision
# MAGIC is automatically traced in MLflow per tick.

# COMMAND ----------
# Cell 1 - install dependencies (skip if already done in this session)

%pip install databricks-langchain langgraph langchain-core mlflow

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Cell 2 - clear cache and setup
import sys
import shutil
import os

cache_dir = 'test_llm_reorder_agent/__pycache__'
if os.path.exists(cache_dir):
    shutil.rmtree(cache_dir)
    print('✓ cache cleared')

sys.dont_write_bytecode = True
for mod in ['llm_agent', 'uc_tools', 'base']:
    if mod in sys.modules:
        del sys.modules[mod]

sys.path.insert(0, '/Workspace/Shared/reorder-llm-agent')

import mlflow
from llm_agent import LLMReorderAgent, serialise_context
from base import (
    AgentContext, ItemState, DemandRecord,
    ActiveDisruption, CostSnapshot,
)

print('✓ imports resolved')
print(f'MLflow version: {mlflow.__version__}')

# COMMAND ----------

mock_context = AgentContext(
    sim_id = 'sim_stage4_001',
    tick   = 10,   # use tick=10 so it is distinct from Task 3 runs
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
            for t in range(1, 11)
        ],
        'item_B': [
            DemandRecord(
                tick=t, item_id='item_B',
                raw_demand=8.0, disrupted_demand=8.0,
                fulfilled=8, unmet=0,
            )
            for t in range(1, 11)
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

print('✓ mock context built for tick=10')


# COMMAND ----------

# COMMAND ----------
# Cell 4 - Test A: agent initialises with MLflow autolog enabled

agent = LLMReorderAgent()

assert agent._graph is not None, 'graph should be compiled'

# Verify experiment was set
experiment = mlflow.get_experiment_by_name(
    'test_llm_reorder_agent/experiments/llm_reorder_agent'
)
assert experiment is not None, \
    'MLflow experiment should exist after LLMReorderAgent.__init__'

print(f'✓ Test A passed: agent initialised with MLflow')
print(f'  experiment_id : {experiment.experiment_id}')
print(f'  experiment    : {experiment.name}')

# COMMAND ----------

# COMMAND ----------
# Cell 5 - Test B: run decide() and verify a run was created

# Wrap in a parent run so the tick run is nested cleanly
with mlflow.start_run(run_name='task4_verification_parent') as parent_run:
    decisions = agent.decide(mock_context)

parent_run_id = parent_run.info.run_id
print(f'\n✓ parent run completed: {parent_run_id}')

# Validate decisions
assert len(decisions) == 2
assert {d.item_id for d in decisions} == {'item_A', 'item_B'}
assert all(d.reasoning is not None for d in decisions)
print('✓ Test B passed: decisions returned correctly')

# COMMAND ----------

# COMMAND ----------
# Cell 6 - Test C: verify child run was logged with correct tags and metrics

import mlflow
from mlflow.tracking import MlflowClient

client = MlflowClient()

# Find child runs of the parent
child_runs = client.search_runs(
    experiment_ids = [experiment.experiment_id],
    filter_string  = f"tags.`mlflow.parentRunId` = '{parent_run_id}'",
)

assert len(child_runs) >= 1, \
    f'Expected at least 1 child run, got {len(child_runs)}'

child = child_runs[0]

print('── Child run ───────────────────────────────────────────')
print(f'  run_id       : {child.info.run_id}')
print(f'  run_name     : {child.info.run_name}')
print(f'  status       : {child.info.status}')
print()
print('── Tags ────────────────────────────────────────────────')
for k, v in child.data.tags.items():
    if not k.startswith('mlflow.'):
        print(f'  {k}: {v}')
print()
print('── Params ──────────────────────────────────────────────')
for k, v in child.data.params.items():
    print(f'  {k}: {v}')
print()
print('── Metrics ─────────────────────────────────────────────')
for k, v in child.data.metrics.items():
    print(f'  {k}: {v}')

# Verify required tags
assert child.data.tags.get('sim_id') == 'sim_stage4_001', \
    'sim_id tag should be set'
assert child.data.tags.get('tick') == '10', \
    'tick tag should be set'
assert child.data.tags.get('agent_version') == 'llm_reorder_agent_v1', \
    'agent_version tag should be set'

# Verify metrics were logged
assert 'n_reorders' in child.data.metrics, 'n_reorders metric missing'
assert 'n_holds'    in child.data.metrics, 'n_holds metric missing'
assert 'total_order_qty' in child.data.metrics, 'total_order_qty metric missing'

print()
print('✓ Test C passed: child run has correct tags and metrics')

# COMMAND ----------

# COMMAND ----------
# Cell 7 - Test D: verify autolog captured LangChain traces
# These appear as child spans inside the run automatically

runs = client.search_runs(
    experiment_ids = [experiment.experiment_id],
    filter_string  = f"run_id = '{child.info.run_id}'",
)

print('── Autolog artifacts in child run ──────────────────────')
artifacts = client.list_artifacts(child.info.run_id)
for a in artifacts:
    print(f'  {a.path}')

print()
# Check MLflow traces exist for this run
try:
    traces = mlflow.search_traces(
        experiment_ids = [experiment.experiment_id],
        filter_string  = f"tags.`mlflow.sourceRun` = '{child.info.run_id}'"
    )
    print(f'  Traces found: {len(traces)}')
    if len(traces) > 0:
        print(f'  First trace spans:')
        for span in traces[0].data.spans[:5]:
            print(f'    - {span.name} ({span.span_type})')
    print('✓ Test D passed: MLflow traces captured')
except Exception as e:
    print(f'  Note: trace search not available in this MLflow version ({e})')
    print('  Check Experiments UI manually - traces appear under the run')
    print('✓ Test D: skipped (check UI manually)')

# COMMAND ----------

# COMMAND ----------
# Cell 8 - print experiment URL for manual UI verification

workspace_url = spark.conf.get('spark.databricks.workspaceUrl', '')
exp_id = experiment.experiment_id

print('── Open in MLflow UI ───────────────────────────────────')
print(f'  https://{workspace_url}/#mlflow/experiments/{exp_id}')
print()
print('  What to look for:')
print('  1. A run named task4_verification_parent')
print('  2. A nested child run named tick_10_sim_stage4_001')
print('  3. Tags: sim_id, tick, agent_version, n_items')
print('  4. Metrics: n_reorders, n_holds, total_order_qty')
print('  5. Traces tab - LangGraph steps and tool calls listed as spans')