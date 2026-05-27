# Databricks notebook source
# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Task 5 - Driver: Log → Evaluate → Register → Deploy
# MAGIC
# MAGIC Follows the Investment Assistant driver.py pattern exactly.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Log the agent as an MLflow model
# MAGIC 2. Build evaluation dataset
# MAGIC 3. Define judge with scoring criteria
# MAGIC 4. Run evaluation against dataset
# MAGIC 5. Register to Unity Catalog (if evaluation passes)
# MAGIC 6. Deploy to Model Serving endpoint

# COMMAND ----------
# Cell 1 - install dependencies

%pip install databricks-langchain langgraph langchain-core mlflow databricks-agents

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Cell 2 - imports and setup

import sys
import os
import shutil
import json
import pandas as pd        # ← add this line

# Clear stale bytecode
cache_dir = 'test_llm_reorder_agent/__pycache__'
if os.path.exists(cache_dir):
    shutil.rmtree(cache_dir)

sys.dont_write_bytecode = True
for mod in ['llm_agent', 'uc_tools', 'base']:
    if mod in sys.modules:
        del sys.modules[mod]

sys.path.insert(0, '/Workspace/Shared/reorder-llm-agent')

import mlflow
import mlflow.langchain
from mlflow.tracking import MlflowClient
from databricks import agents

from llm_agent import LLMReorderAgent, serialise_context
from base import (
    AgentContext, ItemState, DemandRecord,
    ActiveDisruption, CostSnapshot,
)

EXPERIMENT_NAME  = 'test_llm_reorder_agent/experiments/llm_reorder_agent'
UC_MODEL_NAME    = 'hackathon_of_the_century.agent_tools.llm_reorder_agent'
AGENT_VERSION    = 'llm_reorder_agent_v1'

mlflow.set_experiment(EXPERIMENT_NAME)
client = MlflowClient()

print('✓ imports resolved')
print(f'MLflow version : {mlflow.__version__}')
print(f'Experiment     : {EXPERIMENT_NAME}')
print(f'UC model       : {UC_MODEL_NAME}')

# COMMAND ----------

# # COMMAND ----------
# # MAGIC %md
# # MAGIC ## Step 1 - Log the agent as an MLflow model

# # COMMAND ----------
# # Cell 3 - define the agent as a PyFunc model wrapper for MLflow logging
# #
# # MLflow needs a pyfunc model to log and serve the agent.
# # The wrapper translates the MLflow predict() interface
# # (which receives a DataFrame or dict) into AgentContext
# # and calls agent.decide() - keeping the agent code unchanged.

# import pandas as pd
# import json
# from typing import Optional

# class ReorderAgentWrapper(mlflow.pyfunc.PythonModel):
#     '''
#     MLflow PyFunc wrapper around LLMReorderAgent.

#     predict() accepts a dict with an AgentContext-compatible
#     JSON payload and returns a list of ReorderDecision dicts.

#     This wrapper is what gets logged, registered, and served.
#     The underlying LLMReorderAgent code is unchanged.
#     '''

#     def load_context(self, context):
#         '''Called once when the model is loaded for serving.'''
#         import sys
#         sys.path.insert(0, '/Workspace/Shared/reorder-llm-agent')
#         from llm_agent import LLMReorderAgent
#         self.agent = LLMReorderAgent()

#     def predict(self, context, model_input):
#         '''
#         model_input: dict or DataFrame with keys matching AgentContext fields.
#         Returns: list of dicts with item_id, decision, order_qty, reasoning.
#         '''
#         from base import (
#             AgentContext, ItemState, PendingOrder,
#             DemandRecord, ActiveDisruption, CostSnapshot,
#         )

#         # Accept dict or single-row DataFrame
#         if isinstance(model_input, pd.DataFrame):
#             payload = model_input.iloc[0].to_dict()
#         else:
#             payload = model_input

#         # Deserialise AgentContext from JSON payload
#         # The payload is the serialised context string from serialise_context()
#         # When served via REST API, this is what the caller sends
#         sim_id           = payload.get('sim_id', 'unknown')
#         tick             = int(payload.get('tick', 0))
#         remaining_budget = payload.get('remaining_budget', None)

#         # Item states
#         item_states_raw = payload.get('item_states', {})
#         if isinstance(item_states_raw, str):
#             item_states_raw = json.loads(item_states_raw)
#         item_states = {
#             k: ItemState(**v) for k, v in item_states_raw.items()
#         }

#         # Demand history
#         demand_history_raw = payload.get('demand_history', {})
#         if isinstance(demand_history_raw, str):
#             demand_history_raw = json.loads(demand_history_raw)
#         demand_history = {
#             k: [DemandRecord(**r) for r in v]
#             for k, v in demand_history_raw.items()
#         }

#         # Pending orders
#         pending_raw = payload.get('pending_orders', [])
#         if isinstance(pending_raw, str):
#             pending_raw = json.loads(pending_raw)
#         pending_orders = [PendingOrder(**o) for o in pending_raw]

#         # Active disruptions
#         disruptions_raw = payload.get('active_disruptions', [])
#         if isinstance(disruptions_raw, str):
#             disruptions_raw = json.loads(disruptions_raw)
#         active_disruptions = [ActiveDisruption(**d) for d in disruptions_raw]

#         # Cost snapshots
#         costs_raw = payload.get('cost_snapshots', {})
#         if isinstance(costs_raw, str):
#             costs_raw = json.loads(costs_raw)
#         cost_snapshots = {
#             k: CostSnapshot(**v) for k, v in costs_raw.items()
#         }

#         agent_context = AgentContext(
#             sim_id             = sim_id,
#             tick               = tick,
#             item_states        = item_states,
#             pending_orders     = pending_orders,
#             demand_history     = demand_history,
#             active_disruptions = active_disruptions,
#             cost_snapshots     = cost_snapshots,
#             remaining_budget   = remaining_budget,
#         )

#         decisions = self.agent.decide(agent_context)

#         return [
#             {
#                 'item_id'   : d.item_id,
#                 'decision'  : 'reorder' if d.is_reorder else 'hold',
#                 'order_qty' : d.order_qty,
#                 'reasoning' : d.reasoning,
#             }
#             for d in decisions
#         ]

# print('✓ ReorderAgentWrapper defined')

# COMMAND ----------

# DBTITLE 1,Cell 5
# Cell 4 - log the agent using code-based logging (MLflow 3.x)

import json
import shutil
import os

# Copy only the files needed for serving into a clean temp directory
# This avoids MLflow trying to copy the experiments/ subfolder
# which contains internal MLflow files that cannot be copied

STAGING_DIR = '/tmp/reorder_agent_staging'

# Clean and recreate staging dir
if os.path.exists(STAGING_DIR):
    shutil.rmtree(STAGING_DIR)
os.makedirs(STAGING_DIR)

# Copy only the source files the agent needs at serve time
FILES_TO_STAGE = [
    'agent_model.py',
    'llm_agent.py',
    'uc_tools.py',
    'base.py',
    'config.yml',
]

for fname in FILES_TO_STAGE:
    src = f'test_llm_reorder_agent/{fname}'
    dst = f'{STAGING_DIR}/{fname}'
    shutil.copy2(src, dst)
    print(f'  staged: {fname}')

print(f'✓ Staging dir ready: {STAGING_DIR}')

# Log the model
with mlflow.start_run(run_name='llm_reorder_agent_log') as run:

    mlflow.set_tag('agent_version', AGENT_VERSION)
    mlflow.set_tag('task', 'driver_log')

    model_info = mlflow.pyfunc.log_model(
        artifact_path    = 'reorder_agent',
        python_model     = f'{STAGING_DIR}/agent_model.py',
        artifacts        = {
            'agent_dir'  : STAGING_DIR    # clean dir, no experiments/ subfolder
        },
        pip_requirements = [
            'databricks-langchain',
            'langgraph',
            'langchain-core',
            'mlflow>=3.0.0',
            'pyyaml',
        ],
        input_example    = {
            'sim_id'            : 'sim_stage4_001',
            'tick'              : 5,
            'item_states'       : json.dumps({
                'item_A': {
                    'item_id'                    : 'item_A',
                    'stock_on_hand'              : 18,
                    'stock_in_transit'           : 0,
                    'expected_arrivals_next_tick': 0,
                    'reorder_point'              : 30,
                    'min_order_qty'              : 50,
                    'max_order_qty'              : 200,
                }
            }),
            'demand_history'    : json.dumps({}),
            'pending_orders'    : json.dumps([]),
            'active_disruptions': json.dumps([]),
            'cost_snapshots'    : json.dumps({}),
            'remaining_budget'  : 800.0,
        },
    )

    logged_run_id    = run.info.run_id
    logged_model_uri = model_info.model_uri

print(f'\n✓ Agent logged via code-based logging')
print(f'  run_id    : {logged_run_id}')
print(f'  model_uri : {logged_model_uri}')

# COMMAND ----------

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 - Build evaluation dataset

# COMMAND ----------
# Cell 5 - define evaluation scenarios
#
# Four representative scenarios covering the key decision branches:
# 1. Healthy stock - expect HOLD
# 2. Imminent stockout - expect REORDER max qty
# 3. Transit delay disruption - expect REORDER with inflated qty
# 4. Pending order already covers need - expect HOLD

import json

def _make_context_payload(
    sim_id, tick, item_states, demand_history=None,
    active_disruptions=None, pending_orders=None,
    cost_snapshots=None, remaining_budget=800.0
):
    '''Helper to build a serialisable AgentContext payload.'''
    return {
        'sim_id'            : sim_id,
        'tick'              : tick,
        'item_states'       : json.dumps(item_states),
        'demand_history'    : json.dumps(demand_history or {}),
        'active_disruptions': json.dumps(active_disruptions or []),
        'pending_orders'    : json.dumps(pending_orders or []),
        'cost_snapshots'    : json.dumps(cost_snapshots or {}),
        'remaining_budget'  : remaining_budget,
    }

eval_scenarios = [
    {
        'scenario'        : 'healthy_stock_hold',
        'description'     : 'Stock well above reorder point, no disruptions. Agent should HOLD.',
        'input'           : _make_context_payload(
            sim_id = 'eval_001', tick = 1,
            item_states = {
                'item_A': {
                    'item_id': 'item_A', 'stock_on_hand': 250,
                    'stock_in_transit': 0, 'expected_arrivals_next_tick': 0,
                    'reorder_point': 50, 'min_order_qty': 50, 'max_order_qty': 200,
                }
            },
            demand_history = {
                'item_A': [
                    {'tick': t, 'item_id': 'item_A', 'raw_demand': 10.0,
                     'disrupted_demand': 10.0, 'fulfilled': 10, 'unmet': 0}
                    for t in range(1, 8)
                ]
            },
        ),
        'expected_decision' : 'hold',
        'expected_item'     : 'item_A',
    },
    {
        'scenario'        : 'imminent_stockout_reorder',
        'description'     : 'Stock critically low, 1.2 ticks to stockout, no pending orders. Agent should REORDER.',
        'input'           : _make_context_payload(
            sim_id = 'eval_002', tick = 1,
            item_states = {
                'item_A': {
                    'item_id': 'item_A', 'stock_on_hand': 12,
                    'stock_in_transit': 0, 'expected_arrivals_next_tick': 0,
                    'reorder_point': 50, 'min_order_qty': 50, 'max_order_qty': 200,
                }
            },
            demand_history = {
                'item_A': [
                    {'tick': t, 'item_id': 'item_A', 'raw_demand': 10.0,
                     'disrupted_demand': 10.0, 'fulfilled': 10, 'unmet': 0}
                    for t in range(1, 8)
                ]
            },
        ),
        'expected_decision' : 'reorder',
        'expected_item'     : 'item_A',
    },
    {
        'scenario'        : 'transit_delay_disruption_reorder',
        'description'     : 'Stock near reorder point with active transit delay (1.4x). Agent should REORDER with inflated qty.',
        'input'           : _make_context_payload(
            sim_id = 'eval_003', tick = 1,
            item_states = {
                'item_A': {
                    'item_id': 'item_A', 'stock_on_hand': 35,
                    'stock_in_transit': 0, 'expected_arrivals_next_tick': 0,
                    'reorder_point': 30, 'min_order_qty': 50, 'max_order_qty': 200,
                }
            },
            demand_history = {
                'item_A': [
                    {'tick': t, 'item_id': 'item_A', 'raw_demand': 12.0,
                     'disrupted_demand': 12.0, 'fulfilled': 12, 'unmet': 0}
                    for t in range(1, 8)
                ]
            },
            active_disruptions = [
                {
                    'disruption_id': 'D001', 'item_id': 'item_A',
                    'disruption_type': 'transit_delay',
                    'effective_magnitude': 1.4,
                    'is_active_this_tick': True,
                }
            ],
        ),
        'expected_decision' : 'reorder',
        'expected_item'     : 'item_A',
    },
    {
        'scenario'        : 'pending_order_covers_hold',
        'description'     : 'Stock below reorder point but pending order arriving next tick covers need. Agent should HOLD.',
        'input'           : _make_context_payload(
            sim_id = 'eval_004', tick = 1,
            item_states = {
                'item_A': {
                    'item_id': 'item_A', 'stock_on_hand': 20,
                    'stock_in_transit': 150, 'expected_arrivals_next_tick': 150,
                    'reorder_point': 50, 'min_order_qty': 50, 'max_order_qty': 200,
                }
            },
            demand_history = {
                'item_A': [
                    {'tick': t, 'item_id': 'item_A', 'raw_demand': 10.0,
                     'disrupted_demand': 10.0, 'fulfilled': 10, 'unmet': 0}
                    for t in range(1, 8)
                ]
            },
            pending_orders = [
                {
                    'order_id': 'ORD_001', 'item_id': 'item_A',
                    'supplier_id': 'SUP_A', 'order_tick': 0,
                    'expected_arrival_tick': 2, 'order_qty': 150,
                }
            ],
        ),
        'expected_decision' : 'hold',
        'expected_item'     : 'item_A',
    },
]

print(f'✓ {len(eval_scenarios)} evaluation scenarios defined')
for s in eval_scenarios:
    print(f'  {s["scenario"]:45s} → expected: {s["expected_decision"]}')

# COMMAND ----------

# Cell 6 - run agent against each scenario and score
# Calls LLMReorderAgent directly - no wrapper needed for local eval

from base import (
    AgentContext, ItemState, PendingOrder,
    DemandRecord, ActiveDisruption, CostSnapshot,
)

agent = LLMReorderAgent()

def _build_context_from_payload(payload):
    '''Convert the eval scenario payload dict into an AgentContext.'''

    def _parse(val):
        if isinstance(val, str):
            return json.loads(val)
        return val

    sim_id           = payload.get('sim_id', 'unknown')
    tick             = int(payload.get('tick', 0))
    remaining_budget = payload.get('remaining_budget', None)
    if remaining_budget is not None:
        remaining_budget = float(remaining_budget)

    item_states = {
        k: ItemState(**v)
        for k, v in _parse(payload.get('item_states', '{}')).items()
    }
    demand_history = {
        k: [DemandRecord(**r) for r in v]
        for k, v in _parse(payload.get('demand_history', '{}')).items()
    }
    pending_orders = [
        PendingOrder(**o)
        for o in _parse(payload.get('pending_orders', '[]'))
    ]
    active_disruptions = [
        ActiveDisruption(**d)
        for d in _parse(payload.get('active_disruptions', '[]'))
    ]
    cost_snapshots = {
        k: CostSnapshot(**v)
        for k, v in _parse(payload.get('cost_snapshots', '{}')).items()
    }

    return AgentContext(
        sim_id             = sim_id,
        tick               = tick,
        item_states        = item_states,
        pending_orders     = pending_orders,
        demand_history     = demand_history,
        active_disruptions = active_disruptions,
        cost_snapshots     = cost_snapshots,
        remaining_budget   = remaining_budget,
    )


eval_results = []

for scenario in eval_scenarios:
    print(f'\n── Scenario: {scenario["scenario"]} ──')
    print(f'   expected : {scenario["expected_decision"]} '
          f'for {scenario["expected_item"]}')

    try:
        # Build real AgentContext and call decide() directly
        context   = _build_context_from_payload(scenario['input'])
        decisions = agent.decide(context)

    except Exception as e:
        import traceback
        print(f'   ERROR: {e}')
        traceback.print_exc()
        eval_results.append({
            'scenario'          : scenario['scenario'],
            'expected_decision' : scenario['expected_decision'],
            'actual_decision'   : 'ERROR',
            'correct'           : False,
            'reasoning'         : '',
            'error'             : str(e),
        })
        continue

    # Find decision for the expected item
    item_decision = next(
        (d for d in decisions if d.item_id == scenario['expected_item']),
        None
    )

    if item_decision is None:
        actual    = 'MISSING'
        correct   = False
        reasoning = ''
    else:
        actual    = 'reorder' if item_decision.is_reorder else 'hold'
        correct   = (actual == scenario['expected_decision'])
        reasoning = item_decision.reasoning or ''

    status = '✓ PASS' if correct else '✗ FAIL'
    print(f'   actual={actual!r} correct={correct}')
    print(f'   reasoning: {reasoning[:100]}')
    print(f'   {status}')

    eval_results.append({
        'scenario'          : scenario['scenario'],
        'description'       : scenario['description'],
        'expected_decision' : scenario['expected_decision'],
        'actual_decision'   : actual,
        'order_qty'         : item_decision.order_qty if item_decision else 0,
        'reasoning'         : reasoning[:120],
        'correct'           : correct,
        'error'             : '',
    })

# Summary
n_pass    = sum(1 for r in eval_results if r['correct'])
n_total   = len(eval_results)
pass_rate = n_pass / n_total if n_total > 0 else 0.0

print(f'\n── Evaluation Summary ──────────────────────────────────')
print(f'  Passed    : {n_pass} / {n_total}')
print(f'  Pass rate : {pass_rate:.0%}')

# COMMAND ----------

# COMMAND ----------
# Cell 7 - log evaluation results to MLflow

with mlflow.start_run(run_id=logged_run_id):
    mlflow.log_metric('eval_pass_rate',   pass_rate)
    mlflow.log_metric('eval_n_pass',      n_pass)
    mlflow.log_metric('eval_n_total',     n_total)

    # Log full results as artifact
    results_df = pd.DataFrame(eval_results)
    results_path = '/tmp/eval_results.csv'
    results_df.to_csv(results_path, index=False)
    mlflow.log_artifact(results_path, artifact_path='evaluation')

print(f'✓ Evaluation metrics logged to run {logged_run_id}')
display(results_df[['scenario', 'expected_decision', 'actual_decision', 'correct', 'reasoning']])


# COMMAND ----------

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 - Register to Unity Catalog (if evaluation passes)

# COMMAND ----------
# Cell 8 - register model to Unity Catalog
#
# Only proceeds if pass rate >= 75%.
# Mirrors the Investment Assistant pattern:
# mlflow.register_model() → UC model registry entry.
# mlflow.models.set_model() marks this as the primary model.

PASS_THRESHOLD = 0.75

if pass_rate >= PASS_THRESHOLD:
    print(f'✓ Pass rate {pass_rate:.0%} meets threshold {PASS_THRESHOLD:.0%}')
    print(f'Registering to Unity Catalog: {UC_MODEL_NAME}')

    registered = mlflow.register_model(
        model_uri    = logged_model_uri,
        name         = UC_MODEL_NAME,
    )

    model_version = registered.version
    print(f'✓ Registered: {UC_MODEL_NAME} version {model_version}')

    # Tag the version with evaluation results
    client.set_model_version_tag(
        name    = UC_MODEL_NAME,
        version = model_version,
        key     = 'eval_pass_rate',
        value   = str(pass_rate),
    )
    client.set_model_version_tag(
        name    = UC_MODEL_NAME,
        version = model_version,
        key     = 'eval_n_pass',
        value   = f'{n_pass}/{n_total}',
    )
    client.set_model_version_tag(
        name    = UC_MODEL_NAME,
        version = model_version,
        key     = 'agent_version',
        value   = AGENT_VERSION,
    )

    print(f'✓ Tags set on version {model_version}')

else:
    print(f'✗ Pass rate {pass_rate:.0%} below threshold {PASS_THRESHOLD:.0%}')
    print('  Model NOT registered. Fix failing scenarios and re-run.')
    model_version = None
    raise Exception(
        f'Evaluation failed: {n_pass}/{n_total} scenarios passed. '
        f'Required: {PASS_THRESHOLD:.0%}'
    )


# COMMAND ----------

# Cell 9 - deploy using Databricks SDK (not agents.deploy())
# agents.deploy() requires ChatCompletionRequest schema - ours is a
# custom payload schema, so we use the standard serving endpoint API.

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
    TrafficConfig,
    Route,
)
import time

SERVING_ENDPOINT = 'reorder-agent-endpoint'

w = WorkspaceClient()

# Check if endpoint already exists
existing = None
try:
    existing = w.serving_endpoints.get(SERVING_ENDPOINT)
    print(f'Endpoint {SERVING_ENDPOINT} already exists - updating...')
except Exception:
    print(f'Creating new endpoint: {SERVING_ENDPOINT}')

endpoint_config = EndpointCoreConfigInput(
    served_entities = [
        ServedEntityInput(
            name               = 'llm_reorder_agent_v1',
            entity_name        = UC_MODEL_NAME,
            entity_version     = str(model_version),
            workload_size      = 'Small',
            scale_to_zero_enabled = True,
        )
    ],
    traffic_config = TrafficConfig(
        routes = [
            Route(
                served_model_name    = 'llm_reorder_agent_v1',
                traffic_percentage   = 100,
            )
        ]
    ),
)

if existing is None:
    deployment = w.serving_endpoints.create(
        name   = SERVING_ENDPOINT,
        config = endpoint_config,
    )
    print(f'✓ Endpoint creation initiated')
else:
    deployment = w.serving_endpoints.update_config(
        name           = SERVING_ENDPOINT,
        served_entities = endpoint_config.served_entities,
        traffic_config  = endpoint_config.traffic_config,
    )
    print(f'✓ Endpoint update initiated')

print(f'  endpoint : {SERVING_ENDPOINT}')
print()
print('Note: endpoint takes 5-10 minutes to become READY.')
print(f'Check at: Serving → Endpoints → {SERVING_ENDPOINT}')

# COMMAND ----------

# COMMAND ----------
# Cell 10 - verify the registered model in UC

print('── Registered model in Unity Catalog ──────────────────')
versions = client.search_model_versions(f"name='{UC_MODEL_NAME}'")
for v in versions:
    print(f'  version : {v.version}')
    print(f'  status  : {v.status}')
    print(f'  run_id  : {v.run_id}')
    for k, val in v.tags.items():
        print(f'  tag [{k}]: {val}')
    print()

print(f'✓ Task 5 complete')
print(f'  UC model  : {UC_MODEL_NAME}')
print(f'  Version   : {model_version}')
print(f'  Endpoint  : {SERVING_ENDPOINT}')